package concurrency;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.lang3.Validate;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import java.io.*;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * A utility class that helps clients read and write archives.
 *
 * @author ramap
 */
@Slf4j @ThreadSafe @Immutable
public class ArchiveSerDes {

    public static final ArchiveSerDes INSTANCE = new ArchiveSerDes();

    private static final Void NULL_VALUE_FOR_FUTURE = null;

    private static final int DEFAULT_MAX_EXTRACTION_WAIT_SECONDS = 10;

    /**
     * This is a file atomically created in the extracted directory to mark the successful completion of archive extraction.
     * We went back and forth on various names and places with which we could create this file. One way is to create it such that
     * the source file name and destination directory are reflected in the name. But, that could lead to very long file names and
     * hit the UNIX 255 character limit on file name length. In the end, we decided to create this filename, which is extremely unlikely
     * for someone to put in an archive file. Since we control the writers and are planning on having all writers use this class to
     * create archives, we can establish and enforce this contract as well.
     */
    public static final String COMPLETION_MARKER_FILE = "__extraction__complete__";

    /**
     * This map holds information about work-in-progress extractions. This is used to prevent multiple threads trying to extract the
     * same archive concurrently. This is keyed by destination because that's where threads are trying to write and concurrent writes
     * can lead to an undesirable state.
     *
     * At some point in future, we should consider writing the source file name into the marker file so that we can detect when two
     * callers are trying to extract different files into the same destination directory.
     */
    private final ConcurrentHashMap<Path, CompletableFuture<Void>> wipExtractions;

    private final int maxExtractionWaitSeconds;

    private final FileSystem fileSystem;

    private ArchiveSerDes() {
        this(DEFAULT_MAX_EXTRACTION_WAIT_SECONDS);
    }

    public ArchiveSerDes(int maxExtractionWaitSeconds) {
        this(FileSystem.SINGLETON, maxExtractionWaitSeconds, new ConcurrentHashMap<>());
    }

    @VisibleForTesting
    ArchiveSerDes(FileSystem fileSystem, int maxExtractionWaitSeconds, ConcurrentHashMap<Path, CompletableFuture<Void>> wipExtractions) {
        Validate.isTrue(maxExtractionWaitSeconds > 0, "maxExtractionWaitSeconds must be positive.", maxExtractionWaitSeconds);
        this.maxExtractionWaitSeconds = maxExtractionWaitSeconds;
        this.wipExtractions = wipExtractions;
        this.fileSystem = fileSystem;
    }

    /**
     * Extracts a .tar.gz (same as .tgz) file to the desired destination. Since Java does not support atomic move of directories,
     * this method uses a file to mark whether extraction has been successfully completed. If the marker file exists, then the method
     * reuses the extracted directory.
     */
    public void extractTgz(final Path srcTgzFile, final Path destExtractionPath) {
        CompletableFuture<Void> future = extractInternal(srcTgzFile, destExtractionPath, (src, dst) -> extractTgzInternal(src, dst));
        waitForFuture(srcTgzFile, destExtractionPath, future);
    }

    /**
     * This method is similar to {@link #extractTgz(Path, Path)} except it operates on .zip files.
     */
    public void extractZip(final Path srcZipFile, final Path destExtractionPath) {
        CompletableFuture<Void> future = extractInternal(srcZipFile, destExtractionPath, (src, dst) -> extractZipInternal(src, dst));
        waitForFuture(srcZipFile, destExtractionPath, future);
    }

    @VisibleForTesting
    CompletableFuture<Void> extractInternal(final Path srcFile, final Path destExtractionPath, BiConsumer<Path, Path> extractor) {
        String debugMsgSuffix = " src: " + srcFile + " dst: " + destExtractionPath;
        Preconditions.checkArgument(srcFile.toFile().exists(), "Source file does not exist." + debugMsgSuffix);
        Preconditions.checkArgument(srcFile.toFile().isFile(), "Source file is not a normal file." + debugMsgSuffix);

        CompletableFuture<Void> resultFuture = new CompletableFuture<>();

        // If the directory already has extraction marker, then there's nothing to do.
        Path extractionCompletionMarkerPath = getExtractionCompletionMarkerPath(srcFile.toAbsolutePath(), destExtractionPath);
        if (fileSystem.exists(extractionCompletionMarkerPath.toFile())) {
            log.info("File already extracted. Returning without further action." + debugMsgSuffix);
            resultFuture.complete(NULL_VALUE_FOR_FUTURE);
            return resultFuture;
        }

        // There is no extraction marker. Try to get a lock on extracting to this destination directory.
        CompletableFuture<Void> existingFuture = wipExtractions.putIfAbsent(destExtractionPath, resultFuture);

        // Already someone is working on extraction. Discard the resultFuture, it will be GC'ed shortly. Use the existing future.
        if (existingFuture != null) {
            log.info("Another thread beat me in getting the lock to extract to this destination. " +
                    "Will wait for it to complete." + debugMsgSuffix);
            return existingFuture;  // NOTE: If you comment this line out, then testConcurrentExtractions() fails - as expected.
        }

        // It is theoretically possible that a thread could not find marker file, proceeded to get a lock. And meanwhile, another thread
        // came by, acquired lock, extracted the archive and then removed the lock. This double-check guards against that condition.
        if (fileSystem.exists(extractionCompletionMarkerPath.toFile())) {
            log.info("File already extracted. Returning without further action." + debugMsgSuffix);
            resultFuture.complete(NULL_VALUE_FOR_FUTURE);
            return resultFuture;
        }

        // Now, we're all set to extract the archive.
        try {
            File destExtractionFile = destExtractionPath.toFile();
            if (fileSystem.exists(destExtractionFile)) {
                log.warn("Destination directory already exists but extraction marker file does not exist. " +
                        "Files will be overwritten." + debugMsgSuffix);
            }

            mkdirs(destExtractionFile);
            extractor.accept(srcFile, destExtractionPath);

            if (!fileSystem.createNewFile(extractionCompletionMarkerPath.toFile())) {
                throw new ArchiveExtractionException("Unable to create extraction completion marker file." + debugMsgSuffix);
            }

            log.debug("Successfully extracted file." + debugMsgSuffix);
            resultFuture.complete(NULL_VALUE_FOR_FUTURE);
        } catch (Exception e) {
            resultFuture.completeExceptionally(e);
        } finally {
            wipExtractions.remove(destExtractionPath);
        }
        return resultFuture;
    }

    private void extractTgzInternal(final Path srcTgzFile, final Path destExtractionPath) {
        try (FileInputStream fis = fileSystem.newFileInputStream(srcTgzFile.toFile());
                GzipCompressorInputStream gzipIn = new GzipCompressorInputStream(fis);
                TarArchiveInputStream tarIn = new TarArchiveInputStream(gzipIn)) {
            TarArchiveEntry entry;
            while ((entry = (TarArchiveEntry) tarIn.getNextEntry()) != null) {
                File f = destExtractionPath.resolve(entry.getName()).toFile();
                if (entry.isDirectory()) {
                    mkdirs(f);
                } else {
                    mkdirs(f.getParentFile());
                    try (FileOutputStream fos = fileSystem.newFileOutputStream(
                            destExtractionPath.resolve(entry.getName()).toFile(),
                            false)) {
                        fileSystem.copy(tarIn, fos);
                    }
                }
            }
        } catch (IOException e) {
            throw new ArchiveExtractionException("Failed to extract from: " + srcTgzFile, e);
        }
    }

    private void extractZipInternal(final Path srcZipFile, final Path destExtractionPath) {
        try (ZipFile zipFile = fileSystem.newZipFile(srcZipFile.toFile())) {
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                File entryDestination = destExtractionPath.resolve(entry.getName()).toFile();
                if (entry.isDirectory()) {
                    mkdirs(entryDestination);
                } else {
                    mkdirs(entryDestination.getParentFile());
                    try (InputStream in = zipFile.getInputStream(entry);
                            OutputStream out = new FileOutputStream(entryDestination)) {
                        fileSystem.copy(in, out);
                    }
                }
            }
        } catch (IOException e) {
            throw new ArchiveExtractionException("Failed to extract from: " + srcZipFile, e);
        }
    }

    @VisibleForTesting
    Path getExtractionCompletionMarkerPath(Path srcFile, Path destDir) {
        return destDir.resolve(COMPLETION_MARKER_FILE);
    }

    private void waitForFuture(Path srcFile, Path dstDir, CompletableFuture<Void> future) {
        try {
            future.get(maxExtractionWaitSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ArchiveExtractionException(String.format("Interrupted while extracting %s to %s", srcFile, dstDir), e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ArchiveExtractionException) {
                throw (ArchiveExtractionException) e.getCause();
            } else {
                throw new ArchiveExtractionException(String.format("Failed to extract %s to %s", srcFile, dstDir),
                        e.getCause() == null ? e : e.getCause());
            }
        } catch (TimeoutException e) {
            throw new ArchiveExtractionException(String.format("Timed out while extracting %s to %s", srcFile, dstDir));
        }
    }

    private void mkdirs(File f) {
        if (!fileSystem.ensureDirExists(f)) {
            throw new ArchiveExtractionException("Unable to create directory: " + f.getAbsolutePath() + " during extraction.");
        }
    }

}