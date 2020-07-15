package concurrency;// Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

import com.amazon.lucene.config.A9FieldType;
import com.amazon.lucene.config.BaseMetadata;
import com.amazon.lucene.config.IndexConfig;
import com.amazon.lucene.index.ConstantFieldNames;
import com.amazon.lucene.index.IndexChunkBase;
import com.amazon.lucene.index.XDFParser;
import com.amazon.lucene.store.IndexInputStream;
import com.amazon.lucene.store.IndexOutputStream;
import com.amazon.lucene.store.LimitInputStream;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.*;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.OfflineSorter;
import org.apache.lucene.util.OfflineSorter.ByteSequencesReader;
import org.apache.lucene.util.ThreadInterruptedException;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.text.ParseException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/** Sorts xdfs by marketplace so all asins in one markeplace are written to orthogonal files. */
public final class SortMarketplaceXDFs {

    private final Path outputRoot;
    private final Path inputRoot;

    static final long START_NS = System.nanoTime();

    private final ExecutorService exec = Executors.newFixedThreadPool(32);
    private final ExecutorService exec2 = Executors.newFixedThreadPool(32);
    //private final ExecutorService exec = Executors.newFixedThreadPool(1);
    //private final ExecutorService exec2 = Executors.newFixedThreadPool(1);

    private final Map<String, Integer> fieldIds;
    private final List<String> fieldNames;
    private final IndexConfig indexConfig;
    private final String stack;
    private final String queryIndex;
    private final String docRoot;
    private static final ConcurrentHashMap<String, String> docidsToFamily = new ConcurrentHashMap<>();

    private SortMarketplaceXDFs(String stack, String queryIndex, String docRoot, IndexConfig indexConfig) throws IOException {
        this.stack = stack;
        this.queryIndex = queryIndex;
        this.docRoot = docRoot;
        outputRoot = Paths.get(docRoot + "/new-bin-" + stack);
        if (Files.exists(outputRoot) == false) {
            Files.createDirectories(outputRoot);
            Files.write(outputRoot.resolve("version"), "2".getBytes(StandardCharsets.UTF_8));
        } else {
            throw new IllegalArgumentException("path \"" + outputRoot + "\" already exists; please remove and rerun");
        }
        inputRoot = Paths.get(docRoot + "/xdf-" + stack);
        this.indexConfig = indexConfig;
        List<String> fieldTypeFieldNames = new ArrayList<>(indexConfig.getFields().keySet());
        Collections.sort(fieldTypeFieldNames);
        // TODO: move the common fields earlier so vInt takes 1 byte?:
        fieldIds = new ConcurrentHashMap<>();
        for (int i = 0; i < fieldTypeFieldNames.size(); i++) {
            fieldIds.put(fieldTypeFieldNames.get(i), i);
        }
        fieldNames = new CopyOnWriteArrayList<>(fieldTypeFieldNames);
    }

    private static final Comparator<BytesRef> COMPARE_MARKETPLACE = new Comparator<BytesRef>() {
            @Override
            public int compare(BytesRef a, BytesRef b) {

                int aLength = a.bytes[a.offset] & 0xFF;
                int bLength = b.bytes[b.offset] & 0xFF;
                long aMarketId = getVLong(a.bytes, a.offset + aLength + 1);
                long bMarketId = getVLong(b.bytes, b.offset + bLength + 1);

                // sort by market place id:
                return Long.compare(aMarketId, bMarketId);
            }
        };

    private static long getVLong(byte[] bytes, int offset) {
        byte b = bytes[offset++];
        long i = b & 0x7F;
        for (int shift = 7; (b & 0x80) != 0; shift += 7) {
            b = bytes[offset++];
            i |= (b & 0x7FL) << shift;
        }
        return i;
    }

    private void sort() throws IOException, InterruptedException {
        Path tmpPath = Paths.get(docRoot + "/tmp");
        Directory dir = FSDirectory.open(tmpPath);

        Path tmpInputPath = tmpPath.resolve("all");

        try {
            dir.deleteFile("all");
        } catch (FileNotFoundException | NoSuchFileException fnfe) {
            // ok
        }

        // fake input file, since we instead open many .xdf files and logically concatenate them:
        try (IndexOutput out = dir.createOutput("all", IOContext.DEFAULT)) {
            CodecUtil.writeFooter(out);
        }

        final AtomicLong totalAsinCount = new AtomicLong();
        final AtomicBoolean inputDone = new AtomicBoolean();

        /*
        ByteSequencesReader reader = new ConcatAllXDFs(dir.openChecksumInput("all", IOContext.DEFAULT), "all",
                                                       Paths.get(docRoot + "/tmpcache"), fieldTypes, totalAsinCount);
        int count = 0;
        while (reader.next() != null) {
            count++;
        }
        System.out.println("DONE: " + count + " asins");
        */

        OfflineSorter.BufferSize bufferSize = OfflineSorter.BufferSize.megabytes(2047);

        String result = new OfflineSorter(dir, "temp", COMPARE_MARKETPLACE, bufferSize, Integer.MAX_VALUE, -1, exec2, 8) {
            @Override
            protected ByteSequencesReader getReader(ChecksumIndexInput in, String name) throws IOException {

                System.out.println("getReader: " + in.toString() + " vs " + tmpInputPath.toString() + " length=" + in.length());
                if (in.toString().contains("path=\"" + tmpInputPath.toString() + "\"")) {
                    //System.out.println("  return concat");
                    return new ConcatAllXDFs(dir.openChecksumInput("all", IOContext.DEFAULT), "all",
                                             inputRoot, indexConfig, totalAsinCount, inputDone);
                } else {
                    // We have to tell GZIPInputStream not to read into the footer (gz encoding is not self delimiting maybe?):
                    InputStream gzIn = new GZIPInputStream(new LimitInputStream(new IndexInputStream(in),
                                                                                in.length() - CodecUtil.footerLength()), 128 * 1024);
                    final InputStreamDataInput dataIn = new InputStreamDataInput(gzIn);

                    return new ByteSequencesReader(in, name) {
                        private byte[] bytes = new byte[2048];
                        private final BytesRef scratch = new BytesRef();

                        @Override
                        public BytesRef next() throws IOException {
                            //System.out.println("reader.next");
                            int numBytes;
                            try {
                                numBytes = dataIn.readVInt();
                                //System.out.println("  numBytes=" + numBytes);
                            } catch (EOFException eofe) {
                                return null;
                            }
                            //System.out.println("  numBytes=" + numBytes);
                            bytes = ArrayUtil.grow(bytes, numBytes);
                            dataIn.readBytes(bytes, 0, numBytes);
                            scratch.bytes = bytes;
                            scratch.length = numBytes;
                            return scratch;
                        }

                        @Override
                        public void close() throws IOException {
                            in.close();
                        }
                    };
                }
            }

            class WriteOneFinalFile implements Callable<Void> {
                private final Path pathOut;
                private final List<byte[]> chunks;
                private final Semaphore writeSemaphore;
                private final AtomicInteger finishedFileCount;

                public WriteOneFinalFile(Path pathOut, List<byte[]> chunks, Semaphore writeSemaphore,
                                         AtomicInteger finishedFileCount) throws InterruptedException {
                    this.chunks = chunks;
                    this.pathOut = pathOut;
                    this.writeSemaphore = writeSemaphore;
                    writeSemaphore.acquire();
                    this.finishedFileCount = finishedFileCount;
                }

                @Override
                public Void call() throws IOException {
                    try (GZIPOutputStream gzOut = new GZIPOutputStream(Files.newOutputStream(pathOut), 128 * 1024)) {
                        DataOutput dataOut = new OutputStreamDataOutput(gzOut);
                        for (byte[] chunk : chunks) {
                            if (chunk.length == 0) {
                                System.out.println("skipping zero-length chunk");
                            } else {
                                dataOut.writeVInt(chunk.length);
                                dataOut.writeBytes(chunk, 0, chunk.length);
                            }
                        }
                        // end marker for current file:
                        dataOut.writeVInt(0);
                    } finally {
                        writeSemaphore.release();
                    }
                    finishedFileCount.incrementAndGet();
                    return null;
                }
            }

            @Override
            protected ByteSequencesWriter getWriter(final IndexOutput out, final long itemCount) throws IOException {

                System.out.println("getWriter pathOut=" + out.getName() + " itemCount=" + itemCount);
                System.out.println("inputDone=" + inputDone.get() + " totalAsins=" + totalAsinCount.get());
                if (inputDone.get() && itemCount == totalAsinCount.get()) {
                    System.out.println("  is last!!");
                    // for the final output we 1) aggregate docs into chunks ~2 MB each, and 2) break into many smallish output files for
                    // easier addressibility e.g. when trying to view atts for one ASIN:

                    return new ByteSequencesWriter(out) {
                        private int fileUpto;
                        private final ByteArrayOutputStream chunkOut = new ByteArrayOutputStream();
                        private final DataOutput chunkDataOut = new OutputStreamDataOutput(chunkOut);
                        private long asinCount;
                        private long pendingNumBytes;
                        private byte[] lastFamily;
                        private long lastMarketplaceId = -1;
                        private final List<byte[]> pendingChunks = new ArrayList<>();
                        private final Semaphore writeSemaphore = new Semaphore(16);
                        private final AtomicInteger finishedFileCount = new AtomicInteger();
                        private long lastMarketplaceAsinCount;

                        @Override
                        public void write(byte[] bytes, int off, int len) throws IOException {
                            int familyLen = bytes[off] & 0xFF;
                            byte[] family;
                            if (familyLen == 0) {
                                family = null;
                            } else {
                                family = new byte[familyLen];
                                System.arraycopy(bytes, off + 1, family, 0, familyLen);
                                //System.out.println("FAMILY: " + new String(family, StandardCharsets.UTF_8));
                            }
                            long marketplaceId = getVLong(bytes, off + familyLen + 1);
                            if (marketplaceId < lastMarketplaceId) {
                                throw new AssertionError("marketplaceId illegal went backwards from "
                                                         + lastMarketplaceId + " to " + marketplaceId);
                            }

                            boolean newMarketplace = marketplaceId != lastMarketplaceId;

                            // always split chunks at a new marketplace, and otherwise only split chunks at family boundaries:
                            if (newMarketplace || ((family == null || (lastFamily != null && Arrays.equals(family, lastFamily) == false))
                                                   && chunkOut.size() > 2 * 1024 * 1024)) {
                                byte[] chunk = chunkOut.toByteArray();
                                pendingChunks.add(chunk);

                                chunkOut.reset();
                                pendingNumBytes += chunk.length;

                                // TODO: we could also make this part concurrent...

                                // uncompressed bytes:
                                if ((newMarketplace && pendingNumBytes > 0) || pendingNumBytes > 200 * 1024 * 1024) {

                                    // start a new file:
                                    System.out.println("start new output file @ asinCount=" + asinCount
                                                       + " marketplaceId=" + marketplaceId);

                                    WriteOneFinalFile job;
                                    try {
                                        job = new WriteOneFinalFile(outputRoot.resolve("final." + lastMarketplaceId
                                                                                        + "." + (fileUpto++) + ".gz"),
                                                                    new ArrayList<>(pendingChunks),
                                                                    writeSemaphore,
                                                                    finishedFileCount);
                                    } catch (InterruptedException ie) {
                                        throw new ThreadInterruptedException(ie);
                                    }
                                    exec.submit(job);
                                    pendingChunks.clear();
                                    pendingNumBytes = 0;
                                    if (newMarketplace) {
                                        fileUpto = 0;
                                        System.out.println("  marketplace " + lastMarketplaceId + " had "
                                                           + (asinCount - lastMarketplaceAsinCount) + " asins");
                                        lastMarketplaceAsinCount = asinCount;
                                    }
                                }
                            }
                            lastFamily = family;
                            lastMarketplaceId = marketplaceId;

                            chunkDataOut.writeVInt(len);
                            chunkDataOut.writeBytes(bytes, off, len);
                            asinCount++;
                        }

                        @Override
                        public void close() throws IOException {
                            byte[] chunk = chunkOut.toByteArray();
                            if (chunk.length > 0) {
                                pendingChunks.add(chunk);
                            }
                            if (pendingChunks.size() > 0) {
                                WriteOneFinalFile job;
                                try {
                                    job = new WriteOneFinalFile(outputRoot.resolve("final." + lastMarketplaceId + "."
                                                                                   + (fileUpto++) + ".gz"),
                                                                new ArrayList<>(pendingChunks),
                                                                writeSemaphore,
                                                                finishedFileCount);
                                } catch (InterruptedException ie) {
                                    throw new ThreadInterruptedException(ie);
                                }
                                exec.submit(job);
                            }

                            chunkOut.reset();
                            pendingChunks.clear();

                            // TODO: don't poll, use condition/phaser/something!
                            while (finishedFileCount.get() < fileUpto) {
                                try {
                                    Thread.sleep(100);
                                } catch (InterruptedException ie) {
                                    throw new ThreadInterruptedException(ie);
                                }
                            }

                            // close original (unused) output passed to us by OfflineSorter:
                            out.close();

                            // TODO: assert asinCount is correct
                            System.out.println("FINAL: " + asinCount + " output asins in " + fileUpto + " files");
                        }
                    };
                } else {
                    return new ByteSequencesWriter(out) {
                        private GZIPOutputStream gzOut = new GZIPOutputStream(new IndexOutputStream(out), 128 * 1024);
                        private OutputStreamDataOutput dataOut = new OutputStreamDataOutput(gzOut);
                        private long count;

                        @Override
                        public void write(byte[] bytes, int off, int len) throws IOException {
                            dataOut.writeVInt(len);
                            dataOut.writeBytes(bytes, off, len);
                            count++;
                            assert count <= itemCount;
                            if (count == itemCount) {
                                //System.out.println("gzout finish " + out.getName());
                                gzOut.finish();
                            }
                        }

                        @Override
                        public void close() throws IOException {
                            out.close();
                            //System.out.println("closeWriter pathOut=" + out.getName() + " fp=" + out.getFilePointer());
                        }
                    };
                }
            }
        }.sort("all");
        System.out.println("DONE SORT");

        dir.deleteFile(result);
        exec.shutdownNow();
        exec2.shutdownNow();

        System.out.println("Done!");
    }

    private static void writeInt(DataOutput out, int fieldId, int value) throws IOException {
        int code = (fieldId << 2);
        out.writeVInt(code);
        out.writeVInt(value);
    }

    private static void writeLong(DataOutput out, int fieldId, long value) throws IOException {
        int code = (fieldId << 2) | 0x1;
        out.writeVInt(code);
        out.writeZLong(value);
    }

    private static void writeString(DataOutput out, int fieldId, String value) throws IOException {
        int code = (fieldId << 2) | 0x2;
        out.writeVInt(code);
        out.writeString(value);
    }

    private static void writeSourceString(DataOutput out, int fieldId, int sourceFieldId, String value) throws IOException {
        int code = (fieldId << 2) | 0x3;
        out.writeVInt(code);
        out.writeVInt(sourceFieldId);
        out.writeString(value);
    }

    private static final byte[] END_DOCUMENT_BYTES = "</document>".getBytes(StandardCharsets.UTF_8);

    /** Reads one text XDF file into byte[] documents. */
    private class LoadOneXDF implements Callable<Void> {
        private final Path xdfPath;
        private final BlockingQueue<byte[][]> outputQueue;
        private final AtomicLong totalAsinCount;
        private final AtomicInteger totalFileCount;
        private final int rootPrefixLength;
        //private final List<String> browseValues = new ArrayList<>();
        //private final List<String> subjectBinValues = new ArrayList<>();

        public LoadOneXDF(AtomicLong totalAsinCount, AtomicInteger totalFileCount, Path xdfRoot,
                          Path xdfPath, BlockingQueue<byte[][]> outputQueue) {
            this.totalAsinCount = totalAsinCount;
            this.totalFileCount = totalFileCount;
            this.rootPrefixLength = xdfRoot.toString().length();
            this.xdfPath = xdfPath;
            this.outputQueue = outputQueue;
        }

        @Override
        public Void call() throws Exception {
            try {
                return innerCall();
            } catch (Throwable t) {
                System.out.println("CALL FAILED: " + xdfPath);
                t.printStackTrace(System.out);
                throw new RuntimeException(t);
            }
        }

        private Void innerCall() throws IOException, ParseException, InterruptedException {

            int docCount = 0;

            // reserved field name to mark proffers following the document's fields
            int nextProfferFieldId = getFieldId("_next_proffer");
            int xdfFileNameFieldId = getFieldId("_xdf_file_name");

            //System.out.println("process file " + xdfPath);
            //try (InputStream in = new GZIPInputStream(Files.newInputStream(xdfPath, StandardOpenOption.READ), 128 * 1024)) {
            try (InputStream in = Files.newInputStream(xdfPath, StandardOpenOption.READ)) {

                int pendingNumBytes = 0;
                List<byte[]> pending = new ArrayList<>();

                String xdfPathString = xdfPath.toString();
                String xdfFileName = xdfPathString.substring(rootPrefixLength + 1, xdfPathString.length());

                Map<String, List<String>> proffers = new HashMap<>();

                ByteArrayOutputStream out = new ByteArrayOutputStream();
                DataOutput dataOut = new OutputStreamDataOutput(out);

                byte[] lastBuffer = null;
                int lastEnd = 0;
                boolean eof = false;
                int skipCount = 0;
                while (eof == false) {
                    //System.out.println("  read next chunk");
                    int lastEndFragmentLength;
                    if (lastBuffer == null) {
                        lastEndFragmentLength = 0;
                    } else {
                        lastEndFragmentLength = lastBuffer.length - lastEnd;
                    }

                    byte[] buffer = new byte[1024 * 1024 + lastEndFragmentLength];
                    if (lastEndFragmentLength != 0) {
                        // copy ending fragment from last buffer:
                        System.arraycopy(lastBuffer, lastBuffer.length - lastEndFragmentLength, buffer, 0, lastEndFragmentLength);
                    }
                    lastBuffer = buffer;

                    int upto = lastEndFragmentLength;
                    while (upto < buffer.length) {
                        int count = in.read(buffer, upto, buffer.length - upto);
                        if (count == -1) {
                            // eof
                            eof = true;
                            break;
                        }
                        upto += count;
                    }
                    if (upto != buffer.length) {
                        // we hit eof:
                        byte[] newBuffer = new byte[upto];
                        System.arraycopy(buffer, 0, newBuffer, 0, newBuffer.length);
                        buffer = newBuffer;
                    }

                    int end = findEndFragment(buffer);
                    if (end == -1) {
                        // whole buffer is still inside one document
                        lastEnd = 0;
                        continue;
                    }
                    //System.out.println("end: " + end + " vs " + buffer.length);

                    lastEnd = end;

                    XDFParser parser = new XDFParser(buffer, 0, end);
                    //System.out.println("FRAGMENT: " + new String(Arrays.copyOfRange(buffer, 0, end), StandardCharsets.UTF_8));
                    //System.out.println("SUFFIX: " + new String(Arrays.copyOfRange(buffer, end, buffer.length), StandardCharsets.UTF_8));
                    while (parser.done() == false) {

                        String serviceId = parser.getDocServiceID();
                        if (serviceId.equals("item") || serviceId.equals("1")) {
                            // ok
                        } else if (serviceId.equals("offering")) {
                            //isItem = false;
                            //docDataOut.writeByte((byte) 1);
                            parser.skipDocument();
                            skipCount++;
                            continue;
                        } else if (serviceId.equals("dummy")) {
                            // what is this :)
                            parser.skipDocument();
                            skipCount++;
                            continue;
                        } else {
                            System.out.println("SKIP service-id=" + serviceId);
                            parser.skipDocument();
                            skipCount++;
                            continue;
                        }

                        String origDocId = parser.getDocID();
                        if (docidsToFamily.putIfAbsent(origDocId, "") != null) {
                            // duplicate
                            parser.skipDocument();
                            skipCount++;
                            //System.out.println("do skip " + origDocId);
                            continue;
                        }
                        //System.out.println("do keep " + origDocId);
                        long marketplaceId;
                        int idx = origDocId.indexOf(':');
                        String asin;
                        if (idx == -1) {
                            // US
                            marketplaceId = 1;
                            asin = origDocId;
                        } else {
                            int idx2 = origDocId.indexOf(':', idx + 1);
                            String marketplaceIdString;
                            if (idx2 != -1) {
                                marketplaceIdString = origDocId.substring(idx + 1, idx2);
                            } else {
                                marketplaceIdString = origDocId.substring(idx + 1);
                            }
                            asin = origDocId.substring(0, idx);
                            try {
                                marketplaceId = Long.parseLong(marketplaceIdString);
                            } catch (NumberFormatException nfe) {
                                System.out.println("ERROR: unable to parse marketplaceId " + marketplaceIdString
                                                   + " as long; skipping docId=" + origDocId + " from file=" + xdfPath);
                                parser.skipDocument();
                                skipCount++;
                                continue;
                            }
                        }
                        //System.out.println("header marketplace id " + marketplaceId);

                        ByteArrayOutputStream docOut = new ByteArrayOutputStream(2048);
                        OutputStreamDataOutput docDataOut = new OutputStreamDataOutput(docOut);

                        docDataOut.writeVLong(marketplaceId);

                        // The 1: prefix is the service-id; it can (very rarely) also be 4!
                        docDataOut.writeString(origDocId);
                        //subjectBinValues.clear();
                        //browseValues.clear();

                        // LUCENE-297: record the original XDF file name
                        writeString(docDataOut, xdfFileNameFieldId, xdfFileName);

                        BytesRef veFamilyId = null;
                        boolean matchesQueryIndex = false;
                        int attrCount = 0;
                        while (parser.nextAttr()) {
                            String value = parser.getAttrValue();
                            //System.out.println("  " + parser.getAttrName() + "=" + value);
                            attrCount++;
                            if (value.length() > 0) {
                                String fieldName = parser.getAttrName();
                                if (fieldName.equals("query-indexes")) {
                                    if (queryIndex != null && value.equals(queryIndex)) {
                                        matchesQueryIndex = true;
                                    }
                                } else if (fieldName.equals("ve-family-id") && veFamilyId == null) {
                                    veFamilyId = new BytesRef(value);
                                    docidsToFamily.put(origDocId, value);
                                } else if (fieldName.equals(ConstantFieldNames.MARKETPLACE_ID)) {
                                    //System.out.println("  att marketplace id " + marketplaceId);

                                    // sanity check
                                    // TODO: one is encoded the other is straight?  e.g.:
                                    //    java.lang.AssertionError: marketplaceId in header is 710497755 but in attr is 6
                                    if (false && Long.parseLong(value) != marketplaceId) {
                                        throw new AssertionError("marketplaceId in header is " + marketplaceId
                                                                 + " but in attr is " + value);
                                    }
                                } else if (fieldName.startsWith("proffer/")) {
                                    // e.g. proffer/x1-3b445cbb90bfdc58422b3a9bddee722f/price-inverse

                                    // Just buffer up proffers in first pass:
                                    String[] parts = fieldName.split("/");
                                    if (parts.length != 3) {
                                        throw new IllegalArgumentException("expected 3 parts but got " + parts.length
                                                                           + " for " + fieldName);
                                    }
                                    String profferId = parts[1];
                                    String profferFieldName = parts[2];
                                    List<String> profferValues = proffers.get(profferId);
                                    if (profferValues == null) {
                                        profferValues = new ArrayList<>();
                                        proffers.put(profferId, profferValues);
                                    }
                                    profferValues.add(profferFieldName);
                                    profferValues.add(value);
                                    continue;
                                }

                                try {
                                    writeField(parser, asin, docDataOut, fieldName, value);
                                } catch (IllegalArgumentException iae) {
                                    System.out.println("ERROR: docId=" + origDocId + " batch=" + xdfPath + " field \""
                                                       + fieldName + "\", value \"" + value + "\": "
                                                       + iae.getMessage() + "; skipping this field");
                                }
                            }
                        }
                        if (queryIndex != null && matchesQueryIndex == false) {
                            proffers.clear();
                            continue;
                        }

                        if (attrCount == 0) {
                            // This can happen from GCR CaptureWriter when a document is deleted -- we just skip the document here
                            System.out.println("SKIP DELETED DOC: " + origDocId);
                            continue;
                        }

                        int totProfferFieldCount = 0;
                        for (Map.Entry<String, List<String>> ent : proffers.entrySet()) {
                            // we ignore the profferID; it's only used to uniquely identify which proffer attributes go together

                            // TODO: we can save a bit of space and not write this empty string, just the fieldId:
                            writeString(docDataOut, nextProfferFieldId, "");

                            List<String> fieldValues = ent.getValue();
                            totProfferFieldCount += fieldValues.size() / 2;
                            //System.out.println("proffer " + ent.getKey() + " file " + xdfPath);
                            for (int i = 0; i < fieldValues.size(); i += 2) {
                                String fieldName = fieldValues.get(i);
                                String value = fieldValues.get(i + 1);
                                //System.out.println("  " + fieldName + "=" + value);
                                try {
                                    writeField(null, null, docDataOut, fieldName, value);
                                } catch (IllegalArgumentException iae) {
                                    System.out.println("ERROR: docId=" + origDocId + " batch=" + xdfPath + " field \""
                                                       + fieldName + "\", value \"" + value + "\": "
                                                       + iae.getMessage() + "; skipping this field");
                                }
                            }
                        }
                        /*
                        if (totProfferFieldCount > 1000) {
                            System.out.println(proffers.size() + " proffers with " + totProfferFieldCount + " total fields");
                        }
                        */
                        proffers.clear();

                        byte[] bytes = docOut.toByteArray();
                        byte[] familyIdBytes = getFamilyBytes(xdfPath, origDocId, veFamilyId);

                        // concat ve-family-id and doc bytes:
                        byte[] finalBytes = new byte[familyIdBytes.length + bytes.length];
                        System.arraycopy(familyIdBytes, 0, finalBytes, 0, familyIdBytes.length);
                        System.arraycopy(bytes, 0, finalBytes, familyIdBytes.length, bytes.length);

                        pending.add(finalBytes);
                        pendingNumBytes += finalBytes.length;
                        docCount++;
                        //System.out.println("add " + finalBytes.length + " to " + pendingNumBytes);

                        if (pendingNumBytes > 2 * 1024 * 1024) {
                            //System.out.println("add pending " + pendingNumBytes);
                            outputQueue.put(pending.toArray(new byte[0][]));
                            pending.clear();
                            pendingNumBytes = 0;
                        }
                    }
                    //System.out.println("parser done");
                }
                //System.out.println("eof");

                if (pending.size() > 0) {
                    //System.out.println("add pending " + pendingNumBytes);
                    outputQueue.put(pending.toArray(new byte[0][]));
                    pending.clear();
                    //pendingNumBytes = 0;
                }
            }

            long curDocCount = totalAsinCount.get();
            double sec = (System.nanoTime() - START_NS) / 1000000000.0;
            System.out.println(String.format(Locale.ROOT, "%6.1fs %d files, %.1fM docs, %.1fK docs/sec",
                                             sec, totalFileCount.get(), curDocCount / 1000000., curDocCount / 1000. / sec));
            totalAsinCount.addAndGet(docCount);
            totalFileCount.incrementAndGet();
            //System.out.println("finish: " + xdfPath);
            return null;
        }

        private byte[] getFamilyBytes(Path src, String docId, BytesRef veFamilyId) throws IOException {
            ByteArrayOutputStream familyIdOut = new ByteArrayOutputStream(16);
            OutputStreamDataOutput familyIdDataOut = new OutputStreamDataOutput(familyIdOut);
            if (veFamilyId == null) {
                familyIdDataOut.writeByte((byte) 0);
            } else if (veFamilyId.length > 255) {
                throw new IllegalArgumentException("ve-family-id is " + veFamilyId.utf8ToString()
                                                   + " for docId=" + docId + " in file " + src);
            } else {
                familyIdDataOut.writeByte((byte) veFamilyId.length);
                familyIdDataOut.writeBytes(veFamilyId.bytes, veFamilyId.offset, veFamilyId.length);
            }
            return familyIdOut.toByteArray();
        }

        private int findEndFragment(byte[] bytes) {
            // TODO: this is naive search; there are more efficient algos!  But it's fine for this offline tool run rarely...
            outer:
            for (int i = bytes.length - 1; i >= END_DOCUMENT_BYTES.length; i--) {
                for (int j = 0; j < END_DOCUMENT_BYTES.length; j++) {
                    if (bytes[i - END_DOCUMENT_BYTES.length + j] != END_DOCUMENT_BYTES[j]) {
                        continue outer;
                    }
                }

                return i + 1;
            }

            return -1;
        }

        private void writeField(XDFParser parser, String asin, DataOutput out,
                                String fieldName, String value) throws IOException {

            Integer fieldId = fieldIds.get(fieldName);
            if (fieldName.contains("fitment")) {
                // TODO:
                //fieldId = getFieldId(fieldName);
                //writeString(out, fieldId, value);
                return;
            }

            switch (fieldName) {
                // the <build-index name="single-index"> skips these:
                case "author-strip":
                case "brandtextbin-jp":
                case "ean":
                case "si-strip-common-body":
                case "subject-strip":
                case "title-strip":
                case "upc":
                case "sku":
                    break;
                // these seem special cased?
                case "a9-doc-version":
                case "_language":
                case "service-number":
                case "service-id":
                    break;
                case "ve-family-id":
                    value = fixVEFamilyID(value);
                    writeString(out, fieldId, value);
                    break;
                case "ve-family-id2":
                    value = fixVEFamilyID(value);
                    writeString(out, fieldId, value);
                    break;
                case "ve-family-id3":
                    value = fixVEFamilyID(value);
                    writeString(out, fieldId, value);
                    break;
                case "single-index-body":
                    String attrSource = parser.getAttrSource();
                    int sourceFieldId = getFieldId(attrSource);
                    writeSourceString(out, fieldId, sourceFieldId, value);
                    break;
                case "is_hdr_video":
                    long longValue = parseLong(value);
                    writeLong(out, fieldId, longValue);
                    break;
                case "original_air_date":
                    try {
                        longValue = Long.parseLong(value);
                    } catch (NumberFormatException nfe) {
                        // Sometimes this field is seconds-since-epoch, other times it's an ISO 8601 date:
                        longValue = Instant.parse(value).getEpochSecond();
                    }
                    writeLong(out, fieldId, longValue);
                    break;
                case "plan_included_data":
                    longValue = parseLong(value);
                    writeLong(out, fieldId, longValue);
                    break;
                case "unit-sales":
                    // single valued
                    int unitSales = Integer.parseInt(value);
                    if (unitSales < 0) {
                        throw new IllegalArgumentException("unit-sales has invalid negative value " + unitSales);
                    }
                    writeInt(out, fieldId, unitSales);
                    break;
                case "unpopularity":
                    int unpopularity = Integer.parseInt(value);
                    writeInt(out, fieldId, unpopularity);
                    break;
                case "subjectbin":
                    //subjectBinValues.add(value);
                    //writeString(out, fieldId, value);
                    break;
                case "browse":
                    // TODO: only special casing this until we better handle the XBM tags
                    writeString(out, fieldId, value);
                    // dup field of subject bin
                    //browseValues.add(value);
                    break;
                case ConstantFieldNames.DOCID:
                    // sanity check:
                    /*
                    if (isItem && value.equals(docId) == false) {
                        throw new IllegalArgumentException("document header said docid=\"" + docId
                                                           + "\" but docid attr is \"" + value + "\"");
                    }
                    */
                    if (fieldId == null) {
                        fieldId = getFieldId(fieldName);
                    }
                    writeString(out, fieldId, value);
                    break;
                case "tc":
                case "tp":
                case "ep":
                case "ec":
                case "ta":
                case "ea":
                case "pte":
                case "coec":
                case "aoea":
                case "poep":
                    writeString(out, fieldId, value);
                    break;
                default:
                    // now check product-search.xbm configuration:
                    if (fieldId == null) {
                        fieldId = getFieldId(fieldName);
                    }
                    writeTypedField(out, fieldId, value);
                    break;
            }
        }

        private void writeTypedField(DataOutput out, int fieldId, String value) throws IOException {
            String fieldName = fieldNames.get(fieldId);
            BaseMetadata.Field field = indexConfig.getField(fieldName);
            if (field == null) {
                writeString(out, fieldId, value);
                return;
            }
            A9FieldType fieldType = field.type();
            switch (fieldType) {
                case meta:
                    // Meta fields are not indexed
                    writeString(out, fieldId, value);
                    break;
                case uint:
                    long longValue = parseLong(value);
                    writeLong(out, fieldId, longValue);
                    break;
                case bin:
                    // TODO: bin-native can apparently be multi-valued?
                case searchable:
                case literal:
                    writeString(out, fieldId, value);
                    break;
                default:
                    throw new IllegalArgumentException("unknown A9FieldType: " + fieldType);
            }
        }

        // For some reason, some asins have odd family id, e.g. asin B002WC80JW has ve-family-id B00009WBZ6:Standard
        // (see http://stack-tools.na-prod.search.amazon.com/sdm.cgi?docid=B002WC80JW)
        private String fixVEFamilyID(String value) {
            int i = value.indexOf(':');
            if (i != -1) {
                return value.substring(0, i);
            } else {
                return value;
            }
        }

        private long parseLong(String value) {
            return IndexChunkBase.parseLong(value, null);
        }
    }
        
    private int getFieldId(String name) throws IOException {
        Integer fieldId = fieldIds.get(name);
        if (fieldId != null) {
            return fieldId;
        }
        return innerGetFieldId(name);
    }

    private synchronized int innerGetFieldId(String name) throws IOException {
        Integer fieldId = fieldIds.get(name);
        if (fieldId == null) {
            // the attr source=... are not necessarily defined fields in product-search.abm
            fieldId = fieldNames.size();
            //System.out.println("FIELD " + name + " -> ID " + fieldId);
            fieldNames.add(name);
            fieldIds.put(name, fieldId);

            StringBuilder b = new StringBuilder();
            for (String fieldName : fieldNames) {
                b.append(fieldName);
                b.append('\n');
            }
            Files.write(outputRoot.resolve("field_names.txt.new"),
                        b.toString().getBytes(StandardCharsets.UTF_8));
            Files.move(outputRoot.resolve("field_names.txt.new"),
                       outputRoot.resolve("field_names.txt"),
                       StandardCopyOption.REPLACE_EXISTING);
        }
        return fieldId;
    }

    /** Logically appends all XDFS for sorting. */
    private class ConcatAllXDFs extends ByteSequencesReader {
        // each entry is ~2 MB total:
        private final BlockingQueue<byte[][]> docQueue = new ArrayBlockingQueue<>(50);
        private final int expectedFileCount;
        private final AtomicInteger actualFileCount = new AtomicInteger();
        private final AtomicLong totalAsinCount;
        private final AtomicBoolean inputDone;
        private byte[][] cur;
        private int curUpto;
        private BytesRef scratch = new BytesRef();

        public ConcatAllXDFs(ChecksumIndexInput in, String name, Path xdfRoot,
                             IndexConfig indexConfig, AtomicLong totalAsinCount,
                             AtomicBoolean inputDone) throws IOException {
            super(in, name);
            int fileCount = 0;
            this.totalAsinCount = totalAsinCount;
            this.inputDone = inputDone;
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(xdfRoot)) {
                for (Path entry: stream) {
                    if (entry.getFileName().toString().startsWith("p")) {
                        try (DirectoryStream<Path> stream2 = Files.newDirectoryStream(entry)) {
                            for (Path entry2: stream2) {
                                exec.submit(new LoadOneXDF(totalAsinCount, actualFileCount, xdfRoot, entry2, docQueue));
                                fileCount++;
                            }
                        }
                    }
                }
            }
            expectedFileCount = fileCount;
            System.out.println("found " + expectedFileCount + " XDFs to process");
        }

        @Override
        public BytesRef next() {
            while (true) {
                if (cur == null || curUpto == cur.length) {
                    try {
                        cur = docQueue.poll(100, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException ie) {
                        throw new ThreadInterruptedException(ie);
                    }
                    if (cur == null) {
                        if (false && actualFileCount.get() >= 300) {
                            System.out.println("STOPPING EARLY");
                            return null;
                        }
                        if (actualFileCount.get() == expectedFileCount) {
                            System.out.println(String.format(Locale.ROOT, "DONE parsing %d XDF files; total %d "
                                                             + "asins input; total time %.1f sec",
                                                             actualFileCount.get(), totalAsinCount.get(),
                                                             (System.nanoTime() - START_NS) / 1000000000.0));
                            inputDone.set(true);
                            return null;
                        } else {
                            continue;
                        }
                    }
                    curUpto = 0;
                }
                scratch.bytes = cur[curUpto++];
                scratch.length = scratch.bytes.length;
                //System.out.println("concat next: " + scratch.length);
                return scratch;
            }
        }
    }

    public static void main(String[] args) throws IOException, SAXException, InterruptedException {
        String stack = args[0];
        // e.g. /local/l:
        String docRoot = args[1];
        String queryIndex = null;
        if (args.length >= 3) {
            queryIndex = args[2];
            if (queryIndex.equals("none")) {
                queryIndex = null;
            }
        }
        String xbmPath;
        if (args.length >= 4) {
            xbmPath = args[3];
        } else {
            xbmPath = "../A9SearchLucene/conf/product-search.xbm";
        }
        IndexConfig indexConfig = new IndexConfig(BaseMetadata.read(Paths.get(xbmPath)));
        SortMarketplaceXDFs sorter = new SortMarketplaceXDFs(stack, queryIndex, docRoot, indexConfig);
        sorter.sort();

        System.out.println("Writing docids + families to ./docids.txt");
        try (OutputStream out = Files.newOutputStream(Paths.get("docids.txt"))) {
            for (Map.Entry<String, String> ent : docidsToFamily.entrySet()) {
                out.write((ent.getKey() + '\t' + ent.getValue() + '\n').getBytes(StandardCharsets.UTF_8));
            }
        }
        System.out.println("Done!");
    }
}