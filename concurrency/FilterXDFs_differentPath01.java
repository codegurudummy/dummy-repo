package concurrency;// Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

/** Filters non-binary xdfs so as to include only those whose asins are listed in an external file.
 *
 * usage: FilterBinaryXDFs {input-folder} {output-folder} {asins-file}
 */
public final class FilterXDFs {

    private FilterXDFs() {
        // no
    }

    static final ConcurrentHashMap<String, Path> DOCID_SEEN = new ConcurrentHashMap<>();

    static final Pattern DOCID_PATTERN = Pattern.compile(".* docid=\"([a-zA-Z0-9:]+)\".*");

    /** One job to filter one file by docid. */
    private static class Job implements Callable<Void> {
        private final Path inpath;
        private final Set<String> docids;
        private final AtomicInteger docidReadCount;
        private final AtomicInteger docidWriteCount;

        public Job(Path inpath, Set<String> docids, AtomicInteger docidReadCount, AtomicInteger docidWriteCount) {
            this.inpath = inpath;
            this.docids = docids;
            this.docidReadCount = docidReadCount;
            this.docidWriteCount = docidWriteCount;
        }

        @Override
        public Void call() throws Exception {
            int lineNumber = 0;
            StringBuilder b = new StringBuilder();
            try (BufferedReader in = new BufferedReader(new InputStreamReader(new GZIPInputStream(
                                             Files.newInputStream(inpath), 128 * 1024), StandardCharsets.UTF_8))) {
                while (true) {
                    String line = in.readLine();
                    if (line == null) {
                        break;
                    }
                    lineNumber++;
                    if (line.startsWith("<document ") == false) {
                        continue;
                    }
                    if (line.contains("service-id=\"dummy\"")) {
                        continue;
                    }
                    String prefix = line.substring(0, 128);
                    Matcher m = DOCID_PATTERN.matcher(prefix);
                    if (m.matches() == false) {
                        throw new RuntimeException("could not locate docid in " + inpath + ":" + lineNumber + ": " + prefix);
                    }
                    String origDocid = m.group(1);
                    docidReadCount.incrementAndGet();
                    int i = origDocid.indexOf(':');
                    String asin;
                    if (i != -1) {
                        // for non-US marketplaces the marketplaceid is appended to the ASIN, e.g. B00NXU7LRE:7
                        asin = origDocid.substring(0, i);
                        //System.err.println(origAsin + " --> " + asin);
                    } else {
                        asin = origDocid;
                    }
                    String serviceIDDocdidD = "1:" + origDocid;
                    if (docids.contains(serviceIDDocdidD)) {
                        if (DOCID_SEEN.putIfAbsent(serviceIDDocdidD, inpath) != null) {
                            System.err.println("DOCID \"" + serviceIDDocdidD + "\" is duplicated; origInPath=" +
                                               DOCID_SEEN.get(serviceIDDocdidD) + " vs " + inpath);
                        } else {
                            docidWriteCount.incrementAndGet();
                            b.append(line);
                            b.append('\n');
                        }
                    }
                }
            }

            System.err.println(inpath + " docidReadCount " + docidReadCount.get() + "; docidWriteCount " + docidWriteCount.get()
                               + "; DOCID_SEEN.size()=" + DOCID_SEEN.size());
            System.out.print(b.toString());
            return null;
        }
    }

    private static final ExecutorService EXEC = Executors.newFixedThreadPool(24);

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: FilterXDFs <input-folder> <docids-file>");
            System.exit(-1);
        }
        Path inputFolder = Paths.get(args[0]);
        Path docidsFile = Paths.get(args[1]);
        Set<String> docids;
        try (Stream<String> stream = Files.lines(docidsFile)) {
            // this file is now docid\tfamily
            docids = stream.map(s -> s.split("\t", 2)).map(arr -> arr[0].trim()).collect(Collectors.toSet());
        }
        System.err.println("read " + docids.size() + " docids from " + docidsFile);

        AtomicInteger docidReadCount = new AtomicInteger();
        AtomicInteger docidWriteCount = new AtomicInteger();
        List<Future<Void>> results = new ArrayList<>();
        int fileCount = 0;
        System.out.println("<?xml version='1.0' encoding='UTF-8'?><batch>");
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(inputFolder)) {
            for (Path entry: stream) {
                if (Files.isDirectory(entry)) {
                    try (DirectoryStream<Path> stream2 = Files.newDirectoryStream(entry)) {
                        for (Path entry2: stream2) {
                            if (entry2.getFileName().toString().endsWith(".gz")) {
                                results.add(EXEC.submit(new Job(entry2, docids, docidReadCount, docidWriteCount)));
                                fileCount++;
                            }
                        }
                    }
                }
            }
        }

        for (Future<Void> result : results) {
            result.get();
        }
        EXEC.shutdown();

        System.out.println("</batch>");

        System.err.println("fileCount " + fileCount + " docidReadCount " + docidReadCount.get()
                           + " docidWriteCount " + docidWriteCount.get());
        System.err.println("\nMissing DOCIDs:");
        int missingCount = 0;
        for (String docid : docids) {
            if (DOCID_SEEN.containsKey(docid) == false) {
                System.err.println("  " + docid);
                missingCount++;
            }
        }
        System.err.println("  total " + missingCount + " missing");
    }
}