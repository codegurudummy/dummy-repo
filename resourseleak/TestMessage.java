package resourceleak;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;


public class TestMessage {

    private static Map<String, JsonObject> testMessage1(String directory, String fileNamePrefix) throws IOException {
        Map<String, JsonObject> modelMeta = new HashMap<>();

        File[] files = new File(directory).listFiles((dir, name) -> name.startsWith(fileNamePrefix));
        for (File file : files) {
            try {
                JsonObject obj = (JsonObject) new JsonParser().parse(new FileReader(file));
                String modelType = getType(file);
                modelMeta.put(modelType, obj);
            } catch (IOException e) {
                LOGGER.error("Reading model metadata file {} error.", file.toString(), e);
                throw e;
            }
        }
        return modelMeta;
    }

    private KeyWriter testMessage2() throws IOException {
        OutputStream out = new FileOutputStream(outputFile);

        if (outputFile.endsWith(".gz")) {
            try {
                out = new GZIPOutputStream(out);
            } catch (IOException e) {
                out.close();
                throw e;
            }
        }

        Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);

        KeyWriter keyWriter;
        if (format.equalsIgnoreCase("sableloadgen")) {
            keyWriter = new SableLoadGenKeyWriter(writer);
        } else if (format.equalsIgnoreCase("blackmarlin")) {
            keyWriter = new BlackMarlinKeyWriter(writer);
        } else {
            throw new IllegalArgumentException("format must be either sableloadgen or blackmarlin");
        }

        return keyWriter;
    }

    private Writer testMessage3() throws IOException {
        OutputStream out = new FileOutputStream(outputFile);

        if (outputFile.endsWith(".gz")) {
            try {
                out = new GZIPOutputStream(out);
            } catch (IOException e) {
                out.close();
                throw e;
            }
        }

        Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);

        if (! format.equalsIgnoreCase("sableloadgen")) {
            throw new IllegalArgumentException("format must be either sableloadgen or blackmarlin");

        }
        return writer;
    }

    private Writer testReturnMinimalFindingsThatSpanAllLines() throws IOException {
        OutputStream out = new FileOutputStream(outputFile);
        Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
        if (! format.equalsIgnoreCase("sableloadgen")) {
            throw new IllegalArgumentException("format must be either sableloadgen or blackmarlin");
        }
        return writer;
    }
}
