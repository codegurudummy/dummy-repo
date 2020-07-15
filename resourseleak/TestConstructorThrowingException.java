package resourceleak;

import com.amazon.awswolverineaudioprocessor.exception.CSVValidationException;
import com.amazon.awswolverineaudioprocessor.orchestrator.dictionarybuilder.DictionaryBuilderContext;
import com.amazon.coral.metrics.Metrics;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMappingException;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.commons.io.IOUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import javax.measure.unit.SI;
import javax.measure.unit.Unit;
import java.io.*;
import java.nio.file.Path;


public class TestConstructorThrowingException {

    public static void testConstructorThrowingException1(final File inputFile, final DictionaryBuilderContext context)
            throws CSVValidationException {
        CsvMapper mapper = new CsvMapper();
        CsvSchema bootstrapSchema = CsvSchema.emptySchema()
                .withColumnSeparator('\t')
                .withNullValue("") // so that missing value will be converted to null
                .withHeader();

        try(BufferedReader reader
                    = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), "UTF-8"));
            BufferedReader headerReader
                    = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), "UTF-8"))
        ) {
            int lineNumber = 2;
            MappingIterator<DictionaryCSVInputObjectModel> it = mapper.readerFor(DictionaryCSVInputObjectModel.class)
                    .with(bootstrapSchema).readValues(reader);
            validateColumns(headerReader.readLine());
            while (it.hasNext()) {
                DictionaryCSVInputObjectModel item = it.next();
                if(item.isEmpty()) {
                    continue;
                }
                validateLine(item, context, lineNumber);
                lineNumber++;
            }
        } catch (RuntimeJsonMappingException | CsvMappingException e) {
            // Tested known cases are
            // 1. Un-recognized header
            // 2. record field length exceeds header length
            LOGGER.info("DictionaryInputContentValidator client error {}", e.getMessage());
            throw new CSVValidationException(VALIDATION_ERROR_FORMAT);
        } catch (CSVValidationException e) {
            LOGGER.info("DictionaryInputContentValidator client error {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            // It's likely a client error, we surface it as client error and internally investigate
            LOGGER.warn("Possibly new encountered DictionaryInputContentValidator client error {}", e.getMessage());
            throw new CSVValidationException(VALIDATION_ERROR_FORMAT);
        }
    }

    public static void testConstructorThrowingException2(Metrics metrics, Path path) {
        JSONParser parser = new JSONParser();
        Reader fileReader = null;
        try {
            fileReader = new InputStreamReader(new FileInputStream(path.toFile()), "UTF-8");
            JSONObject obj = (JSONObject) parser.parse(fileReader);

            //Latency Metrics
            JSONArray latencyJsonArray = (JSONArray) obj.get("Metrics");
            for (Object latencyJsonObject : latencyJsonArray) {
                JSONObject jsonObject = (JSONObject) latencyJsonObject;
                String apptekMetricName = (String) jsonObject.get(APPTEK_METRIC_NAME_FIELD);
                if (MetricNames.apptekLatencyMetricsMap.get(apptekMetricName) != null) {
                    metrics.addTime(MetricNames.apptekLatencyMetricsMap.get(apptekMetricName),
                            Double.parseDouble(jsonObject.get(APPTEK_METRIC_VALUE_FIELD).toString()), SI.SECOND);
                } else if (MetricNames.APPTEK_RTF.equals(apptekMetricName)) {
                    metrics.addCount(MetricNames.AP_APPTEK_RTF, Double.parseDouble(jsonObject.get(APPTEK_METRIC_VALUE_FIELD).toString()),
                            Unit.ONE);
                }
            }

            //Error Metrics
            JSONArray errorJsonArray = (JSONArray) obj.get("Errors");
            for (Object errorJsonObject : errorJsonArray) {
                JSONObject jsonObject = (JSONObject) errorJsonObject;
                String apptekMetricName = (String) jsonObject.get(APPTEK_METRIC_NAME_FIELD);
                if (MetricNames.apptekErrorMetricsMap.get(apptekMetricName) != null) {
                    metrics.addCount(MetricNames.apptekErrorMetricsMap.get(apptekMetricName),
                            Double.parseDouble(jsonObject.get(APPTEK_METRIC_VALUE_FIELD).toString()), Unit.ONE);
                }
            }

        } catch (Exception e) {
            //Swallowing exception as these metrics are only for debugging.
            log.error("Error pushing apptek metrics.", e);
        } finally {
            IOUtils.closeQuietly(fileReader);
        }
    }


}
