package resourceleak;

import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

public class TestPhiNodes {

    private int attempt = 0;
    private AmazonS3 amazonS3;
    private Schema avroSchema;
    private RecordProcessor recordProcessor;
    private String bucket;
    private List<String> keys;
    private String outputPathHdfs;
    private Logger LOG = Logger.getLogger(S3CpTask.class);

    public void testPhiNodes(final String bucket, final List<String> keys, final String outputPathHdfs) throws Exception {
        attempt++;
        LOG.info("Downloading " + outputPathHdfs + " attempt " + attempt);
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
        try (final OutputStream hdfsOut = openHdfs(outputPathHdfs, true);
             final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)
                     .setCodec(CodecFactory.deflateCodec(5))
                     .create(avroSchema, hdfsOut)) {
            for (final String key : keys) {
                LOG.info("Downloading file " + key + " to " + outputPathHdfs);

                try (final S3Object object = amazonS3.getObject(bucket, key);
                     final InputStream is = object.getObjectContent();
                     final InputStream wrappedInput = key.endsWith(".gz") ? new GZIPInputStream(is) : new BufferedInputStream(is)) {
                    // TODO: avoid filename collisions on retry
                    jsonToAvro(wrappedInput, avroSchema, key, dataFileWriter, recordProcessor);
                } catch (EOFException e) {
                    LOG.warn("key " + key + " is incomplete gzip", e);
                } catch (Exception e) {
                    LOG.error("Caught exception on key " + key + " outputPathHdfs " + outputPathHdfs + " attempt " + attempt, e);
                    throw e;
                }
            }
        }
    }
}
