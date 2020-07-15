package foo;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import javax.measure.unit.Unit;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Example {
    AmazonS3 s3Client;

    private String getObjectContent_getObject_GetObjectContent_toString_1(String locale) {
        final String bucket = appConfigUtil.findString(S3_AIRPORTS_BUCKET);
        final String file = S3_AIRPORTS_CODE_FILE_NAME_PREFIX + locale + DOT_JSON;
        final S3Object object = s3Client.getObject(bucket, file);
        try (final InputStream reportStream = object.getObjectContent();) {
            return IOUtils.toString(reportStream);
        } catch (IOException e) {
            LOGGER.error("Error trying to read from S3 to string. Bucket: Bucket: "
                    + bucket + ", Filename: " + locale);
            throw new RuntimeException(e);
        }
    }

    // Adapted from
    // https://code.amazon.com/packages/GuruArtifactBuilder/blobs/bbce41c00341ecfaa36b87f2ea4261f8c4eabb05/--/src/com/amazonaws/guru/services/artifactbuilder/models/preconditions/PreconditionSnippetDao.java#L51
    private String getObjectContent_getObject_GetObjectContent_toString_2(final String key) {
        try {
            return cache.get(key, () -> {
                        final InputStream inputStream = s3.getObject(
                                bucket, String.format("%s/%s", directory, key)).getObjectContent();
                        return Optional.of(IOUtils.toString(inputStream));
                    }
            );
        } catch (ExecutionException | UncheckedExecutionException e) {
            return Optional.empty();
        }
    }

    private String getObjectContent_getObject_GetObjectContent_readLines(String locale) {
        final S3Object object = s3Client.getObject(bucket, file);
        InputStream reportStream = object.getObjectContent();
        return IOUtils.readLines(reportStream);
    }

    private String getObjectContent_getObject_GetObjectContent_reader_1(String locale) {
        final S3Object object = s3Client.getObject(bucket, file);
        InputStream reportStream = object.getObjectContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(reportStream));
        String line = null;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
    }

    // Adapted from
    // https://code.amazon.com/packages/GuruArtifactBuilder/blobs/bbce41c00341ecfaa36b87f2ea4261f8c4eabb05/--/src/com/amazonaws/guru/services/artifactbuilder/models/LocalVariableNamingWithLookup.java#L311
    private String getObjectContent_getObject_GetObjectContent_reader_2(final AmazonS3 s3) {
        final List<String> result = new LinkedList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                s3.getObject(ARTIFACTS_BUCKET, LOOKUP_TABLE_S3_KEY).getObjectContent(), Charset.defaultCharset()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                result.add(line);
            }
        } catch (RuntimeException | IOException e) {
            return null;
        }
        return result;
    }

    private String getObjectContent_alsoReadsMetadata(String locale) {
        final S3Object object = s3Client.getObject(bucket, file);
        System.out.println(object.getObjectMetadata())
        InputStream reportStream = object.getObjectContent();
        return IOUtils.readLines(reportStream);
    }

    private String getObjectMetadata_getObject_getObjectMetadata(String locale) {
        S3Object s3o = s3Client.getObject(bucketName, fileName);
        return s3o.getObjectMetadata().getVersionId();
    }

    private S3Object getObjectMetadata_getObject_getObjectMetadata_returns(String locale) {
        S3Object s3o = s3Client.getObject(bucketName, fileName);
        System.out.println(s3o.getObjectMetadata().getVersionId());
        return s3o;
    }
}
