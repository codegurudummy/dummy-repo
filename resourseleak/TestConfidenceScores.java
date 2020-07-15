package resourceleak;

import com.amazon.accessdeviceregisterservice.RegisterAVSDeviceResponse;
import com.amazon.arest.client.aaa.AaaFeature;
import com.amazon.coral.metrics.Metrics;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.a4b.endpointoperationservice.dependency.accessor.MediaStorageServiceAccessor;
import com.amazonaws.a4b.endpointoperationservice.dependency.exception.DependencyServiceException;
import com.amazonaws.a4b.endpointoperationservice.logic.OperationLogic;
import com.amazonaws.a4b.endpointoperationservice.logic.S3Logic;
import com.amazonaws.a4b.endpointoperationservice.model.OperationModel;
import com.amazonaws.a4b.endpointoperationservice.stepfunctions.model.UploadMediaActivityInput;
import com.amazonaws.a4b.endpointoperationservice.util.SerializationUtils;
import com.amazonaws.a4b.platform.common.exception.HttpClientException;
import com.amazonaws.a4b.platform.exception.InvalidDeviceStateException;
import com.amazonaws.a4b.platform.utils.HttpClientHelper;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.io.IOUtils;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.time.Clock;

public class TestConfidenceScores {

    private static final int CONNECTION_TIME_OUT = 3000;
    private static final String APP_NAME = "A4BAnalyticsLambda";
    private final MediaStorageServiceAccessor mssAccessor;
    private final S3Logic s3Logic;
    private final OperationLogic operationLogic;
    private final Clock clock;


    public String testConfidenceScores1() throws IOException {
        final StringWriter writer = new StringWriter();
        IOUtils.copy(this.getClass().getResourceAsStream("/report_template.html"), writer);
        return writer.toString();
    }

    public RegisterAVSDeviceResponse testConfidenceScores2(final String clientId, final String customerId, final String productId,
                                                       final String productSerialNumber) {

        try {
            final String adrsRegisterAVSDeviceEndpoint = adrsEndpoint + ADRS_REGISTER_AVS_DEVICE_PATH;

            final WebTarget target = client.target(adrsRegisterAVSDeviceEndpoint).register(AaaFeature.class).queryParam("clientId", clientId)
                    .queryParam("accountPool", accountPool).queryParam("productSerialNumber", productSerialNumber).queryParam("customerId", customerId)
                    .queryParam("productId", productId);
            final Response response = target.request().property(SERVICE_KEY, SERVICE_NAME).property(OPERATION_KEY, OPERATION_NAME).get();

            HttpClientHelper.inspectStatusCode(response, SERVICE_NAME);
            return response.readEntity(RegisterAVSDeviceResponse.class);
        } catch (HttpClientException e) {
            throw new InvalidDeviceStateException(ADRS_ERROR_MESSAGE, e);
        } catch (DependencyServiceException e) {
            throw e;
        } catch (Exception e) {
            throw new DependencyServiceException(ADRS_ERROR_MESSAGE, e);
        }
    }

    private static final File testConfidenceScores3(final String str, final String prefix, final String suffix) {
        final File tempFile;

        try {
            tempFile = File.createTempFile(prefix, suffix);
        } catch (IOException e1) {
            throw new AmazonServiceException("Could not create temp file, temp dir: " +
                    System.getProperty("java.io.tmpdir"));
        }

        try {
            final FileWriter fw = new FileWriter(tempFile);
            fw.write(str);
            fw.close();
        } catch (final IOException e) {
            throw new AmazonServiceException("Could not write temp file: " +
                    tempFile.getAbsolutePath());
        }

        return tempFile;
    }

    public String testConfidenceScores4(final String input, final Metrics metrics) {
        log.info("Received input:{}", input);

        final UploadMediaActivityInput inputModel = SerializationUtils.deserialize(input, UploadMediaActivityInput.class);

        String mediaId = null;

        if (clock.instant().isBefore(inputModel.getExpirationTime())) {
            final OperationModel operationModel = operationLogic.getOperationPayloadRecord(inputModel.getOrganizationArn(), inputModel.getAnnouncementArn());

            if (operationModel == null) {
                log.error("DDB record with payload deleted before announcement completed for organizationArn: {} and announcementArn: {}",
                        inputModel.getOrganizationArn(), inputModel.getAnnouncementArn());
                throw new IllegalStateException("DDB record with payload deleted before announcement completed");
            }

            try {
                final S3Object s3Object = s3Logic.getObjectFromServiceAccount(operationModel.getOperationPayload());
                mediaId = mssAccessor.uploadAudioAsCustomer(s3Object, inputModel.getCustomerId());
            } catch (final DependencyServiceException e) {
                log.error("Encountered dependency error for announcementArn: {}", inputModel.getAnnouncementArn());
                throw e;
            }
        }

    }
}