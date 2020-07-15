// Taken from https://code.amazon.com/reviews/CR-8140063/revisions/1#/diff
package apay.kuber.service.util;

import amazon.platform.config.AppConfig;
import apay.kuber.service.logger.KLog4JLogger;
import apay.kuber.service.metric.MetricsContextProviderFilter;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.Record;
import org.apache.log4j.Logger;
import org.springframework.scheduling.annotation.Async;

import javax.inject.Inject;
import javax.inject.Named;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.concurrent.Future;

import static apay.kuber.service.metric.Met.KINESIS_DATA_PUBLISH_ERROR_COUNT;
import static apay.kuber.service.metric.Met.SCANCODE_DATA_CUSTOMER_ID_ERROR_COUNT;
import static apay.kuber.service.util.CommonConstants.DATE_FORMAT;
import static apay.kuber.service.util.CommonConstants.KINESIS_PUBLISHING_TIME_FORMAT;
import static apay.kuber.service.util.CommonConstants.UTC;
import static com.amazon.coral.internal.netty4.io.netty.util.internal.StringUtil.LINE_FEED;
import static com.amazon.coral.internal.netty4.io.netty.util.internal.StringUtil.TAB;
import StringEscapeUtils;

/**
 * Kinesis Stream Data Publisher
 */

@Named
public class KinesisStreamDataPublisher {

    private Logger LOG = KLog4JLogger.getLogger(KinesisStreamDataPublisher.class);

    private static final String CUSTOMER_BEHAVIOUR_DATA_STREAM_CONFIG_PARAM = "customer_behaviour_data_stream";
    private static final String QR_CODE_TSV_STREAM_CONFIG_PARAM = "qr_code_tsv_stream_name";
    private static final String EMPTY_CUSTOMER_ID = "EMPTY";
    private static final int QR_CODE_MAX_LENGTH = 1000;
    private DateTimeUtils dateTimeUtils;

    @Inject
    public KinesisStreamDataPublisher(AmazonKinesisFirehose amazonKinesisFirehose, MetricsContextProviderFilter mp) {
        this.amazonKinesisFirehose = amazonKinesisFirehose;
        this.mp = mp;
        this.CUSTOMER_BEHAVIOUR_DATA_STREAM_NAME = AppConfig.findString(CUSTOMER_BEHAVIOUR_DATA_STREAM_CONFIG_PARAM);
        this.QR_CODE_DATA_TSV_STREAM_NAME = AppConfig.findString(QR_CODE_TSV_STREAM_CONFIG_PARAM);
        this.dateTimeUtils = new DateTimeUtils();
    }

    // Fix for https://tt.amazon.com/0188663432 and https://tt.amazon.com/0190174059
    private String postProcessScanCode(String scanCodeData) {
        String processedScanCodeData = null;
        if (scanCodeData.length() > QR_CODE_MAX_LENGTH) {
            processedScanCodeData = scanCodeData.substring(0, QR_CODE_MAX_LENGTH - 1);
        } else {
            processedScanCodeData = scanCodeData;
        }
        return StringEscapeUtils.escapeJava(processedScanCodeData);
    }
}