package concurrency;

import com.amazon.bigbird.concurrency.BundledSerializedTask;
import com.amazon.bigbird.config.Config;
import com.amazon.bigbird.config.ConfigKeys;
import com.amazon.bigbird.dataaccess.OpContext;
import com.amazon.bigbird.metrics.BBCoralMetrics;
import com.amazon.bigbird.storageengine.UpdateRequestResult;
import com.amazon.bigbird.storageengine.UpdateRequestResult.UpdateRequestStatus;
import com.amazon.bigbird.throttle.TimeSrc;

import javax.measure.unit.SI;
import javax.measure.unit.Unit;
import java.util.HashMap;
import java.util.Map;

import static com.amazon.bigbird.exceptions.BigBirdAssert.bbAssert;

/**
 * This class is used in {@Link ItemExpiryPurger} for aggregating and
 * collecting metrics.
 * @author jinglz
 *
 */
public class ItemExpiryPurgerTaskMetricsCollector {
    private final Config config;
    private final TimeSrc timeSource;
    private final Map<String, Integer> purgerCountMetricsMap;
    private final Map<String, ItemExpiryPurgerTimeMetricPair> purgerTimeMetricsMap;
    private volatile long lastReportMetricsTime;

    public ItemExpiryPurgerTaskMetricsCollector(Config config, TimeSrc timeSource) {
        this.config = config;
        this.timeSource = timeSource;
        this.purgerCountMetricsMap = new HashMap<>();
        this.purgerTimeMetricsMap = new HashMap<>();
        this.lastReportMetricsTime = timeSource.getMilli();
        resetPurgerMetrics();
    }

    /**
     * Only one instance of the purger task (per base table partition) will ever
     * be running. This is achieved through {@link BundledSerializedTask},
     * concurrency is guaranteed.
     **/
    private synchronized void resetPurgerMetrics() {
        purgerCountMetricsMap.put(BBCoralMetrics.Field.ItemExpiryPurgerTaskConsumedWriteIops.name(), 0);
        purgerCountMetricsMap.put(BBCoralMetrics.Field.ItemExpiryPurgerTaskSuccessfulDeletion.name(), 0);
        purgerCountMetricsMap.put(BBCoralMetrics.Field.ItemExpiryPurgerTaskUnsuccessfulDeletion.name(), 0);
        purgerCountMetricsMap.put(BBCoralMetrics.Field.ItemExpiryPurgerTaskFailedByException.name(), 0);
        purgerCountMetricsMap.put(BBCoralMetrics.Field.ItemExpiryPurgerTaskThrottledByBaseTableIops.name(), 0);
        purgerCountMetricsMap.put(BBCoralMetrics.Field.ItemExpiryPurgerTaskThrottledByGsi.name(), 0);
        purgerCountMetricsMap.put(BBCoralMetrics.Field.ItemExpiryPurgerTaskThrottledByUpdateStreams.name(), 0);
        purgerCountMetricsMap.put(BBCoralMetrics.Field.ItemExpiryPurgerTaskThrottledByReplicaTableEntryThrottle.name(), 0);
        purgerCountMetricsMap.put(BBCoralMetrics.Field.ItemExpiryPurgerTaskConditionCheckFailed.name(), 0);
        purgerCountMetricsMap.put(BBCoralMetrics.Field.ItemExpiryPurgerTaskUnexpiredItemDequeued.name(), 0);
        purgerCountMetricsMap.put(BBCoralMetrics.Field.ItemExpiryPurgerExpressionZero.name(), 0);
        purgerCountMetricsMap.put(BBCoralMetrics.Field.ItemExpiryWriteIOPsPublishedToStreams.name(), 0);
        purgerTimeMetricsMap.put(BBCoralMetrics.Field.ItemExpiryPurgerStaleness.name(), new ItemExpiryPurgerTimeMetricPair());
    }

    public void aggregatePurgerMetrics(UpdateRequestResult result) {
        if (result.status == UpdateRequestStatus.SUCCESS) {
            addToSingleCountMetricMapEntry(BBCoralMetrics.Field.ItemExpiryPurgerTaskConsumedWriteIops.name(),
                    result.logicalIOCount /* value */);
            incrementSingleCountMetricMapEntry(BBCoralMetrics.Field.ItemExpiryPurgerTaskSuccessfulDeletion.name());
        } else {
            incrementSingleCountMetricMapEntry(BBCoralMetrics.Field.ItemExpiryPurgerTaskUnsuccessfulDeletion.name());
        }
    }

    private void collectPurgerMetrics(final OpContext opContext) {
        collectCountAndTimeMetrics(opContext);
    }

    /**
     * We want to aggregate metrics in 1 min time window to avoid writing to
     * metrics too frequently.
     * 
     * @param opContext
     */
    public void collectMetrics(OpContext opContext) {
        long currentTime = timeSource.getMilli();
        if ((currentTime - lastReportMetricsTime) > config.getLong(ConfigKeys.ItemExpiryReportMetricsIntervalMillis)) {
            collectPurgerMetrics(opContext);
            this.lastReportMetricsTime = timeSource.getMilli();
            resetPurgerMetrics();
        }
    }

    private void incrementSingleCountMetricMapEntry(final String metricName) {
        addToSingleCountMetricMapEntry(metricName, 1 /* value */);
    }

    private synchronized void addToSingleCountMetricMapEntry(final String metricName, final Integer value) {
        Integer metricValue = purgerCountMetricsMap.get(metricName);
        bbAssert(metricValue != null, "addToSingleMetricMapEntry", "Metric not available", "metricName", metricName);
        purgerCountMetricsMap.put(metricName, metricValue + value);
    }
    private synchronized void addToSingleTimeMetricMapEntry (final String metricName, final long value) {
        ItemExpiryPurgerTimeMetricPair metricValue = purgerTimeMetricsMap.get(metricName);
        bbAssert(metricValue != null, "addToSingleTimeMetricMapEntry", "Metric not available", "metricName", metricName);
        metricValue.addItemExpiryPurgerStaleness(value);
        purgerTimeMetricsMap.put(metricName, metricValue);
    }

    public void updateItemExpiryPurgerStaleness(final long staleness) {
        addToSingleTimeMetricMapEntry(BBCoralMetrics.Field.ItemExpiryPurgerStaleness.name(), staleness);
    }

    public void increasePurgerTaskGetZeroNtpTime() {
        incrementSingleCountMetricMapEntry(BBCoralMetrics.Field.ItemExpiryPurgerExpressionZero.name());
    }

    public void increasePurgerTaskFailedByException() {
        incrementSingleCountMetricMapEntry(BBCoralMetrics.Field.ItemExpiryPurgerTaskFailedByException.name());
    }

    public void increasePurgerTaskThrottledByBaseTableIops() {
        incrementSingleCountMetricMapEntry(BBCoralMetrics.Field.ItemExpiryPurgerTaskThrottledByBaseTableIops.name());
    }

    public void increasePurgerTaskThrottledByGsi() {
        incrementSingleCountMetricMapEntry(BBCoralMetrics.Field.ItemExpiryPurgerTaskThrottledByGsi.name());
    }

    public void increasePurgerTaskThrottledByUpdateStreams() {
        incrementSingleCountMetricMapEntry(BBCoralMetrics.Field.ItemExpiryPurgerTaskThrottledByUpdateStreams.name());
    }

    public void increasePurgerTaskThrottledByReplicaTableEntryThrottle() {
        incrementSingleCountMetricMapEntry(BBCoralMetrics.Field.ItemExpiryPurgerTaskThrottledByReplicaTableEntryThrottle.name());
    }

    public void increasePurgerTaskConditionCheckFailed() {
        incrementSingleCountMetricMapEntry(BBCoralMetrics.Field.ItemExpiryPurgerTaskConditionCheckFailed.name());
    }

    public void increasePurgerTaskUnexpiredItemDequeued() {
        incrementSingleCountMetricMapEntry(BBCoralMetrics.Field.ItemExpiryPurgerTaskUnexpiredItemDequeued.name());
    }

    public void increaseItemExpiryWriteIOPsPublishedToStreams(int logicalIOCount) {
        addToSingleCountMetricMapEntry(BBCoralMetrics.Field.ItemExpiryWriteIOPsPublishedToStreams.name(), logicalIOCount /* value */);
    }

    private void collectCountAndTimeMetrics(final OpContext opContext) {
        for (Map.Entry<String, Integer> entry : purgerCountMetricsMap.entrySet()) {
            opContext.getMetrics().addCount(entry.getKey(), entry.getValue(), Unit.ONE);
        }
        for (Map.Entry<String, ItemExpiryPurgerTimeMetricPair> entry : purgerTimeMetricsMap.entrySet()) {
            ItemExpiryPurgerTimeMetricPair metricValue = entry.getValue();
            if (metricValue.itemExpiryPurgerTimeMetricCount > 0) {
                long averageMetricValue = metricValue.itemExpiryPurgerTimeMetricSum / metricValue.itemExpiryPurgerTimeMetricCount;
                opContext.getMetrics().addTime(entry.getKey(), (double) averageMetricValue, SI.MILLI(SI.SECOND));
            }
        }
    }

    protected synchronized int getPurgerTaskThrottledByBaseTableForTestOnly() {
        return purgerCountMetricsMap.get(BBCoralMetrics.Field.ItemExpiryPurgerTaskThrottledByBaseTableIops.name());
    }

    protected synchronized int getPurgerTaskThrottledByUpdateStreamsForTestOnly() {
        return purgerCountMetricsMap.get(BBCoralMetrics.Field.ItemExpiryPurgerTaskThrottledByUpdateStreams.name());
    }

    protected synchronized int getItemExpiryWriteIOPsPublishedToStreams() {
        return purgerCountMetricsMap.get(BBCoralMetrics.Field.ItemExpiryWriteIOPsPublishedToStreams.name());
    }

    protected synchronized int getItemExpiryPurgerTaskConsumedWriteIops() {
        return purgerCountMetricsMap.get(BBCoralMetrics.Field.ItemExpiryPurgerTaskConsumedWriteIops.name());
    }

    protected synchronized int getPurgerTaskThrottledByReplicaTableEntryThrottleForTestOnly() {
        return purgerCountMetricsMap.get(BBCoralMetrics.Field.ItemExpiryPurgerTaskThrottledByReplicaTableEntryThrottle.name());
    }

    private class ItemExpiryPurgerTimeMetricPair {
        private long itemExpiryPurgerTimeMetricSum;
        private int itemExpiryPurgerTimeMetricCount;

        public ItemExpiryPurgerTimeMetricPair() {
            this.itemExpiryPurgerTimeMetricCount = 0;
            this.itemExpiryPurgerTimeMetricSum = 0;
        }
        private void addItemExpiryPurgerStaleness (long value) {
            this.itemExpiryPurgerTimeMetricSum += value;
            this.itemExpiryPurgerTimeMetricCount += 1;
        }
    }
}