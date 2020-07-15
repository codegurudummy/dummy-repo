package concurrency;

import android.content.Context;
import com.amazon.client.metrics.thirdparty.*;
import com.amazon.client.metrics.thirdparty.clickstream.UsageInfo;
import com.amazon.sellermobile.android.common.util.logging.Slog;
import com.amazon.sellermobile.commands.ParameterNames;
import com.dp.utils.SystemTime;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Utility for sending metrics to the DCM backend.
 */
public final class DCMetricsUtils {

    private static final String TAG = DCMetricsUtils.class.getSimpleName();

    // This index stores MetricEvent objects with a unique String id.  This allows for easy manipulation
    // of a given metric in different areas of the app without needing the pass the MetricEvent object back and forth.
    private static final ConcurrentHashMap<String, MetricEvent> METRIC_INDEX = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> TIMER_SAMPLE_INDEX = new ConcurrentHashMap<>();

    private static DCMetricsUtils dcMetricsUtils;

    private static DCMetricsConfig dcmConfig;
    private static boolean isInitialized = false;

    private static MetricsFactory mFactory;
    private static final String REFMARKER_DATA_POINT_NAME = "ref-override";
    private static final String NOT_ALLOWED_METRIC_CHARACTERS = "[^A-Za-z0-9\\.:@_\\-/]";
    private static final String METRIC_REPLACEMENT_CHARACTER = "_";

    private static String serviceName;
    private static String methodName;
    private static String teamName;
    private static String siteVariant;

    private boolean verbose;

    private DCMetricsUtils(boolean verboseLogging) {
        verbose = verboseLogging;
    }

    /**
     * @return instance of DCMetricsUtils
     */
    public static DCMetricsUtils getInstance() {
        return getInstance(false);
    }

    /**
     * Fetches the singleton instance.
     *
     * @param verboseLogging True if each metric sent should be logged.
     * @return instance of DCMetricsUtils
     */
    public static DCMetricsUtils getInstance(boolean verboseLogging) {
        if (!isInitialized) {
            Slog.e(TAG, "Unable to fetch an instance since the DCM library is not initialized");
            throw new IllegalStateException("DCM not properly initialized");
        }

        if (dcMetricsUtils == null) {
            dcMetricsUtils = new DCMetricsUtils(verboseLogging);
        }

        return dcMetricsUtils;
    }

    /**
     * Initializes the DCM library to start being used.
     * @param newConfig The configuration that you want to have for the DCM settings
     */
    public static void initialize(Context context, DCMetricsConfig newConfig) {
        dcmConfig = newConfig;
        serviceName = dcmConfig.getServiceName();
        methodName = dcmConfig.getMethodName();
        teamName = dcmConfig.getTeamName();
        siteVariant = dcmConfig.getSiteVariant();
        mFactory = AndroidMetricsFactoryImpl.getInstance(context);
        AndroidMetricsFactoryImpl.setOAuthHelper(context, dcmConfig.getOAuthHelper());
        AndroidMetricsFactoryImpl.setDeviceType(context, dcmConfig.getDeviceType());
        AndroidMetricsFactoryImpl.setDeviceId(context, dcmConfig.getDeviceId());
        isInitialized = true;
    }

    /**
     * @return Whether or not the DCM Utilities have been properly initialized
     */
    public static boolean isInitialized() {
        return isInitialized;
    }

    /**
     * Disables the DCM library from usage and marks it as no longer initialized
     */
    public static void disable() {
        isInitialized = false;
    }

    /**
     * Add a MetricEvent object to the METRIC_INDEX using the metricId as a key.  If a MetricEvent already exists using the metricId, nothing is done.
     */
    public DCMetricsUtils createMetric(String metricId) {
        return createMetric(metricId, MetricEventType.getDefault());
    }

    public DCMetricsUtils createMetric(String metricId, MetricEventType eventType) {
        Slog.v(TAG, "Create Metric: " + metricId);

        if (!METRIC_INDEX.containsKey(metricId)) {
            MetricEvent metricEvent = mFactory.createMetricEvent(serviceName, methodName, eventType);
            metricEvent.addString(DCMetricsConfig.SESSION_ID_LABEL, dcmConfig.getSessionId());
            METRIC_INDEX.put(metricId, metricEvent);
        } else {
            Slog.d(TAG, "MetricEvent with metricId: " + metricId + " already exists.");
        }

        return this;
    }

    /**
     * Get the raw MetricEvent object from the metric index.  For advanced metric manipulation or testing purposes.
     */
    public MetricEvent getMetricEvent(String metricId) {
        Slog.v(TAG, "Get Metric: " + metricId);

        return METRIC_INDEX.get(metricId);
    }

    /**
     * Send a metricEvent to the DCM backend.  The MetricEvent is then removed from the index.
     *
     * Optional parameter priority governs how quickly the MetricsTransport service will send it.
     *     Priority.HIGH means the service will send it right away.
     *     Priority.NORMAL means the service may wait until it has several metrics to send and bundle them all at once.
     */
    public DCMetricsUtils recordMetric(String metricId) {
        return recordMetric(metricId, DCMetricsConfig.DEFAULT_PRIORITY);
    }

    public DCMetricsUtils recordMetric(String metricId, Priority priority) {
        Slog.v(TAG, "Record Metric: " + metricId + " with Priority: " + priority.toString());

        if (METRIC_INDEX.containsKey(metricId)) {
            MetricEvent metricEvent = METRIC_INDEX.get(metricId);
            if (verbose) {
                Slog.v(TAG, "Record Metric Event: " + metricEvent.toString());
            }
            mFactory.record(metricEvent, priority, Channel.ANONYMOUS);
            METRIC_INDEX.remove(metricId);
            TIMER_SAMPLE_INDEX.remove(metricId);
        }

        return this;
    }

    /**
     * Removes a metric from the current map
     * @param metricId The ID of the metric that you want to remove
     * @return This instantiation of DCMetricsUtil
     */
    public DCMetricsUtils removeMetric(String metricId) {
        Slog.v(TAG, "Remove Metric: " + metricId);

        METRIC_INDEX.remove(metricId);
        TIMER_SAMPLE_INDEX.remove(metricId);

        return this;
    }

    /**
     * Detects whether or not a metric currently exists in the metric table
     * @param metricId The ID of the metric that you want to check on
     * @return Whether or not it exists in the current table
     */
    public boolean containsMetric(String metricId) {
        Slog.v(TAG, "Validate Containment of Metric: " + metricId);

        return METRIC_INDEX.containsKey(metricId);
    }

    /**
     * In a given MetricEvent in the METRIC_INDEX, increment a counter by the given amount.
     * If the MetricEvent doesn't exist, it will be created and added it to the index first.
     */
    public DCMetricsUtils incrementMetricCounter(String metricId, String counterName, double increment) {

        Slog.v(TAG, "Increment Metric Counter for Metric: " + metricId);

        if (!containsMetric(metricId)) {
            createMetric(metricId);
        }

        MetricEvent metricEvent = getMetricEvent(metricId);
        if (metricEvent != null) {
            counterName = stripIllegalCharacters(counterName);
            metricEvent.addCounter(counterName, increment);
        }
        return this;
    }

    /**
     * Adds a timer for a specific metric id
     * @param metricId The ID to the metric that needs the timer
     * @param timerName The name of the metric timer
     * @param startTime The time that you want to start the timer at, or add on to the current timer
     * @param recurrences The amount of samples taken to sum up the given startTime
     * @return This instantiation of DCMetricsUtil
     */
    public DCMetricsUtils createMetricTimer(String metricId, String timerName, long startTime, int recurrences) {
        MetricEvent metricEvent = METRIC_INDEX.get(metricId);

        if (metricEvent == null) {
            Slog.w(TAG, "Adding timer for a metric that doesn't exist. " + metricId);
            return this;
        }

        timerName = stripIllegalCharacters(timerName);
        metricEvent.addTimer(timerName, startTime, recurrences);

        Map<String, Integer> timerMap = TIMER_SAMPLE_INDEX.get(metricId);
        if (timerMap != null) {
            timerMap.put(timerName, 0);
        } else {
            ConcurrentHashMap<String, Integer> newTimerMap = new ConcurrentHashMap<>();
            newTimerMap.put(timerName, 0);
            TIMER_SAMPLE_INDEX.put(metricId, newTimerMap);
        }

        return this;
    }

    /**
     * Fetches the number of samples contained in a timer
     * @param metricId The name of the metric ID
     * @param timerName The name of the timer metric
     * @return The number of samples contained in the timer, or -1 if either the metric or timer couldn't be found
     */
    public int getMetricTimerSampleSize(String metricId, String timerName) {
        timerName = stripIllegalCharacters(timerName);
        if (TIMER_SAMPLE_INDEX.get(metricId) == null || TIMER_SAMPLE_INDEX.get(metricId).get(timerName) == null) {
            return -1;
        } else {
            return TIMER_SAMPLE_INDEX.get(metricId).get(timerName);
        }
    }

    /** Starts a timer for a specific metric id, will create a timer if needed
     * @param metricId The ID to the metric that needs the timer
     * @param timerName The name of the metric timer
     * @return This instantiation of DCMetricsUtil
     */
    public DCMetricsUtils startMetricTimer(String metricId, String timerName) {
        return startMetricTimer(metricId, timerName, 0, 0);
    }

    /**
     * Starts a timer for a specific metric id, will create a timer if needed
     * @param metricId The ID to the metric that needs the timer
     * @param timerName The name of the metric timer
     * @param defaultStartTime The time that you want to start the timer at, or add on to the current timer if the metric doesn't exist
     * @param defaultRecurrences The amount of samples taken to sum up the given startTime if the metric doesn't already exist
     * @return This instantiation of DCMetricsUtil
     */
    public DCMetricsUtils startMetricTimer(String metricId, String timerName, long defaultStartTime, int defaultRecurrences) {

        Slog.v(TAG, "Start Metric Timer for Metric: " + metricId);

        MetricEvent metricEvent = METRIC_INDEX.get(metricId);

        timerName = stripIllegalCharacters(timerName);
        if (metricEvent == null) {
            Slog.w(TAG, "Starting timer for a metric that doesn't exist. " + metricId + ":" + timerName);
            return this;
        }

        // If there are no samples then that means that the timer does not exist so create it
        if (getMetricTimerSampleSize(metricId, timerName) == -1) {
            metricEvent.addTimer(timerName, defaultStartTime, defaultRecurrences);

            Map<String, Integer> timerMap = TIMER_SAMPLE_INDEX.get(metricId);
            if (timerMap != null) {
                timerMap.put(timerName, 0);
            } else {
                ConcurrentHashMap<String, Integer> newTimerMap = new ConcurrentHashMap<>();
                newTimerMap.put(timerName, 0);
                TIMER_SAMPLE_INDEX.put(metricId, newTimerMap);
            }
        }

        metricEvent.startTimer(timerName);

        return this;
    }

    /**
     * Stops a timer for a specific metric id
     * @param metricId The ID to the metric that needs the timer
     * @param timerName The name of the metric timer
     * @return This instantiation of DCMetricsUtil
     */
    public DCMetricsUtils stopMetricTimer(String metricId, String timerName) {
        Slog.v(TAG, "Stop Metric Timer for Metric: " + metricId);

        MetricEvent metricEvent = METRIC_INDEX.get(metricId);

        timerName = stripIllegalCharacters(timerName);
        if (metricEvent == null) {
            Slog.w(TAG, "Stopping timer for a metric that doesn't exist. " + metricId + ":" + timerName);
            return this;
        }

        metricEvent.stopTimer(timerName);

        Map<String, Integer> timerMap = TIMER_SAMPLE_INDEX.get(metricId);
        Integer sampleSize = (timerMap == null) ? 1 : (timerMap.get(timerName) == null ? 1 : timerMap.get(timerName) + 1);
        if (timerMap != null) {
            timerMap.put(timerName, sampleSize);
        } else {
            ConcurrentHashMap<String, Integer> newTimerMap = new ConcurrentHashMap<>();
            newTimerMap.put(timerName, sampleSize);
            TIMER_SAMPLE_INDEX.put(metricId, newTimerMap);
        }

        return this;
    }

    /**
     * Adds a string value to the metric event that can be accessed as meta data. This will not be available
     * for plotting in iGraph
     * @param metricId The ID to the metric that needs the value added
     * @param key The key that you want to call the new string by
     * @param value The value to set the key to
     * @return This instantiation of DCMetricsUtil
     */
    public DCMetricsUtils addString(String metricId, String key, String value) {
        Slog.v(TAG, "Add String for Metric: " + metricId);

        key = stripIllegalCharacters(key);
        MetricEvent metricEvent = METRIC_INDEX.get(metricId);
        if (metricEvent == null) {
            Slog.w(TAG, "Adding a string for a metric that doesn't exist. " + metricId + ":" + key + "=" + value);
            return this;
        }

        metricEvent.addString(key, value);

        return this;
    }

    /********************************************* CLICKSTREAM METRIC IMPLEMENTATION ****************************************************************/

    /**
     * Creates a click stream event for the appropriate page and sub page types that are provided as well as any page action which is taken
     * @param metricId The metric ID string you want to associate with this metric
     * @param pageType The page type of the page you are recording action on
     * @param hitType The hit type associated with the metric you want to log
     * @param pageAction The action taken on the page, if there is no action then just pass in null such as for a page hit
     * @param refMarker The refMarker associated with the metric.
     * @return The new click stream event
     */
    public DCMetricsUtils createClickStreamMetric(String metricId, String metricName, String pageType, String hitType, String pageAction, String
            refMarker) {
        return createClickStreamMetric(
                metricId,
                metricName,
                pageType,
                null,
                hitType,
                pageAction,
                refMarker
        );
    }

    /**
     * Creates a click stream event for the appropriate page and sub page types that are provided as well as any page action which is taken
     * @param metricId The metric ID string you want to associate with this metric
     * @param pageType The page type of the page you are recording action on
     * @param subPageType The sub page type of the page you are recording action on
     * @param hitType The hit type associated with the metric you want to log
     * @param pageAction The action taken on the page, if there is no action then just pass in null such as for a page hit
     * @param refMarker The refMarker associated with the metric.
     * @return The new click stream event
     */
    public DCMetricsUtils createClickStreamMetric(String metricId, String metricName, String pageType, String subPageType, String hitType,
                                                  String pageAction, String refMarker) {
        Slog.d(TAG, "Creating ClickStream Metric - "
            + metricId + ","
            + pageType + ","
            + subPageType + ","
            + hitType + ","
            + pageAction
            + " serviceName=" + serviceName
            + ", methodname=" + methodName
            + ", customerId=" + dcmConfig.getCustomerId()
            + ", session-id=" + dcmConfig.getSessionId()
        );
        if (!METRIC_INDEX.containsKey(metricId)) {
            ClickStreamMetricsEvent metricEvent = mFactory.createClickStreamMetricEvent(serviceName, methodName);

            UsageInfo usageInfo = new UsageInfo(pageType, hitType, teamName, siteVariant);
            if (subPageType != null && !subPageType.isEmpty()) {
                usageInfo.setSubPageType(subPageType);
            }
            if (pageAction != null && !pageAction.isEmpty()) {
                usageInfo.setPageAction(pageAction);
            }

            DataPoint refTagDataPoint = new DataPoint(
                    REFMARKER_DATA_POINT_NAME,
                    refMarker,
                    1, DataPointType.CK
            );

            try {
                metricEvent.addDataPoint(refTagDataPoint);
            } catch (MetricsException ex) {
                Slog.e(TAG, "Unable to set data point in event for ref_marker", ex);
            }

            metricEvent.setUsageInfo(usageInfo);

            String customerId = dcmConfig.getCustomerId();
            String sessionId = dcmConfig.getSessionId();
            if (customerId != null && sessionId != null) {
                metricEvent.setAnonymous(false);
                metricEvent.setNonAnonymousCustomerId(customerId);
                metricEvent.setNonAnonymousSessionId(sessionId);
            } else {
                metricEvent.setAnonymous(true);
            }

            metricName = stripIllegalCharacters(metricName);
            metricEvent.addCounter(metricName, 1);

            METRIC_INDEX.put(metricId, metricEvent);
        } else {
            METRIC_INDEX.get(metricId).incrementCounter(metricId, 1);
        }

        return this;
    }

    /**
     * Strip illegal metric characters to allow for reporting of metrics even if improper characters are provided:
     * https://w.amazon.com/index.php/MobileDiagnostics/Platform/Metrics/FAQ
     * #What_are_the_PMET_imposed_restrictions_on_Data_.28Counters.2FTimers.2F_Other_Metrics.29_names.3F
     *
     * @param stringToCheck The string to check/strip
     * @return The new string with illegal characters stripped, or null if the string was null
     */
    public static String stripIllegalCharacters(String stringToCheck) {
        return (stringToCheck == null) ? null : stringToCheck.replaceAll(NOT_ALLOWED_METRIC_CHARACTERS, METRIC_REPLACEMENT_CHARACTER);
    }

    /**
     * This is a helper method for fetching some common event parameters that have to do with metrics, more specifically this will mark that the
     * event should log a metric and will also include the timestamp of the current system time in milliseconds based off of the definition provided
     * by the DCM library
     * @return An event param map for use by the SMPEvents library
     */
    public static Map<String, Object> getMetricParams() {
        Map<String, Object> params = new HashMap<>();
        params.put(ParameterNames.LOG_METRIC, true);
        params.put(ParameterNames.TIME_STAMP, SystemTime.now());
        return params;
    }

    /**
     * Takes a metricPrefix, runs the sanitization helper logic, and then returns either
     * the empty string, or a metricPrefix of the format "metricPrefix:" (note the colon)
     */
    public static String sanitizePrefix(String prefix) {
         String sanitizedPrefix = stripIllegalCharacters(prefix);
         return (sanitizedPrefix == null || sanitizedPrefix.isEmpty()) ? "" : sanitizedPrefix + ":";
    }
}