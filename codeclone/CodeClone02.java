package codeclone;

import com.amazon.bigbird.autoadmin.common.TableBucketizer;
import com.amazon.bigbird.autoadmin.heartbeat.SweeperHeartbeatHandler;
import com.amazon.bigbird.config.Config;
import com.amazon.bigbird.config.ConfigKeys;
import com.amazon.bigbird.constants.CommittedThroughput;
import com.amazon.bigbird.dataaccess.OpContext;
import com.amazon.bigbird.metadata.EventAccess;
import com.amazon.bigbird.metadata.EventType;
import com.amazon.bigbird.metadata.MetadataAccess;
import com.amazon.bigbird.metadata.model.CreateTableEventData;
import com.amazon.bigbird.metadata.model.DeleteTableEventData;
import com.amazon.bigbird.metadata.model.EventData;
import com.amazon.bigbird.metadata.model.UpdateTableEventData;
import com.amazon.bigbird.metrics.BBCoralMetricsFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import s3.commons.log.S3Logger;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.amazon.bigbird.exceptions.BigBirdAssert.bbAssert;

/**
 * A sweeper that queries control plane customer facing events and emits metrics on the number of unique
 * subscribers that have breached the Green-I, Yellow, and/or Red criteria.
 *
 *
 * Refer: https://w.amazon.com/index.php/BigBird/ControlPlane/Operations/ControlPlanePostingCriteria
 */
public class EventAnomalySweeper extends BaseSweeper {
    private static final S3Logger log = new S3Logger();

    public static final String SWEEPER_NAME = "EventAnomalySweeper";
    public static final String CREATE_TABLE_GREEN_I_METRIC = "SubscriberCountExceedingCreateTableGreenIThreshold";
    public static final String CREATE_TABLE_YELLOW_METRIC = "SubscriberCountExceedingCreateTableYellowThreshold";
    public static final String CREATE_TABLE_RED_METRIC = "SubscriberCountExceedingCreateTableRedThreshold";
    public static final String DELETE_TABLE_GREEN_I_METRIC = "SubscriberCountExceedingDeleteTableGreenIThreshold";
    public static final String DELETE_TABLE_YELLOW_METRIC = "SubscriberCountExceedingDeleteTableYellowThreshold";
    public static final String DELETE_TABLE_RED_METRIC = "SubscriberCountExceedingDeleteTableRedThreshold";
    public static final String UPDATE_TABLE_GREEN_I_METRIC_PREFIX = "SubscriberCountExceedingUpdateTableGreenIThreshold-";
    public static final String UPDATE_TABLE_YELLOW_METRIC_PREFIX = "SubscriberCountExceedingUpdateTableYellowThreshold-";
    public static final String UPDATE_TABLE_RED_METRIC_PREFIX = "SubscriberCountExceedingUpdateTableRedThreshold-";
    public static final String DIAL_DOWNS = "DialDowns";

    private EventAccess eventAccess;

    public EventAnomalySweeper(BBCoralMetricsFactory metricsFactory,
                        Config config,
                        MetadataAccess metadataAccess,
                        SweeperHeartbeatHandler sweeperHeartbeatHandler) {
        super(SWEEPER_NAME, metricsFactory, config, metadataAccess, sweeperHeartbeatHandler);
        this.eventAccess = metadataAccess.getEventAccess();
    }

    @Override
    protected void doWork(OpContext opContext) {
        emitPostingCriteriaForControlPlaneEvents(opContext);
    }

    @VisibleForTesting
    protected void emitPostingCriteriaForControlPlaneEvents(OpContext opContext) {
        emitCreateTablePostingCriteria(opContext);
        emitDeleteTablePostingCriteria(opContext);
        emitUpdateTablePostingCriteria(opContext);
    }

    @VisibleForTesting
    protected void emitCreateTablePostingCriteria(OpContext opContext) {
        final String METHOD_NAME = "emitCreateTablePostingCriteria";

        List<EventData> eventDatasForCreateTable =
                eventAccess.getAllEventsForAnEventType(opContext, EventType.CREATE_TABLE, /*consistent*/ false);
        bbAssert(eventDatasForCreateTable.stream().allMatch(eventData -> eventData instanceof CreateTableEventData), METHOD_NAME,
                "List of create table event datas cannot contain any other event data", "eventDatas", eventDatasForCreateTable);

        Function<EventData, String> dataToSubscriberIdFunction = (e) -> ((CreateTableEventData) e).subscriberId;

        Set<String> subscribersExceedingGreenIThreshold = getSubscribersExceedingPostingCriteria(eventDatasForCreateTable,
                dataToSubscriberIdFunction, ConfigKeys.CreateTableGreenICriteriaThreshold);
        if (subscribersExceedingGreenIThreshold.size() > 0) {
            log.warn(METHOD_NAME, "Subscribers are exceeding create table Green-I threshold",
                    "subscriberIds", subscribersExceedingGreenIThreshold);
        }

        Set<String> subscribersExceedingYellowThreshold = getSubscribersExceedingPostingCriteria(eventDatasForCreateTable,
                dataToSubscriberIdFunction, ConfigKeys.CreateTableYellowCriteriaThreshold);
        if(subscribersExceedingYellowThreshold.size() > 0) {
            log.warn(METHOD_NAME, "Subscribers are exceeding create table Yellow threshold",
                    "subscriberIds", subscribersExceedingYellowThreshold);
        }

        Set<String> subscribersExceedingRedThreshold = getSubscribersExceedingPostingCriteria(eventDatasForCreateTable,
                dataToSubscriberIdFunction, ConfigKeys.CreateTableRedCriteriaThreshold);
        if (subscribersExceedingRedThreshold.size() > 0) {
            log.warn(METHOD_NAME, "Subscribers are exceeding create table Red threshold",
                    "subscriberIds", subscribersExceedingRedThreshold);
        }

        opContext.getMetrics().addCount(CREATE_TABLE_GREEN_I_METRIC, subscribersExceedingGreenIThreshold.size());
        opContext.getMetrics().addCount(CREATE_TABLE_YELLOW_METRIC, subscribersExceedingYellowThreshold.size());
        opContext.getMetrics().addCount(CREATE_TABLE_RED_METRIC, subscribersExceedingRedThreshold.size());
    }

    @VisibleForTesting
    protected void emitDeleteTablePostingCriteria(OpContext opContext) {
        final String METHOD_NAME = "emitDeleteTablePostingCriteria";

        List<EventData> eventDatasForDeleteTable =
                eventAccess.getAllEventsForAnEventType(opContext, EventType.DELETE_TABLE, /*consistent*/ false);
        bbAssert(eventDatasForDeleteTable.stream().allMatch(eventData -> eventData instanceof DeleteTableEventData), METHOD_NAME,
                "List of delete table event datas cannot contain any other event data", "eventDatas", eventDatasForDeleteTable);

        Function<EventData, String> dataToSubscriberIdFunction = (e) -> ((DeleteTableEventData) e).subscriberId;

        Set<String> subscribersExceedingGreenIThreshold = getSubscribersExceedingPostingCriteria(eventDatasForDeleteTable,
                dataToSubscriberIdFunction, ConfigKeys.DeleteTableGreenICriteriaThreshold);
        if (subscribersExceedingGreenIThreshold.size() > 0) {
            log.warn(METHOD_NAME, "Subscribers are exceeding delete table Green-I threshold",
                    "subscriberIds", subscribersExceedingGreenIThreshold);
        }

        Set<String> subscribersExceedingYellowThreshold = getSubscribersExceedingPostingCriteria(eventDatasForDeleteTable,
                dataToSubscriberIdFunction, ConfigKeys.DeleteTableYellowCriteriaThreshold);
        if (subscribersExceedingYellowThreshold.size() > 0) {
            log.warn(METHOD_NAME, "Subscribers are exceeding delete table Yellow threshold",
                    "subscriberIds", subscribersExceedingYellowThreshold);
        }

        Set<String> subscribersExceedingRedThreshold = getSubscribersExceedingPostingCriteria(eventDatasForDeleteTable,
                dataToSubscriberIdFunction, ConfigKeys.DeleteTableRedCriteriaThreshold);
        if (subscribersExceedingRedThreshold.size() > 0) {
            log.warn(METHOD_NAME, "Subscribers are exceeding delete table Red threshold",
                    "subscriberIds", subscribersExceedingRedThreshold);
        }

        opContext.getMetrics().addCount(DELETE_TABLE_GREEN_I_METRIC, subscribersExceedingGreenIThreshold.size());
        opContext.getMetrics().addCount(DELETE_TABLE_YELLOW_METRIC, subscribersExceedingYellowThreshold.size());
        opContext.getMetrics().addCount(DELETE_TABLE_RED_METRIC, subscribersExceedingRedThreshold.size());
    }

    @VisibleForTesting
    protected void emitUpdateTablePostingCriteria(OpContext opContext) {
        final String METHOD_NAME = "emitUpdateTablePostingCriteria";

        List<EventData> eventDatasForUpdateTable =
                eventAccess.getAllEventsForAnEventType(opContext, EventType.UPDATE_TABLE, /*consistent*/ false);
        bbAssert(eventDatasForUpdateTable.stream().allMatch(eventData -> eventData instanceof UpdateTableEventData), METHOD_NAME,
                "List of update table event datas cannot contain any other event data", "eventDatas", eventDatasForUpdateTable);

        List<UpdateTableEventData> updateTableEventDatas = (List<UpdateTableEventData>)(List<?>) eventDatasForUpdateTable;
        CommittedThroughput.IopsConfig iopsConfig = getIopsConfig();

        Table<String, ConfigKeys, Set<String>> iopsRangeToThresholdToBreachingSubscribersTable = initIOPSRangeToCriteriaThresholdToSubscribersTable();

        populateTableForSubscribersBreachingCriteria(iopsRangeToThresholdToBreachingSubscribersTable, updateTableEventDatas, iopsConfig);

        for (Table.Cell<String, ConfigKeys, Set<String>> cell : iopsRangeToThresholdToBreachingSubscribersTable.cellSet()) {
            String iopsRange = cell.getRowKey();
            ConfigKeys threshold = cell.getColumnKey();
            Set<String> subscribers = cell.getValue();
            opContext.getMetrics().addCount(deriveMetricNameFromTableCell(iopsRange, threshold), subscribers.size());

            if (subscribers.size() > 0) {
                log.warn(METHOD_NAME, "Subscribers are exceeding update table posting criteria", "iopsRange",
                        iopsRange, "threshold", threshold.name(), "subscribers", subscribers);
            }
        }
    }

    @VisibleForTesting
    protected CommittedThroughput.IopsConfig getIopsConfig() {
        return new CommittedThroughput.IopsConfig(config,
                /*minRead*/ null, /*maxRead*/null);
    }

    @VisibleForTesting
    protected void populateTableForSubscribersBreachingCriteria(Table<String, ConfigKeys, Set<String>> iopsRangeToThresholdToBreachingSubscribersTable,
                                                                List<UpdateTableEventData> eventDatasForUpdateTable,
                                                                CommittedThroughput.IopsConfig iopsConfig) {
        eventDatasForUpdateTable.stream().forEach(updateTableEventData -> {

            //TODO: Add criteria for tables transitioning throughput modes
            if (isTableTransitioningThroughputModes(updateTableEventData)) {
                return;
            }

            long newEcReadIops = updateTableEventData.newIops.ecRead;
            long newWriteIops = updateTableEventData.newIops.write;
            long oldEcReadIops = updateTableEventData.oldIops.ecRead;
            long oldWriteIops = updateTableEventData.oldIops.write;
            double newTotalIops =  CommittedThroughput.totalIops(iopsConfig, newEcReadIops, newWriteIops);
            double oldTotalIops = CommittedThroughput.totalIops(iopsConfig, oldEcReadIops, oldWriteIops);
            Date recordInsertionTime = updateTableEventData.getEventRecordInsertionTime();

            String iopsRange;

            if (newTotalIops < oldTotalIops) {
                iopsRange = DIAL_DOWNS;
            } else {
                iopsRange = TableBucketizer.getIopsRange(newTotalIops);
            }

            //Get the map of threshold keys to subscribers for the derived IOPS range
            Map<ConfigKeys, Set<String>> criteriaThresholdsToBreachingSubscribers =
                    iopsRangeToThresholdToBreachingSubscribersTable.row(iopsRange);

            for (Map.Entry<ConfigKeys, Set<String>> thresholdToSubscribers : criteriaThresholdsToBreachingSubscribers.entrySet()) {
                ConfigKeys threshold = thresholdToSubscribers.getKey();

                if (eventExceedsCriteria(recordInsertionTime, getDurationFromConfig(threshold))) {
                    Set<String> subscribers = thresholdToSubscribers.getValue();
                    subscribers.add(updateTableEventData.subscriberId);
                    iopsRangeToThresholdToBreachingSubscribersTable.put(iopsRange, threshold, subscribers);
                }
            }
        });
    }

    @VisibleForTesting
    protected Set<String> getSubscribersExceedingPostingCriteria(List<EventData> eventDatas,
                                                                 Function<EventData, String> dataToSubscriberIdFunction,
                                                                 ConfigKeys thresholdKey) {

        return eventDatas.stream().
                filter(eventData -> eventExceedsCriteria(eventData.getEventRecordInsertionTime(), getDurationFromConfig(thresholdKey)))
                .map(dataToSubscriberIdFunction)
                .collect(Collectors.toSet());
    }

    private boolean eventExceedsCriteria(Date eventRecordInsertionTime,
                                         Duration exceedsCriteriaThreshold) {

        //This shouldn't ideally be null; but good to have a check to prevent an NPE
        if (eventRecordInsertionTime == null) {
            return false;
        }

        Instant recordInsertedInstant = eventRecordInsertionTime.toInstant();
        return recordInsertedInstant.isBefore(Instant.now().minus(exceedsCriteriaThreshold));
    }

    private Duration getDurationFromConfig(ConfigKeys key) {
        String METHOD_NAME = "getDurationFromConfig";
        try {
            return Duration.parse(config.getString(key));
        } catch (DateTimeParseException e) {
            log.error(METHOD_NAME, "Invalid duration passed through task config; returning default value");
            return Duration.parse(key.defaultValue);
        }
    }

    /**
     * Initializes a table which contains a mapping from IOPS ranges -> update table threshold key for green-i, yellow,
     * or red criteria -> unique subscribers breaching the respective criteria.
     * @return
     */
    public static Table<String, ConfigKeys, Set<String>> initIOPSRangeToCriteriaThresholdToSubscribersTable() {
        Table<String, ConfigKeys, Set<String>> table = HashBasedTable.create();
        //Green-I
        table.put(DIAL_DOWNS, ConfigKeys.UpdateTableGreenICriteriaThreshold_DialDowns, new HashSet<>());
        table.put(TableBucketizer.IOPS_RANGE_VERY_SMALL, ConfigKeys.UpdateTableGreenICriteriaThreshold_NoSplit, new HashSet<>());
        table.put(TableBucketizer.IOPS_RANGE_SMALL, ConfigKeys.UpdateTableGreenICriteriaThreshold_3k_30k, new HashSet<>());
        table.put(TableBucketizer.IOPS_RANGE_MEDIUM, ConfigKeys.UpdateTableGreenICriteriaThreshold_30k_300k, new HashSet<>());
        table.put(TableBucketizer.IOPS_RANGE_LARGE, ConfigKeys.UpdateTableGreenICriteriaThreshold_300k_3M, new HashSet<>());
        table.put(TableBucketizer.IOPS_RANGE_VERY_LARGE, ConfigKeys.UpdateTableGreenICriteriaThreshold_3M_15M, new HashSet<>());
        table.put(TableBucketizer.IOPS_RANGE_VERY_VERY_LARGE, ConfigKeys.UpdateTableGreenICriteriaThreshold_15M_30M, new HashSet<>());
        table.put(TableBucketizer.IOPS_RANGE_GIGANTIC, ConfigKeys.UpdateTableGreenICriteriaThreshold_30M_Inf, new HashSet<>());

        //Yellow
        table.put(DIAL_DOWNS, ConfigKeys.UpdateTableYellowCriteriaThreshold_DialDowns, new HashSet<>());
        table.put(TableBucketizer.IOPS_RANGE_VERY_SMALL, ConfigKeys.UpdateTableYellowCriteriaThreshold_NoSplit, new HashSet<>());
        table.put(TableBucketizer.IOPS_RANGE_SMALL, ConfigKeys.UpdateTableYellowCriteriaThreshold_3k_30k, new HashSet<>());
        table.put(TableBucketizer.IOPS_RANGE_MEDIUM, ConfigKeys.UpdateTableYellowCriteriaThreshold_30k_300k, new HashSet<>());
        table.put(TableBucketizer.IOPS_RANGE_LARGE, ConfigKeys.UpdateTableYellowCriteriaThreshold_300k_3M, new HashSet<>());
        table.put(TableBucketizer.IOPS_RANGE_VERY_LARGE, ConfigKeys.UpdateTableYellowCriteriaThreshold_3M_15M, new HashSet<>());
        table.put(TableBucketizer.IOPS_RANGE_VERY_VERY_LARGE, ConfigKeys.UpdateTableYellowCriteriaThreshold_15M_30M, new HashSet<>());
        table.put(TableBucketizer.IOPS_RANGE_GIGANTIC, ConfigKeys.UpdateTableYellowCriteriaThreshold_30M_Inf, new HashSet<>());

        //Red
        table.put(DIAL_DOWNS, ConfigKeys.UpdateTableRedCriteriaThreshold_DialDowns, new HashSet<>());
        table.put(TableBucketizer.IOPS_RANGE_VERY_SMALL, ConfigKeys.UpdateTableRedCriteriaThreshold_NoSplit, new HashSet<>());
        table.put(TableBucketizer.IOPS_RANGE_SMALL, ConfigKeys.UpdateTableRedCriteriaThreshold_3k_30k, new HashSet<>());
        table.put(TableBucketizer.IOPS_RANGE_MEDIUM, ConfigKeys.UpdateTableRedCriteriaThreshold_30k_300k, new HashSet<>());
        table.put(TableBucketizer.IOPS_RANGE_LARGE, ConfigKeys.UpdateTableRedCriteriaThreshold_300k_3M, new HashSet<>());
        table.put(TableBucketizer.IOPS_RANGE_VERY_LARGE, ConfigKeys.UpdateTableRedCriteriaThreshold_3M_15M, new HashSet<>());
        table.put(TableBucketizer.IOPS_RANGE_VERY_VERY_LARGE, ConfigKeys.UpdateTableRedCriteriaThreshold_15M_30M, new HashSet<>());
        table.put(TableBucketizer.IOPS_RANGE_GIGANTIC, ConfigKeys.UpdateTableRedCriteriaThreshold_30M_Inf, new HashSet<>());
        return table;
    }

    @VisibleForTesting
    protected String deriveMetricNameFromTableCell(String iopsRange,
                                                   ConfigKeys thresholdKey) {
        String METHOD_NAME = "deriveMetricNameFromTableCell";
        String threshold = thresholdKey.name().toLowerCase();
        //Hacky; but better than maintaining a huge map of thresholds -> metric names
        if(threshold.contains("greeni")) {
            return UPDATE_TABLE_GREEN_I_METRIC_PREFIX + iopsRange;
        } else if (threshold.contains("yellow")) {
            return UPDATE_TABLE_YELLOW_METRIC_PREFIX + iopsRange;
        } else if (threshold.contains("red")) {
            return UPDATE_TABLE_RED_METRIC_PREFIX + iopsRange;
        } else {
            log.error(METHOD_NAME, "Threshold name contains something other than green-i, yellow, or red",
                     "threshold", threshold);
            return "Other" + iopsRange;
        }
    }

    private boolean isTableTransitioningThroughputModes(UpdateTableEventData eventData) {
        if (eventData.newTableThroughputMode == null) {
            return false;
        }
        return eventData.oldTableThroughputMode != eventData.newTableThroughputMode;
    }

    @Override
    public ConfigKeys getPausedConfigKey() {
        return ConfigKeys.EventAnomalySweeperIsPaused;
    }
}