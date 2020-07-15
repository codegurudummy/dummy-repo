package concurrency;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.aws158.flow.sampling.interfaces.DoubleCountFilter;
import com.amazon.aws158.flow.sampling.interfaces.SamplingRateEstimator;
import com.amazon.aws158.snmp.perInterface.SNMPConstants;
import com.amazon.aws158.snmp.perInterface.SNMPConsumer;
import com.amazon.aws158.snmp.perInterface.SNMPSnapshot;
import com.amazon.coral.metrics.MetricsFactory;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.NonNull;
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * A sampling rate estimator based on data obtained via SNMP.
 * NetFlow-reported data should be reported by reportFlowPackets.
 * SNMP data is periodically refreshed via a call to recalculate.
 * After a successful call to recalculate, updated sampling rates
 * are now available per network interface.
 *
 * reportFlowPackets, necessarily, can be called from a distinct
 * thread than the one calling recalculate. On the other hand,
 * recalculate must not be called from multiple threads. It will
 * duplicate some work, where the SNMP-responding router is the
 * critical resource we want minimally use; doing so may also
 * expose an unprotected critical section.
 *
 * @author Abid Masood (amasood)
 */
@ThreadSafe
public class SnmpBasedEstimator implements SamplingRateEstimator, SNMPConsumer {
    private static final Log LOG = LogFactory.getLog(SnmpBasedEstimator.class);

    /**
     * If the previous successful poll was older than this then don't consider it useful
     * and instead start from scratch as if this was the first poll
     */
    protected static final long MAX_TIME_INTERVAL_TO_PROCESS_MINUTES = 5;

    /**
     * Juniper routers for aggregated netflow emit flow packets in burst with period which is multiple of a minute.
     * Default configuration is one minute - there fore computing sampling rate on time period less than a minute
     * will result in sampling rate fluctuation and since we apply result to next period - will result in value overestimated.
     */
    protected static final long MIN_TIME_INTERVAL_TO_PROCESS_SECONDS = 45;

    private static final double LOG_LOW_SAMPLE_RATE_THRESHOLD = 0.00009; //9E-5

    private static final String LOW_SAMPLING_RATE_TRIGGER = "[LOW_SAMPLING_RATE]";

    private static final String NEGATIVE_SNMP_DELTA = "NegativeSnmpDelta";

    private static final String SUCCESS = "Success";

    protected static final int MIN_PACKETS_FOR_DYNAMIC_SAMPLING_RATE = 1000;
    protected static final int MIN_PERIODS_FOR_DYNAMIC_SAMPLING_RATE = 5;

    private final double defaultSamplingRate;
    private final double minimumSamplingRate;
    private final boolean deDupeEnabled;

    private final DoubleCountFilter doubleCountFilter;

    private final MetricsFactory metricsFactory;

    @Getter
    private final String routerFullName;

    private final Object lock = new Object();

    @GuardedBy("lock")
    private Map<Long, Long> flowOctets = new HashMap<>();

    @GuardedBy("lock")
    private Map<Long, Long> flowPackets = new HashMap<>();

    @GuardedBy("lock")
    private Map<Long, String> ifNames = new HashMap<>();

    @GuardedBy("lock")
    private SNMPSnapshot previousData;

    /**
     * The time we last successfully fetched the metrics from the router
     */
    @GuardedBy("lock")
    private long lastFetchTime;

    @GuardedBy("periodsAboveMinFlowPackets")
    private final Map<Long, Long> periodsAboveMinFlowPackets = new HashMap<>();

    // ifIndex -> best-guess sampling rate
    private final ConcurrentMap<Long, Double> samplingRateOfPackets = new ConcurrentHashMap<>();

    /**
     * Constructor which accepts different values for the different sampling rates and dedupe enabled
     * Non-blocking creation.
     */
    public SnmpBasedEstimator(@NonNull String routerFullName, @NonNull DoubleCountFilter doubleCountFilter,
            boolean deDupeEnabled, double defaultSamplingRate,
            double minimumSamplingRate, @NonNull MetricsFactory metricsFactory) {
        Validate.isTrue(minimumSamplingRate > 0 && minimumSamplingRate <= 1, "Sampling rate value out of range: " + minimumSamplingRate);
        Validate.isTrue(defaultSamplingRate > 0 && defaultSamplingRate <= 1, "Sampling rate value out of range: " + defaultSamplingRate);

        this.routerFullName = routerFullName;
        this.doubleCountFilter = doubleCountFilter;

        this.deDupeEnabled = deDupeEnabled;
        this.defaultSamplingRate = defaultSamplingRate;
        this.minimumSamplingRate = minimumSamplingRate;

        this.metricsFactory = metricsFactory;
    }

    /**
     * Private method to update latest snapshot and time stamp, to be called only from
     * synchronized section.
     */
    @GuardedBy("lock")
    private void resetAggregatedState(long timestamp, SNMPSnapshot snapshot) {
        flowOctets = Maps.newHashMap();
        flowPackets = Maps.newHashMap();
        ifNames =  extractIfNames(snapshot);

        lastFetchTime = timestamp;
        previousData = snapshot;
    }

    @Override
    public void accept(long timestamp, SNMPSnapshot snapshot) {
        Map<Long, Long> prevFlowOctets;
        Map<Long, Long> prevFlowPackets;
        SNMPSnapshot oldSnapshot;
        long timeSinceLastFetch;

        synchronized (lock) {
            timeSinceLastFetch = timestamp - lastFetchTime;
            if (previousData == null) {
                // First time to accept - pick up snapshot, timestamp and reset counters
                resetAggregatedState(timestamp, snapshot);
                return;
            }
            if (timeSinceLastFetch > TimeUnit.MINUTES.toNanos(MAX_TIME_INTERVAL_TO_PROCESS_MINUTES)) {
                resetAggregatedState(timestamp, snapshot);
                samplingRateOfPackets.clear();
                LOG.info("Reseting Sampling rate SNMP counters for " + routerFullName + " as the previous data is too old.");
                return;
            } else if (timeSinceLastFetch < TimeUnit.SECONDS.toNanos(MIN_TIME_INTERVAL_TO_PROCESS_SECONDS)) {
                // if time period too small - do nothing and keep old snapshot.
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring SNMP data for " + routerFullName + " as period covered is too small: " + timeSinceLastFetch + " nanos");
                }
                return;
            }

            // Swap data
            prevFlowOctets = flowOctets;
            prevFlowPackets = flowPackets;
            oldSnapshot = previousData;

            resetAggregatedState(timestamp, snapshot);
        }

        try (TSDMetrics metrics = new TSDMetrics(metricsFactory, "SnmpBasedEstimator.Recalculate")) {
            metrics.addZero(SUCCESS);

            Map<Long, Long> snmpOctets = calculateSnmpDelta(SNMPConstants.IN_OCTET_OIDS, snapshot, oldSnapshot, metrics);
            Map<Long, Long> snmpPackets = calculateSnmpDelta(SNMPConstants.IN_PACKET_OIDS, snapshot, oldSnapshot, metrics);


            if (LOG.isDebugEnabled()) {
                LOG.debug("Router: " + routerFullName + " - SNMP snapshot with form ifIndex -> object ID (ASN.1) -> object: " + snapshot);
                LOG.debug("Router: " + routerFullName + " - SNMP octet counts with form ifIndex -> count: " + snmpOctets);
                LOG.debug("Router: " + routerFullName + " - SNMP packet counts with form ifIndex -> count: " + snmpPackets);
                LOG.debug("Router: " + routerFullName + " - NetFlow octets with form ifIndex -> count: " + prevFlowOctets);
                LOG.debug("Router: " + routerFullName + " - NetFlow packets with form ifIndex -> count: " + prevFlowPackets);
                LOG.debug("Router: " + routerFullName + " - SNMP ifIndex -> ifName: " + ifNames);
            }

            calculateSamplingRateOfPackets(prevFlowPackets, snmpPackets);

            LOG.info("Router: " + routerFullName + " - Recalculated sampling rates are " + samplingRateOfPackets);

            metrics.addOne(SUCCESS);
        }
    }

    /**
     * returns a ifIndex->ifName map from ifIndex -> (OID, ifName) map
     */
    private Map<Long, String> extractIfNames(SNMPSnapshot snapshot) {
        Map<Long, String> names = Maps.newHashMap();
        for (Long ifIndex : snapshot.getDescribedInterfaces()) {
            Map<String, String> ifData = snapshot.getInterfaceData(ifIndex);
            if (ifData.containsKey(SNMPConstants.IF_NAME)) {
                names.put(ifIndex, ifData.get(SNMPConstants.IF_NAME));
            }
        }

        return names;
    }

    /**
     * Calculates delta for given OIDs in provided snapshots.
     */
    private Map<Long, Long> calculateSnmpDelta(Set<String> oids, SNMPSnapshot currentSnmpSnapshot,
            SNMPSnapshot prevSnmpSnapshot, TSDMetrics metrics) {
        metrics.addZero(NEGATIVE_SNMP_DELTA);
        // ifIndex -> delta packet count
        Map<Long, Long> snmpPacketCount = Maps.newHashMap();
        for (Long ifIndex: currentSnmpSnapshot.getDescribedInterfaces()) {
            // counter (ASN.1) -> count as String
            Map<String, String> currentCounters = currentSnmpSnapshot.getInterfaceData(ifIndex);

            if (!prevSnmpSnapshot.containsIfData(ifIndex)) {
                LOG.warn(String.format("Router: %s - ifIndex=%d was returned by the current SNMP poll but was not found in the previous SNMP poll.",
                            routerFullName, ifIndex));
                continue;
            }

            Map<String, String> prevCounters = prevSnmpSnapshot.getInterfaceData(ifIndex);

            long sum = 0;

            for (String oid : oids) {
                if (!currentCounters.containsKey(oid) || !prevCounters.containsKey(oid)) {
                    // This imply misconfiguration in SNMPFetcher.
                    LOG.error("Failed to found requested SNMP counter in SNMP snapshot. OID: " + oid);
                    continue;
                }
                long counterInc = Long.parseLong(currentCounters.get(oid)) - Long.parseLong(prevCounters.get(oid));
                if (counterInc < 0) {
                    metrics.addOne(NEGATIVE_SNMP_DELTA);;
                    LOG.warn(String.format(
                            "New counter value ( %s ) is less than old counter value ( %s ) for router %s and oid %s. Ignoring.",
                            currentCounters.get(oid),
                            prevCounters.get(oid),
                            routerFullName,
                            oid
                    ));
                    continue;
                }
                if (sum + counterInc < sum) {
                    LOG.warn("Difference in SNMP counters overflown long value. Setting to Long.MAX_VALUE");
                    sum = Long.MAX_VALUE;
                    break;
                }
                sum += counterInc;
            }
            snmpPacketCount.put(ifIndex, sum);
        }

        return snmpPacketCount;
    }

    /**
     * This function actually calculates the sampling rate of packet for each
     * interface
     *
     * SamplingRate (interface) =
     * TotalFlowPacketCount(interface)/TotalSNMPPacketCount(interface)
     *
     * @param netflowPacketsCount ifIndex -&gt; packet count
     * @param snmpPacketCount ifIndex -&gt; packet count
     */
    private void calculateSamplingRateOfPackets(
            final Map<Long, Long> netflowPacketsCount,
            final Map<Long, Long> snmpPacketCount) {
        // NOTE: if we don't find packets for some ifIndex
        // then last intervals sampling rate will retain.
        if (netflowPacketsCount.entrySet().size()==0) {
            LOG.debug("Router: " + routerFullName + " - No Flow Entries");
        }

        synchronized(periodsAboveMinFlowPackets) {
            int lowSamplingRateInterfaceCount = 0;
            Set<Long> interfaceIndexes = new HashSet<>(netflowPacketsCount.keySet());
            interfaceIndexes.addAll(snmpPacketCount.keySet());
            for (long ifIndex : interfaceIndexes) {
                if (!snmpPacketCount.containsKey(ifIndex)) {
                    LOG.warn(String.format("Router: %s - NetFlow says there was traffic but SNMP suspiciously claims there was no additional traffic since " +
                            "previous SNMP poll for ifIndex %d. Possible that relevant counter overflowed or router-state was reset.",
                            routerFullName, ifIndex));
                    if (!samplingRateOfPackets.containsKey(ifIndex)) {
                        samplingRateOfPackets.put(ifIndex, defaultSamplingRate);
                    }

                    continue;
                }
                long snmpCount = snmpPacketCount.get(ifIndex);

                if (!netflowPacketsCount.containsKey(ifIndex)) {
                    if (snmpCount > 0) {
                        LOG.debug(String.format("No Netflow packets received for interface %s while SNMP report %s packets seen.",
                                ifIndex, snmpCount));
                    }
                    samplingRateOfPackets.put(ifIndex, defaultSamplingRate);
                    continue;
                }
                long netflowCount = netflowPacketsCount.get(ifIndex);

                Double previousSamplingRate = samplingRateOfPackets.get(ifIndex);
                if (previousSamplingRate == null) {
                    previousSamplingRate = defaultSamplingRate;
                }
                double samplingRate = defaultSamplingRate;

                if (!periodsAboveMinFlowPackets.containsKey(ifIndex)) {
                    periodsAboveMinFlowPackets.put(ifIndex, 0L);
                }

                // Count the number of consecutive periods with enough flow for dynamic sampling rate
                if (netflowCount < MIN_PACKETS_FOR_DYNAMIC_SAMPLING_RATE) {
                    periodsAboveMinFlowPackets.put(ifIndex, 0L);
                } else {
                    periodsAboveMinFlowPackets.put(ifIndex, periodsAboveMinFlowPackets.get(ifIndex) + 1);
                }

                if (periodsAboveMinFlowPackets.get(ifIndex) <  MIN_PERIODS_FOR_DYNAMIC_SAMPLING_RATE) {
                    LOG.warn(
                        String.format(
                            "Router: %s - Not enough netflow for enough periods to compute dynamic sampling rate, set to default rate %1.4E: ifIndex: %d, flowCount: %d, snmpCount: %d, periodsAboveMinFlowPackets: %d",
                            routerFullName, samplingRate, ifIndex, netflowCount, snmpCount, periodsAboveMinFlowPackets.get(ifIndex)));
                } else if (snmpCount != 0 && snmpCount >= netflowCount) {
                    // compute dynamic sampling rate
                    samplingRate = netflowCount / (double)snmpCount;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Router: %s - ifIndex: %d, flowCount: %d, snmpCount: %d samplingRate: %1.4E",
                                routerFullName, ifIndex, netflowCount, snmpCount, samplingRate));
                    } else if (samplingRate < LOG_LOW_SAMPLE_RATE_THRESHOLD) {
                        LOG.info("Router: " + routerFullName + " - Low Sampling rate "+
                                String.format("ifIndex: %d, flowCount: %d, snmpCount: %d samplingRate: %1.4E", ifIndex, netflowCount, snmpCount, samplingRate));
                    }

                    // dampen quickly decreasing sampling rates
                    if (samplingRate < (previousSamplingRate / 10.0)) {
                        double prevSamplingRate = samplingRate;
                        samplingRate = ((samplingRate * 2.0) + previousSamplingRate) / 3.0;
                        LOG.info(String.format("Router: %s - Volatile sampling rate for ifIndex %d computed sampling rate of %1.4E which change by more than 10x from previous rate of:%1.4E. Rate dampened to:%1.4E",
                                routerFullName, ifIndex, prevSamplingRate, previousSamplingRate, samplingRate));
                    }

                    // set it to the minimum if it's lower than the minimum threshold.
                    if (samplingRate < minimumSamplingRate) {
                        lowSamplingRateInterfaceCount++;
                        samplingRate = minimumSamplingRate;
                        LOG.info(String.format("Router: %s - Low sampling rate For ifIndex %d computed sampling rate of %1.4E which is below the minimum sampling rate of:%1.4E. Set to minimum rate",
                                routerFullName, ifIndex, samplingRate, minimumSamplingRate));
                    }
                } else {
                    // the counters are insane, or the calculations are messed up.
                    // Use the default sampling rate
                    LOG.warn(String.format("Router: %s - Insane SamplingRate, set to default rate %1.4E: ifIndex: %d, flowCount: %d, snmpCount: %d", routerFullName, samplingRate, ifIndex, netflowCount, snmpCount));
                }
                samplingRateOfPackets.put(ifIndex, samplingRate);
            }

            if (lowSamplingRateInterfaceCount > 0
                    && lowSamplingRateInterfaceCount == netflowPacketsCount.entrySet().size()
                    && lowSamplingRateInterfaceCount > (samplingRateOfPackets.size() / 2)) {
                LOG.warn(String.format(LOW_SAMPLING_RATE_TRIGGER + "Router: %s - %d inferfaces out of %d computed sampling rates below the minimum sampling rate of %1.4E",
                        routerFullName, lowSamplingRateInterfaceCount, netflowPacketsCount.entrySet().size(), minimumSamplingRate));
            }
        }
    }

    /**
     * Returns sampling rate of packets for interface ifIndex. This
     * best-effort sampling rate calculation is based on data collected
     * in the previous interval.
     *
     * @param ifIndex the interface index.
     *
     * @return The default sampling rate if NetFlow was observed on ifIndex but SNMP
     * indicated none (error condition), or the best-effort packet-
     * based sampling rate for ifIndex.
     */
    @Override
    public double getSamplingRateOfPackets(long ifIndex) {
        Double samplingRate = samplingRateOfPackets.get(ifIndex);
        if (samplingRate == null) {
            return defaultSamplingRate;
        }
        return samplingRate;
    }

    /**
     * To report the flow packet count
     *
     * @param srcIfIndex
     * @param destIfIndex
     * @param octets
     * @param packets
     */
    @Override
    public boolean reportFlow(long srcIfIndex, long destIfIndex, long octets, long packets) {
        Validate.isTrue(srcIfIndex >= 0);
        Validate.isTrue(destIfIndex >= 0);
        Validate.isTrue(octets >= 0);
        Validate.isTrue(packets >= 0);

        synchronized (lock) {
            // shouldCount should be true by default
            boolean shouldCount = true;
            if (ifNames.containsKey(srcIfIndex) && ifNames.containsKey(destIfIndex) && deDupeEnabled) {
                shouldCount = doubleCountFilter.shouldCount(ifNames.get(srcIfIndex), ifNames.get(destIfIndex));
            }

            if (shouldCount) {
                // flow octet counting
                long oldOctets = 0;
                if (flowOctets.containsKey(srcIfIndex)) {
                    oldOctets = flowOctets.get(srcIfIndex);
                }
                flowOctets.put(srcIfIndex, oldOctets + octets);

                // flow packet counting
                long oldPackets = 0;
                if (flowPackets.containsKey(srcIfIndex)) {
                    oldPackets = flowPackets.get(srcIfIndex);
                }
                flowPackets.put(srcIfIndex, oldPackets + packets);
            }

            return shouldCount;
        }
    }

    @Override
    public double getMinimumSamplingRate() {
        return this.minimumSamplingRate;
    }
}