package concurrency;

import com.amazon.coral.metrics.Metrics;
import com.amazon.ion.Decimal;
import com.amazon.sable.constants.Headers;
import com.amazon.sable.netty.context.RequestContext;
import com.amazon.sable.netty.framing.stumpy.Message;
import com.amazon.sable.netty.util.SableUtils;
import com.amazon.sable.qos.throttle.RateLimitingThrottle;
import com.amazon.sable.qos.throttle.TokenBucketThrottle;
import com.amazon.sable.router.cache.config.EntityKey;
import com.amazon.sable.util.clock.NanoClock;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.measure.unit.Unit;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implement {@link EdgeCacheThrottleHandler} to handle edge cache throttle by different verbs.
 *
 */
public class EdgeCacheThrottleVerbHandler implements EdgeCacheThrottleHandler {
    /**
     * The logger for this class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(EdgeCacheThrottleVerbHandler.class);

    /**
     * The prefix for the edge cache request key being rate-limiting throttled.
     */
    public static final String METRIC_PREFIX_KEY = "Throttle:Key:RefreshRequest:";

    /**
     * The separator to use between different components of the throttle key.
     */
    private static final String THROTTLE_KEY_SEPARATOR = ":";

    /**
     * Metric emitted when a throttle rule does not exist for a request.
     */
    private static final String MISSING_THROTTLE_RULE_METRIC_KEY = "EdgeCacheThrottle:MissingRule";

    /**
     * Wild card for client/scope.
     */
    private static final String WILD_CARD = "*";

    /**
     * NanClock for getting time in nanoseconds.
     */
    private final NanoClock clock;

    /**
     * The map of entity to current throttle rule.
     */
    private volatile Map<EntityKey, RateLimitingThrottle> autoThrottleKeyToThrottleMap;

    private volatile Map<EntityKey, RateLimitingThrottle> manualThrottleKeyToThrottleMap;

    /**
     * Token bucket refresh rate.
     */
    private Double tokenBucketRefreshRate;
    /**
     * Token bucket size
     */
    private int tokenBucketSize;

    /**
     * Get dynamic fleet size from calculator
     */
    private EdgeCacheThrottleRatePerBoxCalculator edgeCacheThrottleRatePerBoxCalculator;

    public EdgeCacheThrottleVerbHandler(
            final NanoClock clock,
            final EdgeCacheThrottleRatePerBoxCalculator edgeCacheThrottleRatePerBoxCalculator) {
        this.autoThrottleKeyToThrottleMap = new ConcurrentHashMap<>();
        this.manualThrottleKeyToThrottleMap = new ConcurrentHashMap<>();
        this.clock = clock;
        this.edgeCacheThrottleRatePerBoxCalculator = edgeCacheThrottleRatePerBoxCalculator;
        this.tokenBucketRefreshRate = edgeCacheThrottleRatePerBoxCalculator.getEdgeCacheThrottleRatePerBox();
        this.tokenBucketSize = (int) Math.ceil(edgeCacheThrottleRatePerBoxCalculator.getEdgeCacheThrottleRatePerBox());
    }

    /**
     * Modify a rule in Edge Cache Refresh throttle.
     */
    @Override
    public void modifyThrottleRule(final EntityKey edgeCacheThrottleKey, final Decimal refreshRatePerSecond,
            final int tokenBucketSize) {
        validateBucketSizeAndRefreshRate(refreshRatePerSecond, tokenBucketSize);

        boolean isThrottleRuleModified = false;

        RateLimitingThrottle rateLimitingThrottle = this.autoThrottleKeyToThrottleMap.get(edgeCacheThrottleKey);
        if (rateLimitingThrottle != null) {
            this.autoThrottleKeyToThrottleMap.put(edgeCacheThrottleKey,
                                                  new TokenBucketThrottle(tokenBucketSize, refreshRatePerSecond.doubleValue(), clock.nanoTime()));
            LOG.warn("Edge cache refresh throttle rule modified. {}: Bucket size = {}; Refresh rate per second = {}.",
                     edgeCacheThrottleKey.toString(), tokenBucketSize, refreshRatePerSecond);
            isThrottleRuleModified = true;
        }

        rateLimitingThrottle = this.manualThrottleKeyToThrottleMap.get(edgeCacheThrottleKey);
        if (rateLimitingThrottle != null) {
            this.manualThrottleKeyToThrottleMap.put(edgeCacheThrottleKey,
                                                    new TokenBucketThrottle(tokenBucketSize,
                                                                            refreshRatePerSecond.doubleValue(), clock.nanoTime()));
            LOG.warn("Edge cache refresh throttle rule modified. {}: Bucket size = {}; Refresh rate per second = {}.",
                     edgeCacheThrottleKey.toString(), tokenBucketSize, refreshRatePerSecond);
            isThrottleRuleModified = true;
        }

        if (!isThrottleRuleModified) {
            LOG.warn("Failure modify throttle rule. Key:{} doesn't exist in the edge cache throttle rules.",
                     edgeCacheThrottleKey.toString());
            throw new IllegalArgumentException("Given key doesn't exist in the edge cache throttle rules.");
        }

    }
    
    /**
     * Modify a rule in Edge Cache Refresh throttle.
     */
    @Override
    public void modifyThrottleRule_VARIATION01(final EntityKey edgeCacheThrottleKey, final Decimal refreshRatePerSecond,
            final int tokenBucketSize) {
        validateBucketSizeAndRefreshRate(refreshRatePerSecond, tokenBucketSize);

        boolean isThrottleRuleModified = false;

        // RateLimitingThrottle rateLimitingThrottle = this.autoThrottleKeyToThrottleMap.get(edgeCacheThrottleKey);
        if (this.autoThrottleKeyToThrottleMap.get(edgeCacheThrottleKey) != null) {
            this.autoThrottleKeyToThrottleMap.put(edgeCacheThrottleKey,
                                                  new TokenBucketThrottle(tokenBucketSize, refreshRatePerSecond.doubleValue(), clock.nanoTime()));
            LOG.warn("Edge cache refresh throttle rule modified. {}: Bucket size = {}; Refresh rate per second = {}.",
                     edgeCacheThrottleKey.toString(), tokenBucketSize, refreshRatePerSecond);
            isThrottleRuleModified = true;
        }

        // rateLimitingThrottle = this.manualThrottleKeyToThrottleMap.get(edgeCacheThrottleKey);
        if (this.manualThrottleKeyToThrottleMap.get(edgeCacheThrottleKey) != null) {
            this.manualThrottleKeyToThrottleMap.put(edgeCacheThrottleKey,
                                                    new TokenBucketThrottle(tokenBucketSize,
                                                                            refreshRatePerSecond.doubleValue(), clock.nanoTime()));
            LOG.warn("Edge cache refresh throttle rule modified. {}: Bucket size = {}; Refresh rate per second = {}.",
                     edgeCacheThrottleKey.toString(), tokenBucketSize, refreshRatePerSecond);
            isThrottleRuleModified = true;
        }

        if (!isThrottleRuleModified) {
            LOG.warn("Failure modify throttle rule. Key:{} doesn't exist in the edge cache throttle rules.",
                     edgeCacheThrottleKey.toString());
            throw new IllegalArgumentException("Given key doesn't exist in the edge cache throttle rules.");
        }

    }

    /**
     * Set throttleRatePerBox. Update the entire map to take the updated throttle rate.
     *
     * @param refreshRatePerSecond
     *            Token Bucket refresh rate per second.
     * @param tokenBucketSize
     *            Token Bucket size.
     */
    @Override
    public void setThrottleRatePerBox(final Decimal refreshRatePerSecond, final int tokenBucketSize) {
        validateBucketSizeAndRefreshRate(refreshRatePerSecond, tokenBucketSize);
        this.tokenBucketRefreshRate = refreshRatePerSecond.doubleValue();
        this.tokenBucketSize = tokenBucketSize;
        LOG.warn("Set Edge Cache refresh throttle bucket size:{}; refresh rate:{}.", this.tokenBucketSize,
                this.tokenBucketRefreshRate);
        Map<EntityKey, RateLimitingThrottle> newAutoThrottleKeyToThrottleMap = new ConcurrentHashMap<>();
        for (EntityKey key : this.autoThrottleKeyToThrottleMap.keySet()) {
            newAutoThrottleKeyToThrottleMap.put(key,
                    new TokenBucketThrottle(this.tokenBucketSize, this.tokenBucketRefreshRate, clock.nanoTime()));
        }
        this.autoThrottleKeyToThrottleMap = newAutoThrottleKeyToThrottleMap;
        Map<EntityKey, RateLimitingThrottle> newManualThrottleKeyToThrottleMap = new ConcurrentHashMap<>();
        for (EntityKey key : this.manualThrottleKeyToThrottleMap.keySet()) {
            newManualThrottleKeyToThrottleMap.put(key,
                                            new TokenBucketThrottle(this.tokenBucketSize, this.tokenBucketRefreshRate, clock.nanoTime()));
        }
        this.manualThrottleKeyToThrottleMap = newManualThrottleKeyToThrottleMap;
    }

    @Override
    public double getThrottleRatePerBox() {
        return this.tokenBucketRefreshRate;
    }

    /**
     * Decide whether the edge cache request needs to be throttled.
     *
     * @param requestContext
     *            reqeust context.
     * @param message
     *            message.
     * @return should throttle or not.
     */
    @Override
    public boolean shouldThrottle(final RequestContext requestContext, final Message message) {
        EntityKey throttleKey = createThrottleEntityKey(message);
        boolean isThrottled = false;
        if (throttleKey != null) {
            Metrics metrics = requestContext.getMetrics();
            RateLimitingThrottle edgeCacheThrottle = getRateLimitingThrottleFromThrottleMap(throttleKey, autoThrottleKeyToThrottleMap);
            if (edgeCacheThrottle == null) {
                edgeCacheThrottle = getRateLimitingThrottleFromThrottleMap(throttleKey, manualThrottleKeyToThrottleMap);
            }
            if (edgeCacheThrottle != null) {
                // Get the time of request in nanoseconds
                final long timeNano = message.getStartTimeInNanos();
                isThrottled = edgeCacheThrottle.shouldThrottle(timeNano);
                // Emit a global service level metric for throttled refresh requests count.
                metrics.addLevel(METRIC_PREFIX_KEY + "Overall", (isThrottled) ? 1 : 0, Unit.ONE);
                // Emit metrics for client + scope + verb throttled refresh requests count.
                metrics.addLevel(
                        METRIC_PREFIX_KEY + message.getHeader(Headers.CLIENT) + THROTTLE_KEY_SEPARATOR
                                + message.getHeader(Headers.SCOPE) + THROTTLE_KEY_SEPARATOR + message.getVerb(),
                        (isThrottled) ? 1 : 0, Unit.ONE);
            } else {
                // This should rarely happen that we have a entity eligible for edge caching but we don't have a edge
                // cache throttle rule. There is a small window where this can happen when rules are being removed,
                // since we remove the throttle rule followed by the edge cache rule.
                LOG.warn("Edge Cache throttle rule does not exist for key {}", throttleKey);
                metrics.addCount(MISSING_THROTTLE_RULE_METRIC_KEY, 1, Unit.ONE);
                metrics.addCount(MISSING_THROTTLE_RULE_METRIC_KEY + ":Tenant:" + requestContext.getTenantMetricName(),
                        1, Unit.ONE);
            }
        } else {
            LOG.warn("Could not evaluate edge cache throttle rule for request {}", message.toDebugString());
        }

        return isThrottled;
    }

    /**
     * Get the throttle rule for a throttle key from the throttle map. This method returns null if the throttle key
     * does not exist in the map.
     *
     * @param throttleKey
     *         throttle key
     * @param throttleMap
     *         throttle map
     *
     * @return the throttle rule for the key or null if there is none
     */
    private RateLimitingThrottle getRateLimitingThrottleFromThrottleMap(
            final EntityKey throttleKey, final Map<EntityKey, RateLimitingThrottle> throttleMap) {
        RateLimitingThrottle edgeCacheThrottle = throttleMap.get(throttleKey);
        if (edgeCacheThrottle == null) {
            EntityKey clientWildCardKey = new EntityKey.Builder().withClient(WILD_CARD)
                                                                 .withScope(throttleKey.getScope())
                                                                 .withEntity(throttleKey.getEntity())
                                                                 .build();
            edgeCacheThrottle = throttleMap.get(clientWildCardKey);
        }
        if (edgeCacheThrottle == null) {
            EntityKey scopeWildCardKey = new EntityKey.Builder().withClient(throttleKey.getClient())
                                                                .withScope(WILD_CARD)
                                                                .withEntity(throttleKey.getEntity())
                                                                .build();
            edgeCacheThrottle = throttleMap.get(scopeWildCardKey);
        }
        return edgeCacheThrottle;
    }

    /**
     * Returns the current set of throttles including the key that is being throttled, the token bucket size, and
     * refresh rate.
     *
     * @return A map where the key is the current throttle key and its corresponding value is the token bucket size, and
     *         refresh rate.
     */
    @Override
    public Map<EntityKey, RateLimitingThrottle> getCurrentThrottleRules() {
        Map<EntityKey, RateLimitingThrottle> rules = new ConcurrentHashMap<>();
        rules.putAll(autoThrottleKeyToThrottleMap);
        rules.putAll(manualThrottleKeyToThrottleMap);
        return Collections.unmodifiableMap(rules);
    }

    /**
     * Validate the bucket size and the refresh rate.
     *
     * @param refreshRatePerSecond
     *            operator supplied refresh rate per second
     * @param tokenBucketSize
     *            operator supplied bucket size
     */
    private void validateBucketSizeAndRefreshRate(final Decimal refreshRatePerSecond, final int tokenBucketSize) {
        Validate.isTrue(refreshRatePerSecond.doubleValue() > 0.0, "The throttle rate for GetKey cannot be less than 0" +
                                                                  ".");
        Validate.isTrue(tokenBucketSize >= 0, "The GetKey token bucket size cannot be less than 0.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyAutoEntityRules(final Collection<EntityKey> autoEntityKeys) {
        Map<EntityKey, RateLimitingThrottle> newThrottleKeyToThrottleMap = new ConcurrentHashMap<>(autoEntityKeys.size());
        for (EntityKey key : autoEntityKeys) {
            RateLimitingThrottle rateLimitingThrottle = this.autoThrottleKeyToThrottleMap.get(key);
            if (rateLimitingThrottle != null) {
                newThrottleKeyToThrottleMap.put(key, rateLimitingThrottle);
            } else {
                newThrottleKeyToThrottleMap.put(key,
                        new TokenBucketThrottle(this.tokenBucketSize, this.tokenBucketRefreshRate, clock.nanoTime()));
                LOG.warn(
                        "Edge cache refresh throttle rule added. {}: Bucket size = {}; Refresh rate per second = {}.",
                        key.toString(), this.tokenBucketSize, this.tokenBucketRefreshRate);
            }
        }
        this.autoThrottleKeyToThrottleMap = newThrottleKeyToThrottleMap;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyManualEntityRules(final Collection<EntityKey> manualEntityKeys) {
        Map<EntityKey, RateLimitingThrottle> newThrottleKeyToThrottleMap = new ConcurrentHashMap<>(manualEntityKeys.size());
        for (EntityKey key : manualEntityKeys) {
            RateLimitingThrottle rateLimitingThrottle = this.manualThrottleKeyToThrottleMap.get(key);
            if (rateLimitingThrottle != null) {
                newThrottleKeyToThrottleMap.put(key, rateLimitingThrottle);
            } else {
                newThrottleKeyToThrottleMap.put(key,
                                                new TokenBucketThrottle(this.tokenBucketSize, this.tokenBucketRefreshRate, clock.nanoTime()));
                LOG.warn(
                        "Edge cache refresh throttle rule added. {}: Bucket size = {}; Refresh rate per second = {}.",
                        key.toString(), this.tokenBucketSize, this.tokenBucketRefreshRate);
            }
        }
        this.manualThrottleKeyToThrottleMap = newThrottleKeyToThrottleMap;
    }

    /**
     * Creates a throttle EntityKey by extracting the required parameters from the Message.
     *
     * @param message
     *            The original request message.
     *
     * @return A key generated from the request or null if no key can be generated.
     */
    private EntityKey createThrottleEntityKey(final Message message) {
        String client = message.getHeader(Headers.CLIENT);
        String scope = message.getHeader(Headers.SCOPE);
        String entity = SableUtils.getEntity(message);

        if ((client == null) || (scope == null) || (entity == null)) {
            return null;
        }

        EntityKey key = new EntityKey.Builder().withClient(client).withScope(scope).withEntity(entity).build();

        return key;
    }

}