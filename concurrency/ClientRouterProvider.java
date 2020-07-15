package concurrency;

import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.coral.security.AccessDeniedException;
import com.amazon.slr.Address;
import com.amazon.spicy.time.TimeSource;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.amazon.slr.communication.TimingConstants.PEER_SET_UPDATE_INTERVAL_MILLIS;

public class ClientRouterProvider {
    /** The amount of time it takes for ClientRouters to time out peer reported servers. */
    public static final long SERVER_TIMEOUT_MS = PEER_SET_UPDATE_INTERVAL_MILLIS * 2L + 2_000L;
    /** Metrics factory used by each ClientRouter created. */
    private final MetricsFactory metricsFactory;
    /** Then endpoint for this router. Used when creating ClientRouter instances */
    private final Address clientEndpoint;
    /** Tracks statistics for this router. */
    private final RequestRateReporter requestRateReporter;
    /** The map of load balancer to ClientRouter. */
    private final ConcurrentHashMap<String, ClientRouter> clientRouterServiceMap = new ConcurrentHashMap<>();
    /** The map of aliases to ClientRouter. */
    private final ConcurrentHashMap<String, ClientRouter> aliasMap = new ConcurrentHashMap<>();
    /** The RulesEngineProvider used when creating a new ClientRouter. */
    private final RuleEngineProvider ruleEngineProvider;
    /** Used to track time. */
    private final TimeSource timeSource;
    /** Sets Client->Server throttle probability. */
    private final ServiceThrottling serviceThrottling;
    /** Used to throttle clients based off tps. */
    private final ClientThrottler clientThrottler;

    private final EndpointsConfig endpointsConfig;

    /**
     * Constructor.
     * @param metricsFactory Metrics factory for creating metrics instances for each ClientRouter.
     * @param clientEndpoint The endpoint for this router. Used when creating a ClientRouter.
     * @param ruleEngineProvider The rules engine provider for each ClientRouter.
     * @param timeSource Used to track time.
     * @param requestRateReporter Tracks the statistics for a router.
     */
    ClientRouterProvider(MetricsFactory metricsFactory,
                         EndpointsConfig endpointsConfig,
                         Address clientEndpoint,
                         RuleEngineProvider ruleEngineProvider,
                         TimeSource timeSource,
                         RequestRateReporter requestRateReporter,
                         ServiceThrottling serviceThrottling,
                         ClientThrottler clientThrottler) {
        this.metricsFactory = metricsFactory;
        this.clientEndpoint = clientEndpoint;
        this.requestRateReporter = requestRateReporter;
        this.timeSource = timeSource;
        this.ruleEngineProvider = ruleEngineProvider;
        this.serviceThrottling = serviceThrottling;
        this.clientThrottler = clientThrottler;
        this.endpointsConfig = endpointsConfig;

        createRouters();
    }

    /**
     * Use the EndpointsConfig object to create routers for all endpoints/aliases which are not already created.
     */
    public void createRouters() {
        //TODO (SLR-1942): handle endpoint/alias deletion
        for (Map.Entry<String, List<String>> aliases : endpointsConfig.getAliasesForLbs().entrySet()) {
            String lbName = aliases.getKey();
            ClientRouter clientRouter = clientRouterServiceMap.computeIfAbsent(lbName, this::buildClientRouter);
            aliasMap.put(lbName, clientRouter);
            for (String alias : aliases.getValue()) {
                aliasMap.put(alias, clientRouter);
            }
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Gets the ClientRouter for a specific load balancer.
     *      If the load balancer is not whitelisted then it will throw an AccessDeniedException.
     *      If the load balancer does not have a client router mapped to it then a new client router will be created.
     * @param loadBalancer The load balancer to look up.
     * @return ClientRouter for the given loadBalancer.
     * @throws AccessDeniedException is the load balancer is unrecognized
     */
    public ClientRouter getClientRouter(String loadBalancer) {
        ClientRouter router = aliasMap.get(loadBalancer);
        if (router == null) {
            if (loadBalancer.startsWith(EndpointsConfig.SLR_CANARY_ENDPOINT_PREFIX)) {
                router = clientRouterServiceMap.computeIfAbsent(loadBalancer, x ->
                        buildClientRouter(loadBalancer));
                aliasMap.put(loadBalancer, router);
            } else {
                throw new AccessDeniedException("LoadBalancer:" + loadBalancer + " is not a valid load balancer on SLR");
            }
        }

        return router;
    }

    /**
     * Returns all known client routers.
     * @return Collection of known ClientRouters.
     */
    public Collection<ClientRouter> getAllRouters() {
        return clientRouterServiceMap.values();
    }

    private ClientRouter buildClientRouter(String loadBalancerName) {
        return ClientRouter.builder()
                .withServiceName(loadBalancerName)
                .withMetricsFactory(metricsFactory)
                .withClientEndpoint(clientEndpoint)
                .withRequestRateReporter(requestRateReporter)
                .withRuleEngineProvider(ruleEngineProvider)
                .withServerTimeoutMs(SERVER_TIMEOUT_MS)
                .withRemoteServerFactory(ClientRouterServer::get)
                .withTimeSource(timeSource)
                .withServiceThrottling(serviceThrottling)
                .withClientThrottler(clientThrottler).build();
    }

    public static class Builder {
        private MetricsFactory metricsFactory;
        private EndpointsConfig endpointsConfig;
        private Address clientEndpoint;
        private RuleEngineProvider ruleEngineProvider;
        private TimeSource timeSource;
        private RequestRateReporter requestRateReporter;
        private String availabilityZone;
        private ClientThrottler clientThrottler;
        private ServiceThrottling serviceThrottling;

        public Builder withMetricsFactory(MetricsFactory metricsFactory) {
            this.metricsFactory = metricsFactory;
            return this;
        }

        public Builder withEndpointsConfig(EndpointsConfig loadBalancerConfig) {
            this.endpointsConfig = loadBalancerConfig;
            return this;
        }

        public Builder withClientEndpoint(Address clientEndpoint) {
            this.clientEndpoint = clientEndpoint;
            return this;
        }

        public Builder withRuleEngineProvider(RuleEngineProvider ruleEngineProvider) {
            this.ruleEngineProvider = ruleEngineProvider;
            return this;
        }

        public Builder withTimeSource(TimeSource timeSource) {
            this.timeSource = timeSource;
            return this;
        }

        public Builder withRequestRateReporter(RequestRateReporter requestRateReporter) {
            this.requestRateReporter = requestRateReporter;
            return this;
        }

        public Builder withServiceThrottling(ServiceThrottling serviceThrottling) {
            this.serviceThrottling = serviceThrottling;
            return this;
        }

        public Builder withClientThrottler(ClientThrottler clientThrottler) {
            this.clientThrottler = clientThrottler;
            return this;
        }

        public ClientRouterProvider build() {
            return new ClientRouterProvider(
                    metricsFactory,
                    endpointsConfig,
                    clientEndpoint,
                    ruleEngineProvider,
                    timeSource,
                    requestRateReporter,
                    serviceThrottling,
                    clientThrottler);
        }
    }

}