package concurrency;

import com.amazon.buckeye.proxy.common.RuntimeEnvironment;
import com.amazon.buckeye.proxy.service.Initializer;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazonaws.sdc.ISDCRetriever;
import com.amazonaws.sdc.SDCConfigurationHelper;
import com.amazonaws.sdc.agent.local.service.model.*;
import com.amazonaws.sdc.jackson.SDCMarshallerJackson;
import com.fasterxml.sdc.agent.java.helper.dependencies.jackson.databind.ObjectMapper;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Manages multiple SDC configurations with one SDC config retriever.
 */
public class SDCOrchestrator implements Initializer
{
    private final Map<String, SDCConfiguration> configurations = new HashMap<>();
    private final Map<String, AtomicReference<?>> refs = new HashMap<>();
    private final Supplier<MetricsFactory> metricsSupplier;

    // Use "sandbox" for local testing of template changes against the SDC sandbox endpoint.
    private static final String SDC_STAGE = "prod";
    private static final String RIP_SERVICE_NAME = "truss-service";
    private ISDCRetriever sdcRetriever;


    public SDCOrchestrator(final Supplier<MetricsFactory> metricsSupplier)
    {
        Objects.requireNonNull(metricsSupplier);

        this.metricsSupplier = metricsSupplier;
    }

    /**
     * Register a new configuration for the orchestrator to manage.
     * @param config
     * @param ref
     */
    public void registerConfig(final SDCConfiguration config, final AtomicReference<?> ref)
    {
        configurations.put(config.getConfigName(), config);
        refs.put(config.getConfigName(), ref);
    }

    @Override
    public synchronized void init()
    {
        if (sdcRetriever != null)
        {
            throw new IllegalStateException("SDCOrchestrator.init can be called exactly once");
        }
        sdcRetriever = createSdcRetriever(configurations.values());

        // The callback actually does provide the latest configuration, but we pull it fresh
        // each time to avoid race conditions; even if we get callbacks out of order, we should never
        // overwrite with stale data.
        sdcRetriever.registerConfigCallback(this::updateConfig);

        // Retrieve and update current configuration values.
        configurations.values().stream()
            .forEach(config -> config.update(sdcRetriever, refs.get(config.getConfigName())));
    }

    /**
     * Update all SDCConfigurations.
     */
    public void updateConfig(final DynamicConfigData config)
    {
        String name = config.getConfigName();
        configurations.get(name).update(sdcRetriever, refs.get(name));
    }

    /**
     * Creates the SDC Retriever given multiple SDCConfigurations for multiple DynamicConfigTuples.
     * @param configs
     * @return
     */
    public ISDCRetriever createSdcRetriever(final Collection<SDCConfiguration> configs)
    {
        MetricsFactory metricsFactory = Objects.requireNonNull(metricsSupplier.get());

        // Setup the Jackson mapper, metrics factory, and ConfigurationHelper for SDC.
        ObjectMapper mapper = new ObjectMapper();
        ISDCRetriever retriever = SDCConfigurationHelper.builder()
            .withRipServiceName(RIP_SERVICE_NAME)
            .withMetrics(metricsFactory)
            .withMarshaller(new SDCMarshallerJackson(mapper))
            .build();

        // Configure the SDCAgent with AWS account role credentials through Odin, the DynamicConfig name, and the
        // schema name/version.
        if (!retriever.configure(SDCAgentConfig.builder()
            .withConfigTuples(configs.stream()
                .map(sdcConfiguration -> DynamicConfigTuple.builder()
                    .withAuthentication(SDCAuthentication.builder()
                        .withAccessId(sdcConfiguration.getMaterialSet(RuntimeEnvironment.instance().region().getName()))
                        .withType(SDCAuthenticationType.ODIN_PROVIDER)
                        .build())
                    .withConfigName(sdcConfiguration.getConfigName())
                    .withDefinitionVersion(sdcConfiguration.getRegistrySchemaVersion().getSchemaVersion())
                    .withRipServiceName(RIP_SERVICE_NAME).build())
                .collect(Collectors.toList()))
            .withStage(SDC_STAGE)
            .build()))
        {
            throw new RuntimeException(
                "Failure configuring the SDC polls! Blocking service startup to prevent serving bad configs! "
                    + "For diagnosis, try search for unauthorized Odin credentials as ONE root cause of this failure: "
                    + "`grep -i ' is not currently available from Odin' * | cut -d '\\\"' -f 2 | sort | uniq -c`"
            );
        }
        return retriever;
    }
}