package concurrency;

import amazon.platform.config.AppConfig;
import amazon.platform.tools.ApolloEnvironmentInfo.EnvironmentRootUndefinedException;
import com.amazon.coral.client.*;
import com.amazon.coral.retry.RetryStrategy;
import com.amazon.coral.retry.strategy.RetryStrategies;
import com.amazon.coral.service.CallAttachment;
import com.amazon.creturn.fc.plugin.inventory.interfaces.KORInventoryAccountingServiceSAO;
import com.amazon.customerReturns.utils.metrics.MetricsPublisher;
import com.amazon.korinventory.accounting.GetCustomerReturnCostForAsinInput;
import com.amazon.korinventory.accounting.GetCustomerReturnCostForAsinOutput;
import com.amazon.korinventory.accounting.KORInventoryAccountingServiceClient;
import com.amazon.korinventory.accounting.impl.GetCustomerReturnCostForAsinCall;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component("KORInventoryAccountingServiceSAOImpl")
@Scope(BeanDefinition.SCOPE_SINGLETON)
public class KORInventoryAccountingServiceSAOImpl implements KORInventoryAccountingServiceSAO {

    private static final Logger logger = Logger.getLogger(KORInventoryAccountingServiceSAOImpl.class);
    private static final String SERVICE_NAME = "KORInventoryAccountingService";
    private static final String GET_CUSTOMER_RETURN_COST_FOR_ASIN = "GetCustomerReturnCostForAsin";
    private static final String DOT = ".";

    /*
     * KORInventory's Coral config just has configuration defined for USAmazon,
     * that's why using constant realm string
     */

    private static final String REALM_STRING = "USAmazon";

    private final ClientBuilder clientBuilder;
    private ConcurrentMap<String, KORInventoryAccountingServiceClient> serviceClientMap;
    private static KORInventoryAccountingServiceSAOImpl instance;

    private final RetryStrategy<Object> GENERIC_RETRY_STRATEGY;
    private final CallAttachment GENERIC_CALL_ATTACHMENT;
    private static final int MAX_ATTEMPTS = 3;
    private static final int MAX_ELAPSED_TIME = 60000;
    private static final int EXPONENTIAL_FACTOR = 1;
    private static final int INITIAL_INTERVAL_MILLIS = 30;

    public KORInventoryAccountingServiceSAOImpl() {
        this.clientBuilder = new ClientBuilder();
        GENERIC_RETRY_STRATEGY = getRetryStrategy(EXPONENTIAL_FACTOR, MAX_ATTEMPTS, MAX_ELAPSED_TIME);
        GENERIC_CALL_ATTACHMENT = Calls.retry(GENERIC_RETRY_STRATEGY);
        serviceClientMap = new ConcurrentHashMap<String, KORInventoryAccountingServiceClient>();
    }
    
    synchronized public static KORInventoryAccountingServiceSAOImpl getInstance() {
        if (instance == null) {
            instance = new KORInventoryAccountingServiceSAOImpl(); 
        }
        return instance;
    }

    synchronized public KORInventoryAccountingServiceClient getServiceClient(String warehouseId)
            throws EnvironmentRootUndefinedException, IOException {
        String realmString = REALM_STRING;// WarehouseConfigReader.INSTANCE.getConfigForWarehouse(warehouseId,
                                          // WarehouseConfigurationKeys.HQ_REALM);
        KORInventoryAccountingServiceClient korInventoryAccountingServiceClient;

        korInventoryAccountingServiceClient = serviceClientMap.get(realmString);
        if (korInventoryAccountingServiceClient != null)
            return korInventoryAccountingServiceClient;
        synchronized (this) {
            korInventoryAccountingServiceClient = serviceClientMap.get(realmString);
            if (korInventoryAccountingServiceClient == null) {
                korInventoryAccountingServiceClient = clientBuilder.remoteOf(KORInventoryAccountingServiceClient.class)
                        .withConfiguration(getConfigParameter(realmString)).newClient();
                serviceClientMap.put(realmString, korInventoryAccountingServiceClient);
            }
        }
        return korInventoryAccountingServiceClient;
    }

    private String getConfigParameter(String realmString) throws EnvironmentRootUndefinedException, IOException {
        String configKey = null;
        if (!AppConfig.isInitialized() || null != AppConfig.getDomain() || null != AppConfig.getRealm()
                || null != AppConfig.getRealm().name())
            configKey = AppConfig.getDomain() + "." + realmString;
        else
            throw new RuntimeException(
                    "Exception while initializing KORInventoryAccountingService AppConfig with domain: "
                            + AppConfig.getDomain() + " and realm: " + AppConfig.getRealm());
        return configKey;
    }

    @SuppressWarnings("unchecked")
    private RetryStrategy<Object> getRetryStrategy(int exponentialFactor, int maxAttempts, int maxElapsedTime) {
        return new RetryStrategies.ExponentialBackoffBuilder()
                .retryOn(HttpConnectionTimeoutException.class, HttpFailureException.class, HttpTimeoutException.class,
                        HttpFailureException.class).withMaxElapsedTimeMillis(maxElapsedTime)
                .withInitialIntervalMillis(INITIAL_INTERVAL_MILLIS).withExponentialFactor(exponentialFactor)
                .withMaxAttempts(maxAttempts).build();
    }

    @Override
    public GetCustomerReturnCostForAsinOutput getOfflineKindleCostData(String asin, String org, String warehouseId) {

        logger.info("Calling " + SERVICE_NAME + " with asin: " + asin + "warehouseId: " + warehouseId);
        MetricsPublisher metricsPublisher = new MetricsPublisher();
        try {
            GetCustomerReturnCostForAsinInput input = new GetCustomerReturnCostForAsinInput();
            input.setAsin(asin);
            input.setCountryCode(org);
            KORInventoryAccountingServiceClient korInventoryAccoutingServiceClient = getServiceClient(warehouseId);
            GetCustomerReturnCostForAsinCall call = korInventoryAccoutingServiceClient
                    .newGetCustomerReturnCostForAsinCall();
            GetCustomerReturnCostForAsinOutput offlineKindleCostData;
            offlineKindleCostData = call.addAttachment(GENERIC_CALL_ATTACHMENT).call(input);
            return offlineKindleCostData;
        } catch (EnvironmentRootUndefinedException | IOException e) {
            String errorMessage = "Error while calling KORInventoryAccountingService.GetCustomerReturnCostForAsin api for asin="
                    + asin + " warehouseId=" + warehouseId + " org=" + org;
            logger.error(errorMessage, e);
            throw new com.amazon.comet.contract.exception.DependencyFailureException(errorMessage, e);
        } finally {
            metricsPublisher.generateLatencyMetric(SERVICE_NAME + DOT + GET_CUSTOMER_RETURN_COST_FOR_ASIN);
        }
    }

}