package concurrency;

import com.amazon.aics.dataprovider.DependencyFailureException;
import com.amazon.aics.dataprovider.InvalidArgumentException;
import com.amazon.aics.dataprovider.metrics.extensions.exceptionCounter.ExceptionCounter;
import com.amazon.chameleon.TypeCreator;
import com.amazon.metrics.declarative.MetricsManager;
import com.amazon.metrics.declarative.servicemetrics.Availability;
import com.amazon.metrics.declarative.servicemetrics.Latency;
import com.amazon.metrics.declarative.servicemetrics.ServiceMetric;
import com.amazon.metrics.declarative.servicemetrics.Timeout;
import com.amazon.mqa.chameleon.ChameleonCreator;
import com.amazon.retry.Retryable;
import com.amazon.weblab.allocation.IWeblabAllocationProvider;
import com.amazon.weblab.allocation.IWeblabAllocationProviderFactory;
import com.amazon.weblab.treatment.SessionInfo;
import com.google.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;

import javax.measure.unit.Unit;
import java.math.BigInteger;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.amazon.aics.dataprovider.constants.WeblabConstants.C_TREATMENT;

@Slf4j
public class WeblabServiceWrapper {
    public static final String BEAN_NAME = "weblabServiceWrapper";
    public static final String WEBLAB_FAILURE = "WeblabFailure";

    private final Map<String, IWeblabAllocationProvider> weblabProviderMap;
    private final IWeblabAllocationProviderFactory weblabAllocationFactory;
    private final MetricsManager metricsManager;

    @Inject
    public WeblabServiceWrapper(IWeblabAllocationProviderFactory weblabAllocationFactory,
                                MetricsManager metricsManager) {
        this.weblabAllocationFactory = weblabAllocationFactory;
        this.metricsManager = metricsManager;
        this.weblabProviderMap = new ConcurrentHashMap<>();
    }

    @ServiceMetric(serviceName = "WeblabService", operation = "getTreatmentAssignmentAndRecordTrigger")
    @ExceptionCounter
    @Availability
    @Timeout
    @Latency
    @Retryable(maxAttempts = 3, backoffCoefficient = 2, delay = 50)
    public String getWeblabTreatment(String weblabName, String encryptedCustomerId, String encryptedMarketplaceId)
            throws InvalidArgumentException, DependencyFailureException {
        try {
            Validate.notBlank(weblabName, "weblab cannot be empty");
            Validate.notBlank(encryptedCustomerId, "customerId cannot be empty");
            Validate.notBlank(encryptedMarketplaceId, "marketplaceId cannot be empty");
            String sessionId = getSessionIdFromCustomerId(encryptedCustomerId);
            SessionInfo sessionInfo = SessionInfo.fromSessionAndCustomerId(sessionId, encryptedCustomerId);
            IWeblabAllocationProvider weblabProvider = getWeblabAllocationProvider(encryptedMarketplaceId);
            return Optional.ofNullable(weblabProvider.getTreatmentAssignmentAndRecordTrigger(weblabName, sessionInfo))
                           .orElseGet(() -> {
                               log.warn("null response received from weblab: {} for customerId: {}, marketplaceId: {}", weblabName, encryptedCustomerId, encryptedMarketplaceId);

                               return C_TREATMENT;
                           });
        }
        catch (NullPointerException | IllegalArgumentException e) {
            String errorMessage = String.format("weblab: %s, customerId: %s, or marketplaceId: %s is blank.", weblabName, encryptedCustomerId, encryptedMarketplaceId);
            log.warn(errorMessage, e);
            emitFailureMetrics(WEBLAB_FAILURE);

            return C_TREATMENT;
        }
        catch (Exception e) {
            String errorMessage = String.format("unable to get treatment weblab: %s, customerId: %s, marketplaceId: %s.", weblabName, encryptedCustomerId, encryptedMarketplaceId);
            log.warn(errorMessage, e);
            emitFailureMetrics(WEBLAB_FAILURE);

            return C_TREATMENT;
        }
    }

    private IWeblabAllocationProvider getWeblabAllocationProvider(String marketplaceId) {
        if (!weblabProviderMap.containsKey(marketplaceId)) {
            IWeblabAllocationProvider weblabProvider = weblabAllocationFactory.getInstance(marketplaceId);
            TypeCreator<IWeblabAllocationProvider> weblabTypeCreator = ChameleonCreator.create(weblabProvider, IWeblabAllocationProvider.class);
            weblabProviderMap.put(marketplaceId, weblabTypeCreator.createInstance());
        }
        return weblabProviderMap.get(marketplaceId);
    }

    // We need to provide a sessionId of format xxx-xxxxxxx-xxxxxxx to avoid ForesterInvalidRequestException
    // https://w.amazon.com/index.php/Weblab/Service/WeblabAllocationProvider#Option_2:_Compact_and_faster
    private String getSessionIdFromCustomerId(String customerId) {
        BigInteger id = new BigInteger(customerId, 36);
        String base10 = id.toString(10);
        StringBuilder session = new StringBuilder("00000000000000000");  // 17 times the '0' character
        session.replace(Math.max(0, 17 - base10.length()), 17, base10);
        session.insert(3, '-');//put the hyphens in the correct position for a 3-7-7 session ID
        session.insert(11, '-');
        return session.substring(0, 19);
    }

    private void emitFailureMetrics(String msg) {
        metricsManager.get().addMetric(msg, 1, Unit.ONE);
        metricsManager.pop();
    }
}