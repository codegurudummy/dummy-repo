package com.amazon.customerfacet.orderplugin.health;

import java.util.Random;

import javax.measure.unit.SI;

import com.amazon.customerfacet.orderplugin.RequestContext;
import com.amazon.customerfacet.orderplugin.facet.registry.FacetWhitelist;
import com.amazon.customerfacet.orderplugin.model.CheckoutType;
import com.amazon.customerfacet.orderplugin.retriever.FacetWhitelistRetriever;
import com.amazon.customerfacet.orderplugin.retriever.FacetWhitelistRetrieverFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.coral.service.HealthCheckStrategy;
import com.amazon.customerfacet.orderplugin.model.FacetLookupKey;
import com.amazon.customerfacet.orderplugin.retriever.CustomerFacetRetriever;
import com.amazon.customerfacet.orderplugin.retriever.CustomerFacetRetrieverFactory;

import amazon.security.AmzUid;
import lombok.AllArgsConstructor;

/**
 * Performs deep health checks on the service. Making sure that the service is properly started before being added to
 * the VIP.
 */
@AllArgsConstructor
public class DeepHealthCheck implements HealthCheckStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(DeepHealthCheck.class);

    // customer ids 0 and 1 are considered invalid by PCS
    // The smallest id ensures that we never create random customer identifiers below this value
    private static final int CUSTOMER_ID_GENERATION_SMALLEST_ID = 100;
    private static final int CUSTOMER_ID_GENERATION_POOL_SIZE = 100000;

    private final CustomerFacetRetrieverFactory customerFacetRetrieverFactory;
    private final String obfuscatedDefaultMarketplaceId;
    private final MetricsFactory metricsFactory;
    private final FacetWhitelistRetrieverFactory retailFacetWhitelistRetrieverFactory;
    private final FacetWhitelistRetrieverFactory digitalFacetWhitelistRetrieverFactory;
    private final ThreadLocal<RequestContext> threadRequestContext;

    /**
     * Performs a deep health check on the service.
     *
     * @return true if the service is healthy
     */
    @Override
    public boolean isHealthy() {
        final long startTime = System.currentTimeMillis();
        Metrics metrics = this.metricsFactory.newMetrics();
        metrics.addProperty("Operation", "DeepHealthCheck");
        metrics.addTime("StartTime", startTime, SI.MILLI(SI.SECOND));
        final RequestContext requestContext = new RequestContext();
        requestContext.setMetrics(metrics);
        this.threadRequestContext.set(requestContext);

        LOG.info("Running Deep Health Check");
        try {
            return checkRetailFacetRetrieval() && checkDigitalFacetRetrieval();
        } finally {
            this.threadRequestContext.remove();
            metrics.close();
        }
    }

    private boolean checkRetailFacetRetrieval() {
        LOG.info("Checking Retail Facet Retrieval");

        final CustomerFacetRetriever facetRetriever = this.customerFacetRetrieverFactory.create(CheckoutType.RETAIL);
        setFacetWhitelist(this.retailFacetWhitelistRetrieverFactory);
        final FacetLookupKey lookupKey = generateFacetLookupKey();

        try {
            facetRetriever.getCurrentCustomerBenefits(lookupKey);
            return true;
        } catch (final Exception ex) {
            return false;
        }

    }

    private boolean checkDigitalFacetRetrieval() {
        LOG.info("Checking Digital Facet Retrieval");

        final CustomerFacetRetriever facetRetriever = this.customerFacetRetrieverFactory.create(CheckoutType.DIGITAL);
        setFacetWhitelist(this.digitalFacetWhitelistRetrieverFactory);
        final FacetLookupKey lookupKey = generateFacetLookupKey();

        try {
            facetRetriever.getCurrentCustomerBenefits(lookupKey);
            return true;
        } catch (final Exception ex) {
            return false;
        }

    }

    private void setFacetWhitelist(FacetWhitelistRetrieverFactory facetWhitelistRetrieverFactory) {
        final FacetWhitelistRetriever facetWhitelistRetriever = facetWhitelistRetrieverFactory.create();
        final FacetWhitelist facetWhitelist = facetWhitelistRetriever.getFacetWhitelist();
        this.threadRequestContext.get().setFacetWhitelist(facetWhitelist);
    }

    private FacetLookupKey generateFacetLookupKey() {
        final String obfuscatedCustomerId = generatePseudoRandomObfuscatedCustomerId();
        return new FacetLookupKey(obfuscatedCustomerId, this.obfuscatedDefaultMarketplaceId);
    }

    /**
     * Use to generate a pseudo random customer id for testing. Used by the service primer as well.
     *
     * @return customerId string
     */
    protected static String generatePseudoRandomObfuscatedCustomerId() {
        final Random random = new Random();
        final int nonObfuscatedRandomCustomerId = random.nextInt(CUSTOMER_ID_GENERATION_POOL_SIZE)
                + CUSTOMER_ID_GENERATION_SMALLEST_ID;
        return AmzUid.encryptCustomerID(nonObfuscatedRandomCustomerId);
    }

}