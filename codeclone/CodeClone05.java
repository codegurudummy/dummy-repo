/* Copyright 2018-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved. */
package com.amazon.malygos.webapp.g2s2;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.measure.unit.SI;
import javax.measure.unit.Unit;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.helper.Dimension;
import com.amazon.ion.IonBool;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonText;

import amazon.platform.g2s2.entity.AuthorizationStatus;
import amazon.platform.g2s2.entity.G2S2Label;
import amazon.platform.g2s2.entity.G2S2Record;
import amazon.platform.g2s2.entity.QueryResult;
import amazon.platform.g2s2.exception.G2S2ForbiddenException;
import amazon.platform.g2s2.exception.G2S2InternalServerException;
import amazon.platform.g2s2.exception.G2S2ResourceNotFoundException;
import amazon.platform.g2s2.schema.SystemFields;
import amazon.platform.g2s2.secure.ActorType;
import amazon.platform.g2s2.secure.BindleResourceType;
import amazon.platform.g2s2.secure.impl.BindleSecureProvider;

/**
 * Checks request authorization.
 */
public final class Authorizer {
    /** Metric for reporting if authorization is required. */
    static final String AUTHORIZATION_REQUIRED_METRIC = "AuthorizationRequired";
    /** Metric for reporting if authorization failed or succeeded. */
    static final String AUTHORIZATION_FAILED_METRIC = "AuthorizationFailure";
    /** Metric for reporting request rate on pass-through resources. */
    static final String AUTHORIZATION_PASS_THROUGH_RATE_METRIC = "AuthorizationPassThroughRate";

    static final int RETRY_INTERVAL = 2000;
    static final int NUM_MAX_RETRIES = 3;
    /** Bindle secure provider. */
    @Nonnull
    public final BindleSecureProvider bindleSecureProvider;
    /** Client used for talking to G2S2. */
    @Nonnull
    private final RecordReader reader;
    /** Current G2S2 client context */
    @Nonnull
    private final G2S2ClientContext context;

    /**
     * Constructor.
     *
     * @param bindleSecureProvider bindle secure provider
     * @param context              G2S2 context
     */
    private Authorizer(@Nonnull final BindleSecureProvider bindleSecureProvider,
                       @Nonnull final G2S2ClientContext context) {
        this.bindleSecureProvider = bindleSecureProvider;
        this.reader = RecordReader.of(context);
        this.context = context;
    }

    /**
     * Factory method.
     *
     * @param bindleSecureProvider bindle secure provider
     * @param context              G2S2 context
     *
     * @return authorizer
     */
    @Nonnull
    public static Authorizer create(@Nonnull final BindleSecureProvider bindleSecureProvider,
                                    @Nonnull final G2S2ClientContext context) {
        return new Authorizer(bindleSecureProvider, context);
    }

    /**
     * Grants the user and Malygos edit access to a label. Granting access through bindles takes some time to propagate,
     * so if an authorization check on the working set label is done immediately after, the request may transiently
     * fail. This method retries the permission check until the permissions pass. If all the retries fail, an exception
     * is thrown, but it's possible that there is no error and the permissions are just taking longer than normal to
     * propagate.
     *
     * @param labelName name of label to grant access to
     * @param userName  name of the current user
     * @param metrics   request metrics
     */
    public void grantAccess(@Nonnull final String labelName,
                            @Nonnull final String userName,
                            @Nonnull final Metrics metrics) throws InterruptedException {
        bindleSecureProvider.grantPermission(ActorType.PERSON,
                                             userName,
                                             BindleResourceType.LABEL,
                                             getResourceBindleName(labelName),
                                             metrics);
        bindleSecureProvider.grantPermission(ActorType.AAA_ID,
                                             "ApolloEnv:Malygos/Beta",
                                             BindleResourceType.LABEL,
                                             getResourceBindleName(labelName),
                                             metrics);
        int numTries = 0;
        for (boolean userHasPermission = false; !userHasPermission; numTries++) {
            if (numTries == NUM_MAX_RETRIES) {
                throw new G2S2InternalServerException("Unable to grant user " + userName + " access to working set in Bindles. This may be a transient error; try retrying your request.");
            }
            userHasPermission = bindleSecureProvider.hasPermission(ActorType.PERSON,
                                                                   userName,
                                                                   BindleResourceType.LABEL,
                                                                   getResourceBindleName(labelName),
                                                                   metrics);
            Thread.sleep(RETRY_INTERVAL);
        }
        numTries = 0;
        for (boolean malygosHasPermission = false; !malygosHasPermission; numTries++) {
            if (numTries == NUM_MAX_RETRIES) {
                throw new G2S2InternalServerException(
                        "Unable to grant editor access to working set in Bindles. This may be a transient error; try retrying your request.");
            }
            malygosHasPermission = bindleSecureProvider.hasPermission(ActorType.AAA_ID,
                                                                      "ApolloEnv:Malygos/Beta",
                                                                      BindleResourceType.LABEL,
                                                                      getResourceBindleName(labelName),
                                                                      metrics);
            Thread.sleep(RETRY_INTERVAL);
        }
    }

    /**
     * Checks authorization for a request.
     *
     * @param username     username of the user making the request
     * @param resourceType type of resource being modified
     * @param resourceName name of resource being modified
     * @param metrics      request metrics
     */
    public void checkAuthorization(@Nonnull final String username,
                                   @Nonnull final BindleResourceType resourceType,
                                   @Nonnull final String resourceName,
                                   @Nonnull final Metrics metrics) {
        final String resourceTypeForMetric = resourceType.toString().substring(0, 1).toUpperCase()
                + resourceType.toString().substring(1);
        final String resourceNameForMetric = context.getRegion().toBindlesResourcePrefix()
                + resourceTypeForMetric
                + ":"
                + resourceName;
        final Dimension dimension = new Dimension(resourceNameForMetric, username);
        final AuthorizationStatus authorizationStatus = getAuthorizationStatus(resourceType, resourceName, metrics);
        if (authorizationStatus == AuthorizationStatus.NOT_SECURED) {
            metrics.addRatio(AUTHORIZATION_REQUIRED_METRIC, 0, 1, dimension);
            return;
        }

        final boolean hasPermission = bindleSecureProvider.hasPermission(ActorType.PERSON,
                                                                         username,
                                                                         resourceType,
                                                                         getResourceBindleName(resourceName),
                                                                         metrics);
        if (authorizationStatus == AuthorizationStatus.PASS_THROUGH) {
            metrics.addMetric(AUTHORIZATION_PASS_THROUGH_RATE_METRIC,
                              hasPermission ? 0 : 1,
                              Unit.ONE,
                              dimension);
            metrics.addRatio(AUTHORIZATION_REQUIRED_METRIC, 0, 1, dimension);
            metrics.addRatio(AUTHORIZATION_FAILED_METRIC, 0, 1, dimension);
            return;
        }

        metrics.addRatio(AUTHORIZATION_REQUIRED_METRIC, 1, 1, dimension);

        if (!hasPermission) {
            metrics.addRatio(AUTHORIZATION_FAILED_METRIC, 1, 1, dimension);
            throw new G2S2ForbiddenException(
                    "User " + username + " is not authorized to modify " + resourceType + " " + resourceName
                            + ". The permissions for " + resourceType + " " + resourceName + " are managed via Bindles at "
                            + bindleSecureProvider.getResourceUrl(resourceType, getResourceBindleName(resourceName))
                            + ". Please contact the resource owner to get permissions.");
        }
        metrics.addRatio(AUTHORIZATION_FAILED_METRIC, 0, 1, dimension);
    }

    /**
     * Gets authorization status for a resource.
     *
     * @param resourceType type of the resource
     * @param resourceName name of the resource
     * @param metrics      request metrics
     *
     * @return the authorization status of the resource
     *
     * @throws IllegalArgumentException if resource type is unsupported
     */
    @Nonnull
    AuthorizationStatus getAuthorizationStatus(@Nonnull final BindleResourceType resourceType,
                                               @Nonnull final String resourceName,
                                               @Nonnull final Metrics metrics) {
        switch (resourceType) {
            case TABLE:
                return getTableAuthorizationStatus(resourceName, metrics);
            case LABEL:
                return getLabelAuthorizationStatus(resourceName, metrics);
            default:
                throw new IllegalArgumentException("Unsupported resource type " + resourceType);
        }
    }

    /**
     * Gets the authorization status of a label.
     *
     * @param labelName name of the label
     * @param metrics   request metrics
     *
     * @return label authorization status
     */
    @Nonnull
    private AuthorizationStatus getLabelAuthorizationStatus(@Nonnull final String labelName,
                                                            @Nonnull final Metrics metrics) {
        final long startTime = System.nanoTime();
        try {
            final Map<String, String> keyMap = new HashMap<>();
            keyMap.put(SystemFields.STAGE_VERSION_LABEL, labelName);

            QueryResult<G2S2Record> authorizations = reader.getRecords("authorize_labels", getStageVersion(), keyMap);
            if (!authorizations.hasNext()) {
                // no hint record, not authorization required.
                return AuthorizationStatus.NOT_SECURED;
            }
            return getAuthorizationStatus(authorizations.next());
        } finally {
            metrics.addTime("getLabelAuthorizationStatus.time",
                            System.nanoTime() - startTime,
                            SI.NANO(SI.SECOND));
        }
    }

    /**
     * Gets the authorization status of a table.
     *
     * @param tableName name of the table
     * @param metrics   request metrics
     *
     * @return table authorization status
     */
    @Nonnull
    private AuthorizationStatus getTableAuthorizationStatus(@Nonnull final String tableName,
                                                            @Nonnull final Metrics metrics) {
        final long startTime = System.nanoTime();
        try {
            final Map<String, String> keyMap = new HashMap<>();
            keyMap.put(SystemFields.METADATA_TABLE_NAME, tableName);

            QueryResult<G2S2Record> authorizations = reader.getRecords("authorize_tables", getStageVersion(), keyMap);
            if (!authorizations.hasNext()) {
                // no hint record, not authorization required.
                return AuthorizationStatus.NOT_SECURED;
            }
            return getAuthorizationStatus(authorizations.next());
        } finally {
            metrics.addTime("getTableAuthorizationStatus.time",
                            System.nanoTime() - startTime,
                            SI.NANO(SI.SECOND));
        }
    }

    /**
     * Gets the authorization status for a hint record.
     *
     * @param record hint record to check
     *
     * @return authorization status
     */
    @Nonnull
    AuthorizationStatus getAuthorizationStatus(@Nonnull final G2S2Record record) {
        final IonStruct payload = record.getPayload();
        final IonText accessType = (IonText) payload.get("access_type");

        if (accessType != null && !"authorized".equalsIgnoreCase(accessType.stringValue())) {
            // no access type => authorized
            // not "authorized" => no authorization needed.
            return AuthorizationStatus.NOT_SECURED;
        }

        final IonBool useBindles = (IonBool) payload.get("use_bindles");
        if (useBindles == null || !useBindles.booleanValue()) {
            // no use_bindles => access, we do not support access
            throw new G2S2ForbiddenException("Writes are not supported through editor for resources authorized "
                                                     + "using Access Service.");
        }

        final IonBool isPassThrough = (IonBool) payload.get("pass_through");
        if (isPassThrough.booleanValue()) {
            return AuthorizationStatus.PASS_THROUGH;
        }

        return AuthorizationStatus.ENFORCED;
    }

    /**
     * @return the stage version @G2S2Acl is pointing to.
     */
    @Nonnull
    String getStageVersion() {
        G2S2Label aclLabel = reader.getStageVersionLabel("G2S2Acl");
        if (aclLabel == null) {
            throw new G2S2ResourceNotFoundException("Unable to find G2S2Acl label");
        }
        return aclLabel.getStageVersion().getName();
    }

    @Nonnull
    private String getResourceBindleName(@Nonnull final String resourceName) {
        return context.getRegion().toBindlesResourcePrefix() + resourceName;
    }
}