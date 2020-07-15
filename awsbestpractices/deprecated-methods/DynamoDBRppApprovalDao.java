package com.amazon.pricing.retailpricepublisher.approval.dao;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newConcurrentHashSet;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import amazon.platform.config.Realm;
import amazon.pricing.core.batching.BatchResult;
import amazon.pricing.core.batching.BatchResultBuilder;
import amazon.pricing.model.retailsystems.AmazonDomain;
import amazon.pricing.model.retailsystems.DataCenterRegion;

import com.amazon.pricing.ddb.partition.manager.tools.TableNameGenerator;
import com.amazon.pricing.dynamodb.AttributeValues;
import com.amazon.pricing.dynamodb.client.DynamoDBRetryableClient;
import com.amazon.pricing.dynamodb.codec.DynamoDBCodec;
import com.amazon.pricing.dynamodb.tablename.DomainAndRealmAwareTableName;
import com.amazon.pricing.model.OfferListingKey;
import com.amazon.pricing.retailpricepublisher.approval.codec.OfferListingKeyCodec;
import com.amazon.pricing.retailpricepublisher.approval.model.ProposedPrice;
import com.amazon.pricing.retailpricepublisher.exception.ApprovalException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;

public class DynamoDBRppApprovalDao implements RppApprovalDao {

    private static Logger logger = LoggerFactory.getLogger(DynamoDBRppApprovalDao.class);

    private final AmazonDynamoDB dynamoDBClient;

    public DynamoDBRppApprovalDao(
            final AmazonDynamoDB dynamoDBClient) {
        this.dynamoDBClient = checkNotNull(dynamoDBClient);
    }

    private PutItemRequest firstChainedCall(final ProposedPrice proposedPrice) {
        // Condition for not replacing existing record.
        final Map<String, ExpectedAttributeValue> expectedAttributeValuesByName = newHashMap();
        final ExpectedAttributeValue nullExpect = new ExpectedAttributeValue().withComparisonOperator(ComparisonOperator.NULL);
        expectedAttributeValuesByName.put(OFFER_LISTING_KEY_ATTRIBUTE, nullExpect);
        expectedAttributeValuesByName.put(VERSION_ATTRIBUTE, nullExpect);

        final PutItemRequest request = new PutItemRequest()
                .withExpected(expectedAttributeValuesByName)
                .withTableName(proposedPricesTableName.getTableName())
                .withItem(this.proposedPriceCodec.serialize(proposedPrice));
        return request;
    }

    private PutItemRequest laterChainedCall(final ProposedPrice proposedPrice) {
        // Condition for not replacing existing record.
        final Map<String, ExpectedAttributeValue> expectedAttributeValuesByName = newHashMap();
        final ExpectedAttributeValue nullExpect = new ExpectedAttributeValue().withComparisonOperator(ComparisonOperator.NULL);
        expectedAttributeValuesByName.put(OFFER_LISTING_KEY_ATTRIBUTE, nullExpect);
        expectedAttributeValuesByName.put(VERSION_ATTRIBUTE, nullExpect);

        final PutItemRequest request = new PutItemRequest()
                .withTableName(proposedPricesTableName.getTableName())
                .withItem(this.proposedPriceCodec.serialize(proposedPrice))
                .withExpected(expectedAttributeValuesByName);
        return request;
    }

    private PutItemRequest normalCall(final ProposedPrice proposedPrice) {
        // Condition for not replacing existing record.
        final Map<String, ExpectedAttributeValue> expectedAttributeValuesByName = newHashMap();
        final ExpectedAttributeValue nullExpect = new ExpectedAttributeValue().withComparisonOperator(ComparisonOperator.NULL);
        expectedAttributeValuesByName.put(OFFER_LISTING_KEY_ATTRIBUTE, nullExpect);
        expectedAttributeValuesByName.put(VERSION_ATTRIBUTE, nullExpect);

        final PutItemRequest request = new PutItemRequest();
        request.withExpected(expectedAttributeValuesByName);
        return request;
    }

    // No deprecated methods here, but some names _similar_ to deprecated.
    // Testing that the matching logic isn't too loose.
    // Copied from
    // https://code.amazon.com/packages/AWSDeepSenseControlPlane/blobs/8262abf1b3e2cb2eedbb885700c9ccb1baedbb72/--/src/com/amazonaws/deepsense/controlplane/wf/util/BotChannelEndpointMapper.java#L51-L68
    public BotChannelEndpoint wrongMethods(final String awsAccountId, final GetApplicationChannelResponse response) {
        Validate.notEmpty(awsAccountId);
        Validate.notNull(response);

        return BotChannelEndpoint.builder()
                .withApplicationAlias(response.getApplicationAlias())
                .withApplicationName(response.getApplicationName())
                .withAwsAccountId(awsAccountId)
                .withChannelType(response.getType())
                .withEndpointId(response.getEndpointId())
                .withEndpointUrl(response.getEndpointUrl())
                .withEndpointToUrlMap(response.getChannelMetadata())
                .withName(response.getName())
                .withRoleArn(response.getIamRoleArn())
                .withCustomerKmsKeyId(response.getKmsKeyId())
                .withStatus(response.getStatus())
                .build();
    }
}