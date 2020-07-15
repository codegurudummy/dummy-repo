package example;


import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.cloudformation.AmazonCloudFormation;
import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;


public class MissingPagination {
    DynamoDB dynamoClient;
    DynamoDBMapper mapper;
    private AmazonCloudFormationClient cloudformation;

    // Missing
    public Set<String> missingPagination_1(){

        this.tableName = EXPIRY_TABLE;

        Set<String> expiredStores = new HashSet<String>();

        Condition stateCondition = new Condition().
                withComparisonOperator(ComparisonOperator.EQ.toString()).
                withAttributeValueList(new AttributeValue().withS("P"));

        Map<String, Condition> conditions = new HashMap<String,Condition>();
        conditions.put("STATUS",stateCondition);

        ScanRequest scanRequest = new ScanRequest()
                .withTableName(tableName)
                .withScanFilter(conditions)
                .withAttributesToGet(Arrays.asList("STORE_ID"));


        ScanResult result = dynamoClient.scan(scanRequest);
        if(result==null || result.getScannedCount()==0){
            logger.info("zero scanned");
        }

        logger.info("Set of expired stores found");

        for (Map<String, AttributeValue> item : result.getItems()) {

            String storeID = item.get("STORE_ID").getS();
            expiredStores.add(storeID);

        }

        return expiredStores;

    }

    // Missing
    // Adapted from
    // https://code.amazon.com/packages/AwsGlueMlArtifactsDeletionLambda/blobs/0b683300e7a9e42b10a7392a52a9e7f3d64308b2/--/src/com/amazon/glue/ml/lambda/artifacts/deletion/OldMLTransformArtifactsDeletion.java#L128-L133
    public Set<String> missingPagination_2(AmazonS3 s3Client, TransformCleanupEntry transformCleanupEntry) {
        MLModelArtifact mlModelArtifact = new MLModelArtifact(transformCleanupEntry.getAccountId(), transformCleanupEntry.getTransformId(),
                "dummyFileName");

        String bucketName = mlModelArtifact.bucketName(stage, region);
        String key = String.format("%s/%s/", transformCleanupEntry.getAccountId(), transformCleanupEntry.getTransformId());
        List<S3ObjectSummary> s3ObjectSummaries = s3Client.listObjects(bucketName, key).getObjectSummaries();
        List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();
        for (S3ObjectSummary s3ObjectSummary : s3ObjectSummaries) {
            log.info("Deleting file : {} with bucketName : {}", s3ObjectSummary.getKey(), bucketName);
            keys.add(new DeleteObjectsRequest.KeyVersion(s3ObjectSummary.getKey()));
        }
        if (!keys.isEmpty()) {
            s3Client.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(keys));
        }
    }

    // Missing
    // Adapted from
    // https://code.amazon.com/packages/AWSServerlessAppsRepoStorage/blobs/6c58cdb1b0bffb657cea856afb1f450ef10a6003/--/src/com/amazonaws/serverlessappsrepo/storage/S3StorageService.java#L323-L341
    public Set<String> missingPagination_3(final AmazonS3 s3Client, final String bucketName, final String prefix) {
        ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withPrefix(prefix);

        ListObjectsV2Result listObjectsV2Result;

        try (Timer.Context s3ListObjectsTimer = MetricBuilder.of(getClass(), "s3ListObjectsAPI.totalTime")
                .timer()
                .time()) {
            log.info("List s3 objects.. bucket: {}, prefix: {}", bucketName, prefix);
            listObjectsV2Result = s3Client.listObjectsV2(listObjectsV2Request);
        }


        return listObjectsV2Result.getObjectSummaries().stream()
                .map(S3ObjectSummary::getKey)
                .collect(Collectors.toList());
    }

    // Missing
    // Adapted from
    // https://code.amazon.com/packages/BrowseItemClassificationServiceLambdaJava/blobs/5eaf5c68ba4188fe786074cc1921f2af04d00be5/--/src/com/amazon/catalog/browse/bics/lambda/handler/MetricsQueryResultHandler.java#L41
    public Set<String> missingPagination_4(Event event, Context context) throws InvalidRequestException {

        checkNotNull(event, "Event cannot be null.");
        checkNotNull(event.getExecutionContext(), "Event execution context cannot be null.");

        final String executionContext = event.getExecutionContext();
        if (executionContext.equals(EXECUTION_CONTEXT_CAC_ONLY)
                || executionContext.equals(EXECUTION_CONTEXT_BOTH)) {

            if (event.getPluginConfig() != null && event.getPluginConfig().containsKey(QUERY_EXECUTION_ID)) {

                // Get Athena client
                AmazonAthena client = null;
                try {
                    client = AthenaClientProvider.getAmazonAthena();
                } catch (Exception e) {
                    context.getLogger().log("Unexpected exception" + e);
                    throw new IllegalStateException(e);
                }

                // Get the query result set
                GetQueryResultsRequest queryResultsRequest = new GetQueryResultsRequest().withQueryExecutionId(event.getPluginConfig().get
                        (QUERY_EXECUTION_ID));
                GetQueryResultsResult queryResultsResult = client.getQueryResults(queryResultsRequest);
                ResultSet resultSet = queryResultsResult.getResultSet();

                // Extract the required metrics
                // Update the metric metadata and data points
                event.setGenerationDate(DateTimeUtils.getDateTimeNow());
                EventUtils.addDataPoints(QueryManager.getMetricsResult(resultSet), event, CATC_PROGRAM_NAME, CATC_RAW_DATASET);
            }

            event.setGenerationDate(DateTimeUtils.getDateTimeNow());
            return event;
        }

        return event;
    }

    // Missing
    // Adapted from
    // https://code.amazon.com/packages/WellArchitectedExternalService/blobs/ea925ff83263e1dca79cfe9495a3b13e269cd725/--/src/com/amazon/wellarchitectedexternalservice/lambda/util/S3ClientUtils.java#L23
    // Tricky because they check isEmpty() and size(), but pagination is still needed.
    public void missingPagination_5(final AmazonS3 s3Client, final String bucketName,
                                    final String pattern) {
        final List<String> objectList = s3Client.listObjectsV2(new ListObjectsV2Request()
                .withBucketName(bucketName))
                .getObjectSummaries().stream()
                .map(S3ObjectSummary::getKey)
                .filter(key -> key.matches(pattern))
                .collect(Collectors.toList());
        if (!objectList.isEmpty()) {
            s3Client.deleteObjects(new DeleteObjectsRequest(bucketName)
                    .withKeys(objectList.toArray(new String[objectList.size()])));
        }
        return objectList.size();
    }

    // Not missing
    public Set<String> paginationPresent_1(){

        this.tableName = EXPIRY_TABLE;

        Set<String> expiredStores = new HashSet<String>();

        Condition stateCondition = new Condition().
                withComparisonOperator(ComparisonOperator.EQ.toString()).
                withAttributeValueList(new AttributeValue().withS("P"));

        Map<String, Condition> conditions = new HashMap<String,Condition>();
        conditions.put("STATUS",stateCondition);

        Map<String,AttributeValue> lastEvaluated = null;



        logger.info("Set of expired stores found");
        do{
            ScanRequest scanRequest = new ScanRequest()
                    .withTableName(tableName)
                    .withScanFilter(conditions)
                    .withExclusiveStartKey(lastEvaluated)
                    .withAttributesToGet(Arrays.asList("STORE_ID"));

            ScanResult result = dynamoClient.scan(scanRequest);

            for (Map<String, AttributeValue> item : result.getItems()) {

                String storeID = item.get("STORE_ID").getS();
                expiredStores.add(storeID);

            }

            lastEvaluated = result.getLastEvaluatedKey();


        }while(lastEvaluated!=null);


        if(expiredStores.size()==0){
            logger.error("No expired stores to be found");
        }

        return expiredStores;

    }

    // Returned
    public ScanResult returned_1(){

        this.tableName = EXPIRY_TABLE;

        Set<String> expiredStores = new HashSet<String>();

        Condition stateCondition = new Condition().
                withComparisonOperator(ComparisonOperator.EQ.toString()).
                withAttributeValueList(new AttributeValue().withS("P"));

        Map<String, Condition> conditions = new HashMap<String,Condition>();
        conditions.put("STATUS",stateCondition);

        Map<String,AttributeValue> lastEvaluated = null;



        logger.info("Set of expired stores found");
        do{
            ScanRequest scanRequest = new ScanRequest()
                    .withTableName(tableName)
                    .withScanFilter(conditions)
                    .withExclusiveStartKey(lastEvaluated)
                    .withAttributesToGet(Arrays.asList("STORE_ID"));

            return dynamoClient.scan(scanRequest);
        }while(lastEvaluated!=null);


        return null;

    }

    // In do-while, not missing
    public void noWhileNotMissing_1(IndexWriter indexWriter, AmazonDynamoDBClient dynamoDbClient) throws IOException {
        ScanResult result = null;
        do {
            ScanRequest req = new ScanRequest();
            req.setTableName(AliasRecord.ISBN_TABLE_NAME);
            if (result != null) {
                req.setExclusiveStartKey(result.getLastEvaluatedKey());
            }
            result = dynamoClient.scan(req);
            System.out.println(result);
        } while (result.getLastEvaluatedKey() != null);
    }

    // Checking isTruncated, not missing
    private static List<ObjectListing> isTruncatedNotMissing_1(ListObjectsRequest request) {
        List<ObjectListing> lists = new ArrayList<>();
        ObjectListing first = s3Client.listObjects(request);
        lists.add(first);
        ObjectListing prev = first;
        while (prev.isTruncated()) {
            ObjectListing curr = s3Client.listNextBatchOfObjects(prev);
            prev = curr;
            lists.add(curr);
        }
        return lists;
    }

    // Not missing auto-paginated mapper is used.
    public void autoPaginatedMapperNotMissing_1() {
        System.out.println(mapper.scan(DynamoSessionItem.class, new DynamoDBScanExpression()));
    }

    // Not missing - only need the firsts
    public long onlyFirst_notMissing_1(final String materialName) {
        final List<Map<String, AttributeValue>> items = dynamoClient.query(
                new QueryRequest()
                        .withTableName(tableName)
                        .withConsistentRead(Boolean.TRUE)
                        .withKeyConditions(
                                Collections.singletonMap(
                                        DEFAULT_HASH_KEY,
                                        new Condition().withComparisonOperator(
                                                ComparisonOperator.EQ).withAttributeValueList(
                                                new AttributeValue().withS(materialName))))
                        .withLimit(1).withScanIndexForward(false)
                        .withAttributesToGet(DEFAULT_RANGE_KEY)).getItems();
        if (items.isEmpty()) {
            return -1L;
        } else {
            return Long.parseLong(items.get(0).get(DEFAULT_RANGE_KEY).getN());
        }
    }

    // Not missing - stack name is set!
    public String oneElementNotMissing_1() throws Exception {

        DescribeStacksRequest wait = new DescribeStacksRequest();
        wait.setStackName(deployment.getCloudFormationStackName());
        System.out.println(new AmazonCloudFormation().describeStacks(wait).getStacks());
    }

    // Not missing - stack name is set (#2)
    public void oneElementNotMissing_2() {
        AmazonEC2 client = null;

        try {
            client = getEc2Client();
            DescribeSecurityGroupsResult result;
            DescribeSecurityGroupsRequest describeSecurityGroupsRequest =
                    new DescribeSecurityGroupsRequest().withGroupIds(config.getACLGroupIdForVPC());
            result = client.describeSecurityGroups(describeSecurityGroupsRequest);
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    // Missing - describe without restrictions
    public void describeNoFilters_1() {
        AmazonEC2 client = null;

        try {
            client = getEc2Client();
            DescribeSecurityGroupsResult result;
            DescribeSecurityGroupsRequest describeSecurityGroupsRequest = new DescribeSecurityGroupsRequest();
            result = client.describeSecurityGroups(describeSecurityGroupsRequest);
            System.out.println(result.getSecurityGroups());
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    // Missing
    public void listAndStream_1() {
        DescribeStacksResponse stacks =
                cloudformation.describeStacks(DescribeStacksRequest.builder().build());

        this.stacks =
                stacks.getStacks()
                        .stream()
                        .map(CfnStack::from)
                        .collect(Collectors.toMap(CfnStack::getName, Function.identity()));
    }

    // Missing.
    // Used to produce dupes.
    protected void possibleDupe_1(BindDomainsContext ctx, String record, String zoneId) {
        AmazonRoute53 r53;

        ResourceRecordSet resourceRecordSet = null;

        ListResourceRecordSetsResult listResourceRecordSets = r53.listResourceRecordSets(new ListResourceRecordSetsRequest(zoneId));

        for (ResourceRecordSet rrs : listResourceRecordSets.getResourceRecordSets()) {
            System.out.println(rss);
        }
    }

    // Missing.
    // Used to be missed.
    private List<String> s3listObjectsV2_1(final AmazonS3 s3Client, final String bucketName, final String prefix) {
        AmazonS3 s3Client;

        ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withPrefix(prefix);

        ListObjectsV2Result listObjectsV2Result;

        try (Timer.Context s3ListObjectsTimer = MetricBuilder.of(getClass(), "s3ListObjectsAPI.totalTime")
                .timer()
                .time()) {
            log.info("List s3 objects.. bucket: {}, prefix: {}", bucketName, prefix);
            listObjectsV2Result = s3Client.listObjectsV2(listObjectsV2Request);
        }

        return listObjectsV2Result.getObjectSummaries().stream()
                .map(S3ObjectSummary::getKey)
                .collect(Collectors.toList());
    }

    // Not missing.
    // Used to be FP.
    private void listAndAdd_1() {
        AmazonS3 s3Client;

        ListObjectsV2Result result = s3Client.listObjectsV2(request);
        List<S3ObjectSummaryHolder> objectSummaryHolders = new ArrayList<>();
        for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
            objectSummaryHolders.add(new S3ObjectSummaryHolder(group, objectSummary));
        }
        while (result.isTruncated() && result.getContinuationToken() != null) {
            result = s3Client.listObjectsV2(new ListObjectsV2Request().withBucketName(s3Bucket).withPrefix(s3Prefix).withContinuationToken(result.getContinuationToken()));
            for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                objectSummaryHolders.add(new S3ObjectSummaryHolder(group, objectSummary));
            }
        }
        return objectSummaryHolders;
    }

    // Not missing
    // 2 reasons not to flag:
    // - the request is generated by another procedure
    // - Output size is checked
    // Adapted from https://code.amazon.com/packages/ElasticAlgorithmComputeServiceWorkflows/blobs/2058a658f14cb4f567943abedc60ebe68870d30d/--/src/com/amazon/elasticalgorithmcomputeservice/service/CloudFormationClient.java#L47-L57
    public void sizeIsChecked_1(final String stackName) {
        final AmazonCloudFormation cloudFormation;

        log.info("Describing stack {}", stackName);
        DescribeStacksRequest describeStacksRequest = cfRequestFactory.generateDescribeStacksRequest(stackName);
        DescribeStacksResult describeStacksResult = cloudFormation.describeStacks(describeStacksRequest);

        Preconditions.checkState(describeStacksResult.getStacks().size() == 1, "Unexpected response for DescribeStacks: "
                + "expected 1, got " + describeStacksResult.getStacks().size());
        Stack stack = describeStacksResult.getStacks().get(0);
        log.info("Describe response is {}", stack);
        return stack;
    }

    // Not missing
    // Not "real" DDB client
    // Adapted from
    // https://code.amazon.com/packages/WellArchitectedExternalService/blobs/4d660e94f9ea778c1b4213e9d281823e081878bb/--/src/com/amazon/wellarchitectedexternalservice/lambda/dao/DynamoDBTemplateNotificationDao.java#L67-L91
    public void wrongClient_1(final NotificationType type,
                              final String targetId, final String sourceId) {
        DynamoDBDao<TemplateNotification> dynamoDBDao;

        final String hashKey = targetId;
        String rangeKeyPrefix = NotificationStatus.ACTIVE + "." + type;
        if (sourceId != null) {
            rangeKeyPrefix += "." + sourceId;
        }
        final Condition condition = ExpressionSpecBuilder
                .S(NotificationTableMetaData.HASH_KEY_NAME).eq(hashKey)
                .and(ExpressionSpecBuilder.S(NotificationTableMetaData.RANGE_KEY_NAME).beginsWith(rangeKeyPrefix));
        final QueryExpressionSpec expressionSpec = new ExpressionSpecBuilder()
                .withKeyCondition(condition)
                .buildForQuery();
        final DynamoDBQueryRequest request = DynamoDBQueryRequest.builder()
                .tableName(NotificationTableMetaData.TABLE_NAME)
                .expression(expressionSpec)
                .indexName(Optional.empty())
                .build();
        final List<TemplateNotification> notificationList = dynamoDBDao.query(request);
        Optional<TemplateNotification> notification = Optional.empty();
        if (!notificationList.isEmpty()) {
            notification = Optional.of(notificationList.get(0));
        }
        return notification;
    }

    // Not missing: maxKeys = 1
    // Loosely based on
    // https://code.amazon.com/packages/TextractAESEventsListenerLambda/blobs/bf53d38cafe11f3fe1d651bd8b5721c17e608b3b/--/src/com/amazon/textractaeseventslistenerlambda/Helpers/TextractHasDataOfCustomer.java#L45
    public boolean oneElementNotMissing_3(final String accountId) {
        AmazonS3 s3Client;

        final ListObjectsV2Request req = new ListObjectsV2Request()
                .withBucketName(customerBucket)
                .withPrefix(accountId)
                .withMaxKeys(1);

        if (s3Client.listObjectsV2(req).getKeyCount() > 0) {
            return true;
        }
        return false;
    }

    // Based on
    // https://code.amazon.com/packages/TrebuchetCoralService/blobs/e50bfb43fa6c31b2e3704d370d0a7e4fc3c0b440/--/src/com/amazonaws/trebuchet/dao/TrebuchetDAO.java#L664-L739
    // Received positive feedback from wave-2 customer.
    public void missingPagination_6(AmazonDynamoDBClient dynamoDbClient) {
        List<String> tables = dynamoDbClient.listTables().getTableNames();

        // feature and feature-archive tables

        if (!tables.contains(Constants.FEATURES_TABLE_NAME)) {
            createTable(FeatureDAO.class, 10L, 10L, null);
        }
        if (!tables.contains(Constants.FEATURES_ARCHIVE_TABLE_NAME)) {
            createArchiveTable(FeatureDAO.class, Constants.FEATURES_ARCHIVE_TABLE_NAME, 5L, 5L);
        }
    }
}
