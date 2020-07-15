package concurrency;

import com.amazon.s3rogueone.DependencyException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * This class manages DynamoDb tables
 */
@Singleton
@Slf4j
public class DdbTableManager {

    private final AmazonDynamoDB ddb;
    private final Long throughput;
    private final Sleeper sleeper;

    @Inject
    public DdbTableManager(AmazonDynamoDB ddb, @Named("rogueone.datastore.throughput") Long throughput, Sleeper sleeper) {
        this.ddb = ddb;
        this.throughput = throughput;
        this.sleeper = sleeper;
    }

    /**
     * Creates the provided app table name in DynamoDb.
     * <p>
     * Note that if the table name ends with "_Log" then "timestamp" and "ttl" attributes will be added automatically.
     * <p>
     * TODO: This method should not rely on the table naming convention. Consider two methods. One for "normal", one for "logging" tables.
     *
     * @return {@code true} if table was created, {@code false} otherwise
     */
    public boolean createAppTable(String appTableName) {
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        AttributeDefinition attributeDefinition = new AttributeDefinition("stepId", "S");
        attributeDefinitions.add(attributeDefinition);

        KeySchemaElement keySchemaElement = new KeySchemaElement("stepId", "HASH");
        List<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(keySchemaElement);
        // add special ttl sort element, and setup the TTL attribute.
        if (appTableName.endsWith("_Log")) {
            keySchema.add(new KeySchemaElement().withAttributeName("timestamp").withKeyType("RANGE"));
            attributeDefinitions.add(new AttributeDefinition().withAttributeName("timestamp").withAttributeType("N"));
        }
        ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput(this.throughput, this.throughput);

        CreateTableRequest createTableRequest = new CreateTableRequest(attributeDefinitions, appTableName, keySchema,
            provisionedThroughput);

        String tableStatus = "";
        try {
            tableStatus = ddb.describeTable(appTableName).getTable().getTableStatus();

            // if the above statement successed, then the table already exists and cannot be recreated.
            // This case should not happen, so returning false to indicate that something is not right.
            log.info("Table: %s already exists", appTableName);
            return false;
        } catch (ResourceNotFoundException e) {
            // Table does not exist, which is expected, so will proceeed to create it
        }

        try {
            boolean tableMissing = true;
            ddb.createTable(createTableRequest);

            while (tableMissing) {
                try {
                    tableStatus = ddb.describeTable(appTableName).getTable().getTableStatus();
                } catch (ResourceNotFoundException resourceNotFoundException) {
                    // Table not created yet, eventually consistent. Wait for it.
                    try {
                        sleeper.sleep(TimeUnit.SECONDS, 2L);
                    } catch (InterruptedException interruptedException) {
                        // Something woke me early, nothing to worry about
                    }
                }
                if ("ACTIVE".equals(tableStatus)) {
                    log.info("Table: %s created successfully", appTableName);
                    if (appTableName.endsWith("_Log")) { //Set the TTL spec now that the log table has been created.
                        UpdateTimeToLiveRequest req = new UpdateTimeToLiveRequest();
                        req.setTableName(appTableName);

                        TimeToLiveSpecification ttlSpec = new TimeToLiveSpecification();
                        ttlSpec.setAttributeName("ttl");
                        ttlSpec.setEnabled(true);

                        req.withTimeToLiveSpecification(ttlSpec);

                        ddb.updateTimeToLive(req);
                    }
                    tableMissing = false;
                }
            }
        } catch (Exception exception) {
            log.info("Exception: " + exception);
            log.info("Cannot create table: %s .... FAILED", appTableName);
            return false;
        }

        return true;
    }

    /**
     * Delete the provided table name from DynamoDb.
     *
     * @return {@code true} if table was delete, {@code false} otherwise
     */
    public boolean deleteTable(String tableName) {
        try {
            ddb.describeTable(tableName).getTable().getTableStatus();
        } catch (ResourceNotFoundException resourceNotFoundException) {
            // if the above statement successed, then the table does not exists and cannot be deleted.
            // This case should not happen, so returning false to indicate that something is not right.
            log.info("Table: %s does not exist", tableName);
            return false;
        }

        ddb.deleteTable(tableName);

        int i = 0;
        while (i < 30) {
            i++;
            try {
                ddb.describeTable(tableName).getTable().getTableStatus();
            } catch (ResourceNotFoundException desiredException) {
                log.info("Table: %s deleted successfully", tableName);
                return true;
            }

            try {
                sleeper.sleep(TimeUnit.SECONDS, 2L);
            } catch (InterruptedException e) {
                log.error("Thread interrupted", e);
            }
        }

        // waiting for table to be deleted more than a minute will fail as it still exists
        log.info("Table: %s delete FAILED", tableName);
        return false;
    }

    /**
     * Get the list of existing tables held by RogueOne account.
     *
     * @return Set of existing tables
     * @throws DependencyException if any underlying service exceptions are thrown
     */
    public Set<String> getExistingTables() {
        Set<String> existingTables = new HashSet<>();
        String lastTableName = null;
        boolean moreTables = true;
        while (moreTables) {
            ListTablesRequest listRequest = new ListTablesRequest();
            if (Objects.nonNull(lastTableName)) {
                listRequest.setExclusiveStartTableName(lastTableName);
            }

            try {
                ListTablesResult response = ddb.listTables(listRequest);
                existingTables.addAll(response.getTableNames());
                lastTableName = response.getLastEvaluatedTableName();

                if (Objects.isNull(lastTableName)) {
                    moreTables = false;
                }
            } catch (Exception e) {
                String message = "Unable to list tables due to exception";
                log.error(message, e);
                throw new DependencyException(message, e);
            }
        }
        return existingTables;
    }

    /**
     * Create a table and await activation of table.
     *
     * @return true if table was created and is now active, false otherwise
     */
    public boolean createTableIfNotExistsAndAwaitActive(CreateTableRequest request) {
        boolean tableCreationRequestSubmitted = TableUtils.createTableIfNotExists(ddb, request);
        if (!tableCreationRequestSubmitted) {
            log.error("Unable to create table. Table request failed to submit.");
            return false;

        }
        try {
            TableUtils.waitUntilActive(ddb, request.getTableName());
            return true;
        } catch (InterruptedException e) {
            log.error("Interrupted while awaiting active table {}", request.getTableName());
            Thread.currentThread().interrupt();
        }
        return false;
    }
}
