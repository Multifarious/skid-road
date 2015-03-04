package io.ifar.skidroad.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;

/**
 *
 */
public class DynamoDBMigrations {


    public static void createTalliesTable(String talliesTable, AmazonDynamoDB ddb,
                                          long readCapacityUnits, long writeCapacityUnits)
    {
        CreateTableRequest ctr = new CreateTableRequest()
                .withAttributeDefinitions(new AttributeDefinition("rolling_cohort", ScalarAttributeType.S),
                                          new AttributeDefinition("node_id", ScalarAttributeType.S))
                .withKeySchema(new KeySchemaElement("rolling_cohort", KeyType.HASH),
                               new KeySchemaElement("node_id", KeyType.RANGE))
                .withTableName(talliesTable)
                .withProvisionedThroughput(
                        new ProvisionedThroughput()
                                .withReadCapacityUnits(readCapacityUnits)
                                .withWriteCapacityUnits(writeCapacityUnits)
                );
        ddb.createTable(ctr);
    }

    public static void createLogFilesTable(String logFilesTable, AmazonDynamoDB ddb,
                                           long readCapacityUnits, long writeCapacityUnits)
    {
        CreateTableRequest ctr = new CreateTableRequest()
                .withAttributeDefinitions(new AttributeDefinition("rolling_cohort", ScalarAttributeType.S),
                                          new AttributeDefinition("file_id", ScalarAttributeType.S))
                .withKeySchema(new KeySchemaElement("rolling_cohort", KeyType.HASH),
                               new KeySchemaElement("file_id", KeyType.RANGE))
                .withTableName(logFilesTable)
                .withProvisionedThroughput(
                        new ProvisionedThroughput()
                                .withReadCapacityUnits(readCapacityUnits)
                                .withWriteCapacityUnits(writeCapacityUnits)
                );
        ddb.createTable(ctr);

    }

}
