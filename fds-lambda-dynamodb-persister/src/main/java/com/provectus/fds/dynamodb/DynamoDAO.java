package com.provectus.fds.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;

import java.util.List;

public class DynamoDAO {
    private final String tableName;
    private final DynamoDB client;

    public DynamoDAO(String tableName, AmazonDynamoDB amazonDynamoDB) {
        this.client = new DynamoDB(amazonDynamoDB);
        this.tableName = tableName;
    }

    public List<Item> batchGet(List<PrimaryKey> keys) {
        BatchGetItemOutcome result  = client.batchGetItem(
                new TableKeysAndAttributes(tableName).withPrimaryKeys(keys.toArray(new PrimaryKey[keys.size()]))
        );
        return result.getTableItems().get(tableName);
    }

    public BatchWriteItemResult batchWrite(List<Item> items) {
        TableWriteItems threadTableWriteItems = new TableWriteItems(tableName).withItemsToPut(items);
        BatchWriteItemOutcome result = client.batchWriteItem(threadTableWriteItems);
        return result.getBatchWriteItemResult();
    }
}
