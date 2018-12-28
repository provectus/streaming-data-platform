package com.provectus.fds.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.BatchGetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.provectus.fds.models.events.Aggregation;

import java.util.*;
import java.util.stream.StreamSupport;

import static com.provectus.fds.dynamodb.ItemMapper.CAMPAIGN_TABLE_HASH_KEY;

public class DynamoDAO {
    private final String tableName;
    private final DynamoDB client;
    private final Table table;

    public DynamoDAO(String tableName, AmazonDynamoDB amazonDynamoDB) {
        this.client = new DynamoDB(amazonDynamoDB);
        this.tableName = tableName;
        this.table = client.getTable(this.tableName);
    }

    public List<Item> batchGet(Collection<PrimaryKey> keys) {
        BatchGetItemOutcome result  = client.batchGetItem(
                new TableKeysAndAttributes(tableName).withPrimaryKeys(keys.toArray(new PrimaryKey[keys.size()]))
        );
        return result.getTableItems().get(tableName);
    }

    public BatchWriteItemResult batchWrite(Collection<Item> items) {
        TableWriteItems threadTableWriteItems = new TableWriteItems(tableName).withItemsToPut(items);
        BatchWriteItemOutcome result = client.batchWriteItem(threadTableWriteItems);
        return result.getBatchWriteItemResult();
    }
}
