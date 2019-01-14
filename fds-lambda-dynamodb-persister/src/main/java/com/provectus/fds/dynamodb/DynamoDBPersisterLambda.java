package com.provectus.fds.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DynamoDBPersisterLambda implements RequestHandler<KinesisEvent, Integer>  {
    private final static String DYNAMO_TABLE_ENV = "DYNAMO_TABLE";
    public static final String DYNAMO_TABLE_DEFAULT = "aggregations";
    private final ItemMapper itemMapper = new ItemMapper();
    private final DynamoDAO dynamoDAO;


    public DynamoDBPersisterLambda() {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        this.dynamoDAO = new DynamoDAO(System.getenv().getOrDefault(DYNAMO_TABLE_ENV, DYNAMO_TABLE_DEFAULT), client);
    }

    @Override
    public Integer handleRequest(KinesisEvent event, Context context) {
        LambdaLogger logger = context.getLogger();


        if (event == null || event.getRecords() == null) {
            logger.log("Event contains no data" + System.lineSeparator());
            return null;
        } else {
            logger.log("Received " + event.getRecords().size() +
                    " records from " + event.getRecords().get(0).getEventSourceARN() + System.lineSeparator());
        }

        Map<PrimaryKey, Item> merged = itemMapper.mergeItems(
                event.getRecords().stream().map( r -> r.getKinesis().getData()).collect(Collectors.toList())
        );

        Collection<PrimaryKey> keys = merged.keySet();
        Collection<Item> created = merged.values();

        List<Item> old = dynamoDAO.batchGet(keys);
        List<Item> items = itemMapper.mergeItems(created, old);

        dynamoDAO.batchWrite(items);
        return event.getRecords().size();
    }
}
