package com.provectus.fds.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;

import java.util.stream.Collectors;

public class DynamoDBPersisterLambda implements RequestHandler<KinesisEvent, Integer>  {
    private final static String DYNAMO_TABLE_ENV = "DYNAMO_TABLE";
    public static final String DYNAMO_TABLE_DEFAULT = "aggregations";
    private final ItemMapper itemMapper = new ItemMapper();
    private final DynamoDB dynamoDB;
    private final String dynamoTableName;


    public DynamoDBPersisterLambda() {
        this.dynamoTableName =  System.getenv().getOrDefault(DYNAMO_TABLE_ENV, DYNAMO_TABLE_DEFAULT);

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                //.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2"))
                .build();

        this.dynamoDB = new DynamoDB(client);
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

        TableWriteItems threadTableWriteItems = new TableWriteItems(dynamoTableName).withItemsToPut(
            event.getRecords().stream()
                    .map( r -> itemMapper.fromByteBuffer(r.getKinesis().getData()))
                    .collect(Collectors.toList())
        );

        dynamoDB.batchWriteItem(threadTableWriteItems);

        return event.getRecords().size();
    }
}
