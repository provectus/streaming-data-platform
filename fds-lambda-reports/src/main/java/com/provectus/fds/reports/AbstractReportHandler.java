package com.provectus.fds.reports;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;

public abstract class AbstractReportHandler {

    protected final AmazonDynamoDB client;

    public AbstractReportHandler() {
        this.client = AmazonDynamoDBClientBuilder.standard().build();
    }


}
