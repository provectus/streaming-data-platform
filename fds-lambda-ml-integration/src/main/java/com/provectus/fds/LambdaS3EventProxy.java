package com.provectus.fds;


import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class LambdaS3EventProxy implements RequestHandler<S3Event, String> {
    private static final Logger logger = LogManager.getLogger(LambdaS3EventProxy.class);
    private static final String OK = "OK";
    private static final String FAILED = "FAILED";

    public LambdaS3EventProxy() {
    }

    public String handle(S3Event s3event) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        logger.info("Output stream for S3 events: " + System.getenv("STREAM_NAME"));

        AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
        logger.info("Client builder created");

        AmazonKinesis kinesisClient = clientBuilder.build();
        logger.info("Kinesis client created");

        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(System.getenv("STREAM_NAME"));

        List<PutRecordsRequestEntry> entryList = new ArrayList<>();

        PutRecordsRequestEntry entry = new PutRecordsRequestEntry();
        entry.setData(ByteBuffer.wrap(mapper.writeValueAsBytes(s3event)));
        entry.setPartitionKey(String.format("partitionKey-%d", s3event.hashCode()));
        entryList.add(entry);

        putRecordsRequest.setRecords(entryList);
        logger.info("PutRecordsRequest created");

        PutRecordsResult putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
        logger.debug("Put Result: " + putRecordsResult);

        return "OK";
    }

    @Override
    public String handleRequest(S3Event s3event, Context context) {
        try {
            return handle(s3event);
        } catch (JsonProcessingException e) {
            logger.catching(e);
            return null;
        }
    }
}
