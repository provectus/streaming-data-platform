package com.provectus.fds.ingestion;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.provectus.fds.models.events.Location;
import com.provectus.fds.models.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class LocationsHandler implements RequestHandler<KinesisEvent, List<String>> {
    private static final Logger logger = LoggerFactory.getLogger(LocationsHandler.class);

    private static final String OK = "OK";
    private static final String FAILED = "FAILED";

    private static final String STREAM_NAME = "STREAM_NAME";
    private static final String AWS_REGION = "AWS_REGION";
    private static final String RECORD_MAX_BUFFERED_TIME = "RECORD_MAX_BUFFERED_TIME";
    private static final String MAX_CONNECTIONS = "MAX_CONNECTIONS";
    private static final String REQUEST_TIMEOUT = "REQUEST_TIMEOUT";
    private static final String AGGREGATION_ENABLED = "AGGREGATION_ENABLED";
    private static final String AGGREGATION_MAX_COUNT = "AGGREGATION_MAX_COUNT";
    private static final String AGGREGATION_MAX_SIZE = "AGGREGATION_MAX_SIZE";

    private static final String PREFIX = "locations/";
    private static final String SEPARATOR = ",";

    private final String streamName = System.getenv().getOrDefault(STREAM_NAME, "locations");
    private final String region = System.getenv().getOrDefault(AWS_REGION, "us-west-2");
    private final long recordMaxBufferedTime = Long.parseLong(System.getenv().getOrDefault(RECORD_MAX_BUFFERED_TIME, "100"));
    private final long maxConnections = Long.parseLong(System.getenv().getOrDefault(MAX_CONNECTIONS, "24"));
    private final long requestTimeout = Long.parseLong(System.getenv().getOrDefault(REQUEST_TIMEOUT, "600"));
    private final boolean aggregationEnabled = Boolean.parseBoolean(System.getenv().getOrDefault(AGGREGATION_ENABLED, "true"));
    private final long getAggregationMaxCount = Long.parseLong(System.getenv().getOrDefault(AGGREGATION_MAX_COUNT, "4294967295"));
    private final long getAggregationMaxSize = Long.parseLong(System.getenv().getOrDefault(AGGREGATION_MAX_SIZE, "51200"));

    private final ObjectMapper mapper;
    private final KinesisProducer producer;
    private final FutureCallback<UserRecordResult> callback;

    public LocationsHandler() {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        producer = new KinesisProducer(new KinesisProducerConfiguration()
                .setRecordMaxBufferedTime(recordMaxBufferedTime)
                .setMaxConnections(maxConnections)
                .setRequestTimeout(requestTimeout)
                .setAggregationEnabled(aggregationEnabled)
                .setAggregationMaxCount(getAggregationMaxCount)
                .setAggregationMaxSize(getAggregationMaxSize)
                .setRegion(region));

        callback = new FutureCallback<UserRecordResult>() {
            @Override
            public void onFailure(Throwable t) {
                logger.error("Error occurred while writing to Kinesis", t);
            }

            @Override
            public void onSuccess(UserRecordResult result) {
                // Success
            }
        };
    }

    @Override
    public List<String> handleRequest(KinesisEvent event, Context context) {
        List<String> results = new ArrayList<>();

        for (KinesisEvent.KinesisEventRecord record : event.getRecords()) {
            try {
                S3Event s3Event = mapper.readerFor(S3Event.class)
                        .readValue(record.getKinesis().getData().array());
                results.add(handleRequest(s3Event));
            } catch (IOException e) {
                logger.error("Can't process S3 event", e);
            }
        }

        return results;
    }

    private String handleRequest(S3Event s3Event) {
        for (S3EventNotification.S3EventNotificationRecord record : s3Event.getRecords()) {
            String bucket = record.getS3().getBucket().getName();
            String key = record.getS3().getObject().getKey();

            if (key.startsWith(PREFIX)) {
                // Download locations csv
                AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
                S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucket, key));

                // Parse csv
                try (InputStream objectData = s3Object.getObjectContent();
                     BufferedReader reader = new BufferedReader(new InputStreamReader(objectData))) {
                    logger.info("Start processing: " + key);

                    String row;
                    while ((row = reader.readLine()) != null) {
                        if (row.isEmpty()) {
                            continue;
                        }

                        try {
                            write(Location.from(row.split(SEPARATOR)));
                        } catch (Exception e) {
                            logger.error(String.format("Can't process location row: '%s'", row), e);
                        }
                    }
                } catch (Exception e) {
                    logger.error("Error occurred while processing S3 object", e);
                    return FAILED;
                }

                logger.info("Processed: " + key);
            }
        }

        producer.flushSync();

        return OK;
    }

    private void write(Location location) {
        try {
            Futures.addCallback(
                    producer.addUserRecord(
                            streamName,
                            location.getAppUID(),
                            ByteBuffer.wrap(JsonUtils.write(location))),
                    callback);
        } catch (JsonProcessingException e) {
            logger.error("Can't prepare location for writing", e);
        }
    }
}