package com.provectus.fds.ingestion;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
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
import com.provectus.fds.models.events.Location;
import com.provectus.fds.models.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class LocationsHandler implements RequestHandler<KinesisEvent, List<String>> {
    private static final Logger logger = LoggerFactory.getLogger(LocationsHandler.class);

    private static final String OK = "OK";
    private static final String FAILED = "FAILED";

    private static final String STREAM_NAME = "STREAM_NAME";
    private static final String STREAM_NAME_DEFAULT = "locations";
    private static final String AWS_REGION = "AWS_REGION";
    private static final String AWS_REGION_DEFAULT = "us-west-2";
    private static final String WRITE_BATCH_SIZE = "WRITE_BATCH_SIZE";
    private static final String WRITE_BATCH_SIZE_DEFAULT = "100";

    private static final String PREFIX = "locations/";
    private static final String SEPARATOR = ",";
    private static final int RETRY_COUNT = 3;

    private final String streamName = System.getenv().getOrDefault(STREAM_NAME, STREAM_NAME_DEFAULT);
    private final String region = System.getenv().getOrDefault(AWS_REGION, AWS_REGION_DEFAULT);
    private final int batchSize = Integer.parseInt(System.getenv().getOrDefault(WRITE_BATCH_SIZE, WRITE_BATCH_SIZE_DEFAULT));

    private final AmazonKinesis producer;
    private ObjectMapper mapper;

    public LocationsHandler() {
        producer = getProducer();
        mapper = new ObjectMapper();

        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
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
            String key = record.getS3().getObject().getKey().replace('+', ' ');

            logger.debug("S3 event. Bucket: {}. Key: {}.", bucket, key);

            if (key.startsWith(PREFIX)) {
                try {
                    key = URLDecoder.decode(key, "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    logger.error("Error occurred while decoding s3 object", e);
                    return FAILED;
                }

                // Download locations csv
                AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
                S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucket, key));

                // Parse csv
                List<Location> buffer = new LinkedList<>();

                try (InputStream objectData = s3Object.getObjectContent();
                     BufferedReader reader = new BufferedReader(new InputStreamReader(objectData))) {
                    logger.info("Start processing: " + key);

                    String row;
                    while ((row = reader.readLine()) != null) {
                        if (row.isEmpty()) {
                            continue;
                        }

                        // Write a batch portion
                        if (buffer.size() == batchSize) {
                            write(buffer);
                            buffer.clear();
                        }

                        try {
                            buffer.add(Location.from(row.split(SEPARATOR)));
                        } catch (Exception e) {
                            logger.error(String.format("Can't parse row: '%s'", row), e);
                        }
                    }

                    // Write the last batch portion
                    if (!buffer.isEmpty()) {
                        write(buffer);
                        buffer.clear();
                    }
                } catch (Exception e) {
                    logger.error("Error occurred while processing S3 object", e);
                    return FAILED;
                }

                logger.info("Processed: " + key);
            }
        }

        return OK;
    }

    private void write(List<Location> locations) {
        List<PutRecordsRequestEntry> requestEntries = new ArrayList<>();

        for (Location location : locations) {
            try {
                PutRecordsRequestEntry entry = new PutRecordsRequestEntry();
                entry.setData(ByteBuffer.wrap(JsonUtils.write(location)));
                entry.setPartitionKey(String.format("%d", location.getCampaignItemId()));
                requestEntries.add(entry);
            } catch (JsonProcessingException e) {
                logger.error("Can't prepare location for writing", e);
            }
        }

        writeWithRetry(requestEntries);
    }

    private void writeWithRetry(List<PutRecordsRequestEntry> requestEntries) {
        PutRecordsRequest request = new PutRecordsRequest();
        request.setStreamName(streamName);
        request.setRecords(requestEntries);

        PutRecordsResult result = producer.putRecords(request);

        int retry = 0;

        while (result.getFailedRecordCount() > 0 && retry < RETRY_COUNT) {
            retry++;

            logger.debug(String.format("A retry attempt of writing to Kinesis. Retry: %d", retry));

            List<PutRecordsResultEntry> resultEntries = result.getRecords();
            List<PutRecordsRequestEntry> failedEntries = new ArrayList<>();

            for (int i = 0; i < resultEntries.size(); i++) {
                PutRecordsResultEntry resultEntry = resultEntries.get(i);

                if (resultEntry.getErrorCode() != null) {
                    PutRecordsRequestEntry requestEntry = requestEntries.get(i);
                    failedEntries.add(requestEntry);

                    logger.error("Kinesis write error. ErrorCode: {}. ErrorMessage: {}. Entry: {}.",
                            resultEntry.getErrorCode(), resultEntry.getErrorMessage(), requestEntry);
                }
            }

            requestEntries = failedEntries;
            request.setRecords(requestEntries);
            result = producer.putRecords(request);
        }

        if (result.getFailedRecordCount() > 0) {
            throw new IllegalStateException(String.format("Can't write to Kinesis after %d attempts", retry));
        }
    }

    private AmazonKinesis getProducer() {
        AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
        clientBuilder.setRegion(region);

        return clientBuilder.build();
    }
}