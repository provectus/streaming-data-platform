package com.provectus.fds.ml;

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

@SuppressWarnings("unused")
public class LambdaS3EventProxy implements RequestHandler<S3Event, String> {
    private static final Logger logger = LogManager.getLogger(LambdaS3EventProxy.class);

    private final ObjectMapper mapper
            = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final LambdaConfiguration config = new LambdaConfiguration();

    private static final String PARTITION_KEY_PATTERN = "partitionKey-%d";

    public LambdaS3EventProxy() {
    }

    public String handle(S3Event s3event) throws JsonProcessingException {

        logger.debug("Handle S3 event: {}", mapper.writeValueAsString(s3event));

        AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
        AmazonKinesis kinesisClient = clientBuilder.build();

        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(config.getStreamName());

        List<PutRecordsRequestEntry> entryList = new ArrayList<>();

        PutRecordsRequestEntry entry = new PutRecordsRequestEntry();
        entry.setData(ByteBuffer.wrap(mapper.writeValueAsBytes(s3event)));
        entry.setPartitionKey(String.format(PARTITION_KEY_PATTERN, s3event.hashCode()));
        entryList.add(entry);

        putRecordsRequest.setRecords(entryList);

        logger.info("Putting S3Event into stream: '{}' '{}'",
                s3event.toString(), config.getStreamName());

        PutRecordsResult putRecordsResult = kinesisClient.putRecords(putRecordsRequest);

        String message = String.format("kinesisClient.putRecords: %s", putRecordsResult.toString());
        logger.info(message);

        return message;
    }

    @Override
    public String handleRequest(S3Event s3event, Context context) {
        try {
            return handle(s3event);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(logger.throwing(e));
        }
    }

    private static class LambdaConfiguration extends Configuration {
        private String streamName;

        private String getStreamName() {
            if (streamName == null)
                streamName = getOrThrow("STREAM_NAME");
            return streamName;
        }
    }

}
