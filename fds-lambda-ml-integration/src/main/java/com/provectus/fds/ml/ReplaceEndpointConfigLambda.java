package com.provectus.fds.ml;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public class ReplaceEndpointConfigLambda implements RequestHandler<KinesisEvent, List<S3Event>> {

    private static final Logger logger = LogManager.getLogger(ReplaceEndpointConfigLambda.class);
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public List<S3Event> handleRequest(KinesisEvent input, Context context) {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        logger.info("Processing Kinesis event");

        List<S3Event> results = new ArrayList<>();

        for (KinesisEvent.KinesisEventRecord r : input.getRecords()) {
            try {
                S3Event s3Event = mapper.readerFor(S3Event.class)
                        .readValue(r.getKinesis().getData().array());
                results.add(handleRequest(s3Event, context));
            } catch (IOException e) {
                logger.throwing(e);
            }
        }
        return results;
    }

    @SuppressWarnings("unused")
    private S3Event handleRequest(S3Event s3Event, Context context) {
        logger.info("Received S3 event");
        String configBucket = System.getenv("S3_BUCKET");

        for (S3EventNotification.S3EventNotificationRecord record : s3Event.getRecords()) {
            String eventBucket = record.getS3().getBucket().getName();
            String eventKey = record.getS3().getObject().getKey();

            logger.info("Got an event with s3://{}/{}", eventBucket, eventKey);

            if (eventKey.endsWith("model.tar.gz")  && eventBucket.equals(configBucket)) {

                logger.info("Starting updating endpoint process");

                EndpointUpdater.EndpointUpdaterBuilder updaterBuilder = new EndpointUpdater.EndpointUpdaterBuilder();
                updaterBuilder
                        .withEndpointName(getEndpointName())
                        .withServicePrefix(getServicePrefx())
                        .withRegionId(getRegionId())
                        .withSageMakerRole(getSageMakerRoleArn())
                        .withDataUrl(String.format("s3://%s/%s", eventBucket, eventKey));

                updaterBuilder.build().updateEndpoint();
            }
        }
        return s3Event;
    }

    private String getRegionId() {
        return System.getenv("REGION_ID");
    }

    private String getSageMakerRoleArn() {
        return System.getenv("SAGEMAKER_ROLE_ARN");
    }

    private String getServicePrefx() {
        return System.getenv("SERVICE_PREFIX");
    }

    private String getEndpointName() {
        return System.getenv("ENDPOINT_NAME");
    }
}
