package com.provectus.fds.ml;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReplaceEndpointConfigLambda implements RequestHandler<KinesisEvent, List<S3Event>> {

    @Override
    public List<S3Event> handleRequest(KinesisEvent input, Context context) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        context.getLogger().log(String.format("Processing Kinesis input: %s", input));

        List<S3Event> results = new ArrayList<>();

        for (KinesisEvent.KinesisEventRecord r : input.getRecords()) {
            try {
                S3Event s3Event = mapper.readerFor(S3Event.class)
                        .readValue(r.getKinesis().getData().array());
                results.add(handleRequest(s3Event, context));
            } catch (IOException e) {
                context.getLogger().log(e.getMessage());
            }
        }
        return results;
    }

    private S3Event handleRequest(S3Event s3Event, Context context) {
        for (S3EventNotification.S3EventNotificationRecord record : s3Event.getRecords()) {
            String s3bucket = record.getS3().getBucket().getName();
            String s3Key = record.getS3().getObject().getKey();

            if (s3Key.endsWith("model.tar.gz")) {
                EndpointUpdater.EndpointUpdaterBuilder updaterBuilder = new EndpointUpdater.EndpointUpdaterBuilder();
                updaterBuilder
                        .withEndpointName(getEndpointName())
                        .withServicePrefix(getServicePrefx())
                        .withRegionId(getRegionId())
                        .withSageMakerRole(getSageMakerRoleArn())
                        .withDataUrl(String.format("s3://%s/%s", s3bucket, s3Key));

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
