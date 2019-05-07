package com.provectus.fds.ml;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification;

public class ReplaceEndpointConfigLambda implements RequestHandler<S3Event, S3Event> {
    @Override
    public S3Event handleRequest(S3Event s3Event, Context context) {
        for (S3EventNotification.S3EventNotificationRecord record : s3Event.getRecords()) {
            String s3bucket = record.getS3().getBucket().getName();
            String s3Key = record.getS3().getObject().getKey();

            if (s3Key.endsWith("model.tar.gz")) {
                EndpointUpdater.EndpointUpdaterBuilder updaterBuilder = new EndpointUpdater.EndpointUpdaterBuilder();
                updaterBuilder
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
}
