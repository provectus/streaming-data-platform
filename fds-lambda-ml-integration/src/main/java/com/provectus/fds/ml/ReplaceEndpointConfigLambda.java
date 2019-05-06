package com.provectus.fds.ml;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.sagemaker.AmazonSageMakerAsync;
import com.amazonaws.services.sagemaker.AmazonSageMakerAsyncClient;
import com.amazonaws.services.sagemaker.AmazonSageMakerAsyncClientBuilder;
import com.amazonaws.services.sagemaker.model.*;

import java.util.Collections;
import java.util.List;

public class ReplaceEndpointConfigLambda implements RequestHandler<S3Event, S3Event> {
    @Override
    public S3Event handleRequest(S3Event s3Event, Context context) {
        for (S3EventNotification.S3EventNotificationRecord record : s3Event.getRecords()) {
            String s3Key = record.getS3().getObject().getKey();

            if (s3Key.startsWith(getModelOutputPath()) && s3Key.endsWith("model.tar.gz")) {
                new EndpointUpdater().updateEndpoint();
            }
        }
        return s3Event;
    }

    protected String getRegionId() {
        return System.getenv("REGION_ID");
    }

    protected String getModelOutputPath() {
        return System.getenv("MODEL_OUTPUT_PATH");
    }

    protected String getSageMakerRoleArn() {
        return System.getenv("SAGEMAKER_ROLE_ARN");
    }
}
