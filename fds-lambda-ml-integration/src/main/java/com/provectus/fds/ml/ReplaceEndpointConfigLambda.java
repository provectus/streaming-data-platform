package com.provectus.fds.ml;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.provectus.fds.ml.utils.IntegrationModuleHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public class ReplaceEndpointConfigLambda implements RequestHandler<KinesisEvent, List<S3Event>> {

    private static final Logger logger = LogManager.getLogger(ReplaceEndpointConfigLambda.class);
    private final ObjectMapper mapper
            = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final IntegrationModuleHelper h = new IntegrationModuleHelper();

    private static final String modelFileName = "model.tar.gz";
    LambdaConfiguration config = new LambdaConfiguration();

    @Override
    public List<S3Event> handleRequest(KinesisEvent input, Context context) {
        logger.debug("Processing Kinesis event: {}", h.writeValueAsString(input, mapper));

        List<S3Event> results = new ArrayList<>();

        for (KinesisEvent.KinesisEventRecord r : input.getRecords()) {
            try {
                S3Event s3Event = mapper.readerFor(S3Event.class)
                        .readValue(r.getKinesis().getData().array());
                results.add(handleRequest(s3Event, context));
            } catch (IOException e) {
                throw new RuntimeException(logger.throwing(e));
            }
        }
        return results;
    }

    @SuppressWarnings("unused")
    private S3Event handleRequest(S3Event s3Event, Context context) {
        logger.debug("Received S3 event: {}", h.writeValueAsString(s3Event, mapper));
        String configBucket = config.getBucket();

        for (S3EventNotification.S3EventNotificationRecord record : s3Event.getRecords()) {
            String eventBucket = record.getS3().getBucket().getName();
            String eventKey = record.getS3().getObject().getKey();

            logger.info("Got an event with s3://{}/{}, {}", eventBucket, eventKey, record.getEventName());

            if (eventKey.endsWith(modelFileName)  && eventBucket.equals(configBucket)) {

                logger.info("Starting updating endpoint process");

                EndpointUpdater.EndpointUpdaterBuilder updaterBuilder
                        = new EndpointUpdater.EndpointUpdaterBuilder(config);

                updaterBuilder
                        .withEndpointName(config.getEndpoint())
                        .withServicePrefix(config.getServicePrefx())
                        .withRegionId(config.getRegion())
                        .withSageMakerRole(config.getSagemakerRole())
                        .withDataUrl(String.format("s3://%s/%s", eventBucket, eventKey));

                updaterBuilder.build().updateEndpoint();
            }
        }
        return s3Event;
    }

    static class LambdaConfiguration extends Configuration {
        private String servicePrefix;
        private Integer initialInstanceCount;

        private String getServicePrefx() {
            if (servicePrefix == null)
                servicePrefix = getOrThrow("SERVICE_PREFIX");
            return servicePrefix;
        }
        public int getInitialInstanceCount() {
            if (initialInstanceCount == null)
                initialInstanceCount = Integer.valueOf(getOrElse("PRODUCTION_VARIANT_INITIAL_INSTANCE_COUNT", "1"));

            return initialInstanceCount;
        }
    }
}
