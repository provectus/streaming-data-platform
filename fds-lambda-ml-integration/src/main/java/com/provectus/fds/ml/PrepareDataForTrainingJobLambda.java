package com.provectus.fds.ml;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.sagemaker.model.CreateTrainingJobResult;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.provectus.fds.ml.utils.IntegrationModuleHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.provectus.fds.ml.processor.CsvRecordProcessor;

public class PrepareDataForTrainingJobLambda implements RequestHandler<KinesisEvent, List<S3Event>> {

    static class LambdaConfiguration extends Configuration {
        private String athenaKey;
        private String athenaDatabase;
        private String modelOutputPath;

        public String getAthenaKey() {
            if (athenaKey == null)
                athenaKey = getOrThrow("ATHENA_KEY");
            return athenaKey;
        }

        public String getAthenaDatabase() {
            if (athenaDatabase == null) {
                athenaDatabase = getOrThrow("ATHENA_DATABASE");
            }
            return athenaDatabase;
        }

        public String getModelOutputPath() {
            if (modelOutputPath == null)
                modelOutputPath = getOrThrow("MODEL_OUTPUT_PATH");
            return modelOutputPath;
        }
    }


    private static final Logger logger = LogManager.getLogger(PrepareDataForTrainingJobLambda.class);
    private final IntegrationModuleHelper h = new IntegrationModuleHelper();
    private final LambdaConfiguration config = new LambdaConfiguration();

    private final ObjectMapper mapper
            = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final JobDataGenerator jobDataGenerator = new JobDataGenerator();
    private final JobRunner jobRunner = new JobRunner();

    public static class PrepareDataForTrainingJobLambdaException extends RuntimeException {
        PrepareDataForTrainingJobLambdaException(Throwable e) {
            super(e);
        }
    }

    @Override
    public List<S3Event> handleRequest(KinesisEvent input, Context context) {

        List<S3Event> results = new ArrayList<>();
        try {
            logger.debug("Processing Kinesis event: {}", h.writeValueAsString(input, mapper));

            for (KinesisEvent.KinesisEventRecord r : input.getRecords()) {
                S3Event s3Event = mapper.readerFor(S3Event.class)
                        .readValue(r.getKinesis().getData().array());
                results.add(handleRequest(s3Event, context));
            }
        } catch (IOException e) {
            throw new RuntimeException(logger.throwing(e));
        }
        return results;
    }

    private S3Event handleRequest(S3Event s3Event, Context context) {
        logger.debug("Received S3 event: {}", h.writeValueAsString(s3Event, mapper));
        String configBucket = config.getBucket();

        boolean isParquetUpdated = false;

        for (S3EventNotification.S3EventNotificationRecord record : s3Event.getRecords()) {
            String eventKey = record.getS3().getObject().getKey();
            String eventBucket = record.getS3().getBucket().getName();

            logger.debug("Got an event with s3://{}/{}", eventBucket, eventKey);

            if (eventKey.startsWith("parquet/impressions/") && eventBucket.equals(configBucket)) {
                logger.info("Found key with 'parquet/impressions/': {}, {}", eventKey, record.getEventName());

                isParquetUpdated = true;
                break;
            }
        }

        if (isParquetUpdated) {
            try {

                logger.debug("Starting data preparation for the training job");
                Map<String, String> statistic
                        = jobDataGenerator.generateTrainingData(config).getStatistic();

                logger.debug("Data was prepared. Training part has {}, validation part has {} records",
                        statistic.get(CsvRecordProcessor.TOTAL_TRAIN_RECORDS_PROCESSED),
                        statistic.get(CsvRecordProcessor.TOTAL_VERIFY_RECORDS_PROCESSED));

                logger.debug("Starting training job");
                CreateTrainingJobResult jobResult
                        = jobRunner.createJob(config,
                                statistic.get(JobRunner.TRAIN),
                                statistic.get(JobRunner.VALIDATION));
                logger.info("Training job started. ARN is: {}", jobResult.getTrainingJobArn());
            } catch (Exception e) {
                throw new PrepareDataForTrainingJobLambdaException(e);
            }
        }
        return s3Event;
    }
}
