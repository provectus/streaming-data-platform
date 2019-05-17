package com.provectus.fds.ml;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.sagemaker.model.CreateTrainingJobResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.provectus.fds.ml.utils.IntegrationModuleHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.provectus.fds.ml.processor.CsvRecordProcessor.TOTAL_TRAIN_RECORDS_PROCESSED;
import static com.provectus.fds.ml.processor.CsvRecordProcessor.TOTAL_VERIFY_RECORDS_PROCESSED;

public class PrepareDataForTrainingJobLambda implements RequestHandler<KinesisEvent, List<S3Event>> {

    static final String SLEEP_AMOUNT_IN_MS = "SLEEP_AMOUNT_IN_MS";
    static final String SLEEP_AMOUNT_IN_MS_DEF = "1000";

    static final String CLIENT_EXECUTION_TIMEOUT = "CLIENT_EXECUTION_TIMEOUT";
    static final String CLIENT_EXECUTION_TIMEOUT_DEF = "0";

    static final String ATHENA_S3_KEY = "ATHENA_S3_KEY";
    static final String ATHENA_S3_KEY_DEF = "athena/";

    static final String TRAINING_FACTOR = "TRAINING_FACTOR";
    static final String TRAINING_FACTOR_DEF = "90";

    static final String VERIFICATION_FACTOR = "VERIFICATION_FACTOR";
    static final String VERIFICATION_FACTOR_DEF = "10";

    private static final Logger logger = LogManager.getLogger(PrepareDataForTrainingJobLambda.class);
    private final IntegrationModuleHelper h = new IntegrationModuleHelper();

    private final ObjectMapper mapper
            = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

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
        String configBucket = System.getenv("S3_BUCKET");

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
                        = new JobDataGenerator().generateTrainingData(h).getStatistic();

                logger.debug("Data was prepared. Training part has {}, validation part has {} records",
                        statistic.get(TOTAL_TRAIN_RECORDS_PROCESSED),
                        statistic.get(TOTAL_VERIFY_RECORDS_PROCESSED));

                logger.debug("Starting training job");
                CreateTrainingJobResult jobResult = new JobRunner()
                        .createJob(h,
                                statistic.get("train"),
                                statistic.get("validation"));
                logger.info("Training job started. ARN is: {}", jobResult.getTrainingJobArn());
            } catch (Exception e) {
                throw new PrepareDataForTrainingJobLambdaException(e);
            }
        }
        return s3Event;
    }
}
