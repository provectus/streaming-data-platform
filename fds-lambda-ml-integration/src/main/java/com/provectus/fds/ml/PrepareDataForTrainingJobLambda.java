package com.provectus.fds.ml;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.sagemaker.model.CreateTrainingJobResult;
import com.provectus.fds.ml.utils.IntegrationModuleHelper;

import java.util.Map;

import static com.provectus.fds.ml.processor.CsvRecordProcessor.TOTAL_TRAIN_RECORDS_PROCESSED;
import static com.provectus.fds.ml.processor.CsvRecordProcessor.TOTAL_VERIFY_RECORDS_PROCESSED;

public class PrepareDataForTrainingJobLambda implements RequestHandler<S3Event, S3Event> {

    static final String ATHENA_REGION_ID = "ATHENA_REGION_ID";
    static final String ATHENA_REGION_ID_DEF = "us-west-2";

    static final String ATHENA_DATABASE = "ATHENA_DATABASE";
    static final String ATHENA_DATABASE_DEF = "default";

    static final String ATHENA_OUTPUT_LOCATION = "ATHENA_OUTPUT_LOCATION";
    static final String ATHENA_OUTPUT_LOCATION_DEF = "s3://newfdsb/athena/";

    static final String SLEEP_AMOUNT_IN_MS = "SLEEP_AMOUNT_IN_MS";
    static final String SLEEP_AMOUNT_IN_MS_DEF = "1000";

    static final String CLIENT_EXECUTION_TIMEOUT = "CLIENT_EXECUTION_TIMEOUT";
    static final String CLIENT_EXECUTION_TIMEOUT_DEF = "0";

    static final String S3_BUCKET = "S3_BUCKET";
    static final String S3_BUCKET_DEF = "newfdsb";

    static final String S3_KEY = "S3_KEY";
    static final String S3_KEY_DEF = "athena/";

    static final String TRAINING_FACTOR = "TRAINING_FACTOR";
    static final String TRAINING_FACTOR_DEF = "90";

    static final String VERIFICATION_FACTOR = "VERIFICATION_FACTOR";
    static final String VERIFICATION_FACTOR_DEF = "10";

    static final String MODEL_OUTPUT_PATH = "MODEL_OUTPUT_PATH";
    static final String MODEL_OUTPUT_PATH_DEF = "s3://newfdsb/ml/model";


    public static class PrepareDataForTrainingJobLambdaException extends RuntimeException {
        PrepareDataForTrainingJobLambdaException(Throwable e) {
            super(e);
        }
    }

    @Override
    public S3Event handleRequest(S3Event s3Event, Context context) {
        context.getLogger().log(String.format("Received: %s", s3Event.toString()));

        boolean isParquetUpdated = false;

        for (S3EventNotification.S3EventNotificationRecord record : s3Event.getRecords()) {
            String s3Key = record.getS3().getObject().getKey();

            if (s3Key.startsWith("parquet/")) {
                isParquetUpdated = true;
            }
        }

        if (isParquetUpdated) {
            try {
                IntegrationModuleHelper h = new IntegrationModuleHelper();

                context.getLogger().log("Starting data preparation for the training job");
                Map<String, String> statistic = new JobDataGenerator().generateTrainingData(h);

                context.getLogger().log("Data was prepared. Training part has "
                        + statistic.get(TOTAL_TRAIN_RECORDS_PROCESSED)
                        + ", validation part has "
                        + TOTAL_VERIFY_RECORDS_PROCESSED
                        + " records");
                context.getLogger().log("Starting training job");
                CreateTrainingJobResult jobResult = new JobRunner()
                        .createJob(
                                h, false,
                                statistic.get("train"),
                                statistic.get("validation"));
                context.getLogger().log("Training job started. ARN is: " + jobResult.getTrainingJobArn());
            } catch (Exception e) {
                throw new PrepareDataForTrainingJobLambdaException(e);
            }
        }
        return s3Event;
    }
}
