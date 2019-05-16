package com.provectus.fds.ml;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.provectus.fds.ml.processor.AthenaConfig;
import com.provectus.fds.ml.processor.AthenaProcessor;
import com.provectus.fds.ml.processor.CsvRecordProcessor;
import com.provectus.fds.ml.utils.IntegrationModuleHelper;

import static com.provectus.fds.ml.PrepareDataForTrainingJobLambda.*;

class JobDataGenerator {

    CsvRecordProcessor generateTrainingData(IntegrationModuleHelper h) throws Exception {
        return generateTrainingData(h, false);
    }

    CsvRecordProcessor generateTrainingData(IntegrationModuleHelper h, boolean enableLocalCredentials) throws Exception {

        ClientConfiguration configuration = new ClientConfiguration()
                .withClientExecutionTimeout(Integer.parseInt(h.getConfig(CLIENT_EXECUTION_TIMEOUT, CLIENT_EXECUTION_TIMEOUT_DEF)));

        AmazonAthenaClientBuilder athenaClientBuilder = AmazonAthenaClientBuilder.standard()
                .withRegion(System.getenv("ATHENA_REGION_ID"))
                .withClientConfiguration(configuration);

        AmazonAthena client = athenaClientBuilder.build();

        int trainingFactor = Integer.parseInt(h.getConfig(TRAINING_FACTOR, TRAINING_FACTOR_DEF));
        int verificationFactor = Integer.parseInt(h.getConfig(VERIFICATION_FACTOR, VERIFICATION_FACTOR_DEF));

        int gcd = h.gcd(trainingFactor, verificationFactor);
        try (CsvRecordProcessor recordProcessor
                = new CsvRecordProcessor(
                System.getenv("S3_BUCKET"),
                h.getConfig(ATHENA_S3_KEY, ATHENA_S3_KEY_DEF),
                trainingFactor / gcd, verificationFactor / gcd)) {

            AthenaConfig athenaConfig = new AthenaConfig();
            athenaConfig.setClient(client);
            athenaConfig.setDbName(System.getenv("ATHENA_DATABASE"));
            athenaConfig.setOutputLocation(getOutputLocation(h));
            athenaConfig.setQuery(h.getResourceFileAsString("categorized_bids.sql"));
            athenaConfig.setSleepTime(Long.parseLong(h.getConfig(SLEEP_AMOUNT_IN_MS, SLEEP_AMOUNT_IN_MS_DEF)));
            athenaConfig.setRecordProcessor(recordProcessor);

            AthenaProcessor athenaProcessor = new AthenaProcessor();
            athenaProcessor.process(athenaConfig);

            return recordProcessor;
        }
    }

    private String getOutputLocation(IntegrationModuleHelper h) {
        return String.format("s3://%s/%s",
                System.getenv("S3_BUCKET"),
                h.getConfig(ATHENA_S3_KEY, ATHENA_S3_KEY_DEF));
    }
}
