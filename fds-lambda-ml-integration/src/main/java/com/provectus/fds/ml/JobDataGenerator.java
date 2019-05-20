package com.provectus.fds.ml;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.provectus.fds.ml.processor.AthenaConfig;
import com.provectus.fds.ml.processor.AthenaProcessor;
import com.provectus.fds.ml.processor.CsvRecordProcessor;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

class JobDataGenerator {

    private static final long SLEEP_TIME = 1000L;

    private static final int CLIENT_EXECUTION_TIMEOUT = 0;
    private static final int TRAINING_FACTOR = 90;
    private static final int VERIFICATION_FACTOR = 10;

    private static final String SQL_RESOURCE = "categorized_bids.sql";

    CsvRecordProcessor generateTrainingData(PrepareDataForTrainingJobLambda.LambdaConfiguration config) throws Exception {

        ClientConfiguration configuration = new ClientConfiguration()
                .withClientExecutionTimeout(CLIENT_EXECUTION_TIMEOUT);

        AmazonAthenaClientBuilder athenaClientBuilder = AmazonAthenaClientBuilder.standard()
                .withRegion(config.getRegion())
                .withClientConfiguration(configuration);

        AmazonAthena client = athenaClientBuilder.build();

        try (CsvRecordProcessor recordProcessor
                = new CsvRecordProcessor(config.getBucket(),
                config.getAthenaKey(),
                TRAINING_FACTOR, VERIFICATION_FACTOR)) {

            AthenaConfig athenaConfig = new AthenaConfig();
            athenaConfig.setClient(client);
            athenaConfig.setDbName(config.getAthenaDatabase());
            athenaConfig.setOutputLocation(getOutputLocation(config));
            athenaConfig.setQuery(getSqlString());
            athenaConfig.setSleepTime(SLEEP_TIME);
            athenaConfig.setRecordProcessor(recordProcessor);

            AthenaProcessor athenaProcessor = new AthenaProcessor();
            athenaProcessor.process(athenaConfig);

            return recordProcessor;
        }
    }

    private String getSqlString() {
        InputStream is = getClass().getClassLoader().getResourceAsStream(JobDataGenerator.SQL_RESOURCE);
        if (is != null) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            return reader.lines().collect(Collectors.joining(System.lineSeparator()));
        }
        return null;
    }

    private String getOutputLocation(PrepareDataForTrainingJobLambda.LambdaConfiguration config) {
        return String.format("s3://%s/%s",
                config.getBucket(),
                config.getAthenaKey());
    }
}
