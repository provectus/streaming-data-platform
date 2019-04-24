package com.provectus.fds.ml;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification;

public class PrepareDataForTrainingJobLambda implements RequestHandler<S3Event, S3Event> {

    private static final String ATHENA_REGION_ID = "ATHENA_REGION_ID";
    private static final String ATHENA_REGION_ID_DEF = "us-west-2";

    private static final String ATHENA_DATABASE = "ATHENA_DATABASE";
    private static final String ATHENA_DATABASE_DEF = "default";

    private static final String ATHENA_QUERY = "ATHENA_QUERY";
    private static final String ATHENA_QUERY_DEF = "select\n" +
            "  case i.txid when null then 0 else 1 end positive,\n" +
            "  bitwise_and(from_big_endian_64(xxhash64(to_utf8(cast(b.campaign_item_id as varchar)))), 9223372036854775807) / 9223372036854775807.0 as campaign_item_id,\n" +
            "  bitwise_and(from_big_endian_64(xxhash64(to_utf8(b.domain))), 9223372036854775807) / 9223372036854775807.0 as domain,\n" +
            "  bitwise_and(from_big_endian_64(xxhash64(to_utf8(b.creative_id))), 9223372036854775807) / 9223372036854775807.0 as creative_id,\n" +
            "  bitwise_and(from_big_endian_64(xxhash64(to_utf8(b.creative_category))), 9223372036854775807) / 9223372036854775807.0 as creative_category,\n" +
            "  bitwise_and(from_big_endian_64(xxhash64(to_utf8(cast(b.win_price as varchar)))), 9223372036854775807) / 9223372036854775807.0 as win_price\n" +
            "from bcns b tablesample bernoulli(100) \n" +
            "  left join impressions i on i.txid = b.txid\n" +
            "  where type = 'bid' -- and \n" +
            "    -- b.day >= round((to_unixtime(current_timestamp) - 86400) / 86400) and \n" +
            "    -- i.day >= round((to_unixtime(current_timestamp) - 86400) / 86400)\n" +
            "  limit 30";


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

    @Override
    public S3Event handleRequest(S3Event s3Event, Context context) {
        context.getLogger().log(String.format("Received: %s", s3Event.toString()));

        boolean isParquetUpdated = false;

        for (S3EventNotification.S3EventNotificationRecord record : s3Event.getRecords()) {
            String s3Key = record.getS3().getObject().getKey();
            String s3Bucket = record.getS3().getBucket().getName();

            if (s3Key.startsWith("parquet/")) {
                isParquetUpdated = true;
            }
        }

        if (isParquetUpdated) {
            try {
                generateTrainingJobData(
                    getConfig(ATHENA_REGION_ID, ATHENA_REGION_ID_DEF),
                    getConfig(ATHENA_DATABASE, ATHENA_DATABASE_DEF),
                    Integer.parseInt(getConfig(CLIENT_EXECUTION_TIMEOUT, CLIENT_EXECUTION_TIMEOUT_DEF)),
                    getConfig(ATHENA_OUTPUT_LOCATION, ATHENA_OUTPUT_LOCATION_DEF),
                    getConfig(ATHENA_QUERY, ATHENA_QUERY_DEF),
                    Long.parseLong(getConfig(SLEEP_AMOUNT_IN_MS, SLEEP_AMOUNT_IN_MS_DEF)),
                    getConfig(S3_BUCKET, S3_BUCKET_DEF),
                    getConfig(S3_KEY, S3_KEY_DEF),
                    Integer.parseInt(getConfig(TRAINING_FACTOR, TRAINING_FACTOR_DEF)),
                    Integer.parseInt(getConfig(VERIFICATION_FACTOR, VERIFICATION_FACTOR_DEF))
                );
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return s3Event;
    }

    private void generateTrainingJobData(String regionId, String dbName,
                                         int executionTimeout, String outputLocation,
                                         String athenaQuery, long sleepTime,
                                         String s3bucket, String s3key,
                                         int trainingFactor, int verificationFactor) throws Exception {

        ClientConfiguration configuration = new ClientConfiguration()
                .withClientExecutionTimeout(executionTimeout);

        AmazonAthena client = AmazonAthenaClientBuilder.standard()
                .withRegion(regionId)
                .withCredentials(InstanceProfileCredentialsProvider.getInstance())
                .withClientConfiguration(configuration)
                .build();

        int gcd = gcd(trainingFactor, verificationFactor);
        CsvRecordProcessor recordProcessor
                = new CsvRecordProcessor(s3bucket, s3key,
                trainingFactor / gcd, verificationFactor / gcd);
        String queryExecutionId
                = AthenaUtils.submitAthenaQuery(client, dbName, outputLocation, athenaQuery);

        AthenaUtils.waitForQueryToComplete(client, queryExecutionId, sleepTime);
        AthenaUtils.processResultRows(client, queryExecutionId, recordProcessor);
    }

    private String getConfig(String key, String val) {
        return System.getenv().getOrDefault(key, val);
    }

    private static int gcd(int a, int b) {
        if (b == 0)
            return a;
        return gcd(b,a%b);
    }
}
