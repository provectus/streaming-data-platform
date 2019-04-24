package com.provectus.fds.ml;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;

/**
 * This sample app allows you execute an integration check...
 */
public class SampleApp {
    public static void main(String[] args) throws Exception {

        AmazonAthena client = AmazonAthenaClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                //.withCredentials(InstanceProfileCredentialsProvider.getInstance())
                .withClientConfiguration(new ClientConfiguration().withClientExecutionTimeout(SampleAppConstants.CLIENT_EXECUTION_TIMEOUT))
                .build();

        CsvRecordProcessor recordProcessor = new CsvRecordProcessor();
        String queryExecutionId
                = AthenaUtils.submitAthenaQuery(
                        client,
                        SampleAppConstants.ATHENA_DEFAULT_DATABASE,
                        SampleAppConstants.ATHENA_OUTPUT_LOCATION,
                        SampleAppConstants.ATHENA_QUERY
                );

        AthenaUtils.waitForQueryToComplete(client, queryExecutionId, SampleAppConstants.SLEEP_AMOUNT_IN_MS);
        AthenaUtils.processResultRows(client, queryExecutionId, recordProcessor);
    }
}
