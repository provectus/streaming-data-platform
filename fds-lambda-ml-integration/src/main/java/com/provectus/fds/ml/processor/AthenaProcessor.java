package com.provectus.fds.ml.processor;

import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.model.*;

import java.util.List;

public class AthenaProcessor {

    public void process(AthenaConfig config) throws Exception {
        String queryExecutionId
                = submitAthenaQuery(config.getClient(),
                config.getDbName(),
                config.getOutputLocation(),
                config.getQuery()
        );

        waitForQueryToComplete(config.getClient(), queryExecutionId, config.getSleepTime());
        processResultRows(config.getClient(), queryExecutionId, config.getRecordProcessor());
    }

    /**
     * Submits a query to Athena and returns the execution ID of the query.
     */
    protected String submitAthenaQuery(AmazonAthena client, String defaultDatabase,
                                           String outputLocation, String athenaQuery) {

        QueryExecutionContext queryExecutionContext
                = new QueryExecutionContext().withDatabase(defaultDatabase);

        ResultConfiguration resultConfiguration = new ResultConfiguration()
                .withOutputLocation(outputLocation);

        StartQueryExecutionRequest startQueryExecutionRequest = new StartQueryExecutionRequest()
                .withQueryString(athenaQuery)
                .withQueryExecutionContext(queryExecutionContext)
                .withResultConfiguration(resultConfiguration);

        StartQueryExecutionResult startQueryExecutionResult = client.startQueryExecution(startQueryExecutionRequest);
        return startQueryExecutionResult.getQueryExecutionId();
    }

    public static class AthenaProcessorException extends RuntimeException {
        AthenaProcessorException(String message) {
            super(message);
        }
    }

    /**
     * Wait for an Athena query to complete, fail or to be cancelled. This is done by polling Athena over an
     * interval of time. If a query fails or is cancelled, then it will throw an exception.
     */
    protected void waitForQueryToComplete(AmazonAthena client, String queryExecutionId, long sleepTime) throws InterruptedException {
        GetQueryExecutionRequest getQueryExecutionRequest = new GetQueryExecutionRequest()
                .withQueryExecutionId(queryExecutionId);

        GetQueryExecutionResult getQueryExecutionResult;
        boolean isQueryStillRunning = true;
        while (isQueryStillRunning) {
            getQueryExecutionResult = client.getQueryExecution(getQueryExecutionRequest);
            String queryState = getQueryExecutionResult.getQueryExecution().getStatus().getState();
            if (queryState.equals(QueryExecutionState.FAILED.toString())) {
                throw new AthenaProcessorException("Query Failed to run with Error Message: " + getQueryExecutionResult.getQueryExecution().getStatus().getStateChangeReason());
            } else if (queryState.equals(QueryExecutionState.CANCELLED.toString())) {
                throw new AthenaProcessorException("Query was cancelled.");
            } else if (queryState.equals(QueryExecutionState.SUCCEEDED.toString())) {
                isQueryStillRunning = false;
            } else {
                // Sleep an amount of time before retrying again.
                Thread.sleep(sleepTime);
            }
        }
    }

    /**
     * This code calls Athena and retrieves the results of a query.
     * The query must be in a completed state before the results can be retrieved and
     * paginated.
     */
    protected void processResultRows(AmazonAthena client, String queryExecutionId, RecordProcessor recordProcessor) throws Exception {
        recordProcessor.initialize();

        GetQueryResultsRequest getQueryResultsRequest = new GetQueryResultsRequest()
                .withQueryExecutionId(queryExecutionId);

        GetQueryResultsResult getQueryResultsResult = client.getQueryResults(getQueryResultsRequest);

        boolean firstPage = true;
        while (true) {
            List<Row> results = getQueryResultsResult.getResultSet().getRows();

            for (int i = 0; i < results.size(); i++) {
                // Process the row. The first row of the first page holds the column names.
                // so skip it.
                if (i == 0 && firstPage) {
                    firstPage = false;
                } else {
                    recordProcessor.process(results.get(i));
                }
            }

            // If nextToken is null, there are no more pages to read. Break out of the loop.
            if (getQueryResultsResult.getNextToken() == null) {
                break;
            }
            getQueryResultsResult = client.getQueryResults(
                    getQueryResultsRequest.withNextToken(getQueryResultsResult.getNextToken()));
        }
    }
}
