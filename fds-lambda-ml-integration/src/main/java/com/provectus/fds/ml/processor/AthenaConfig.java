package com.provectus.fds.ml.processor;

import com.amazonaws.services.athena.AmazonAthena;

public class AthenaConfig {

    private AmazonAthena client;
    private String dbName;
    private String outputLocation;
    private String query;
    private long sleepTime;
    private RecordProcessor recordProcessor;

    public AmazonAthena getClient() {
        return client;
    }

    public void setClient(AmazonAthena client) {
        this.client = client;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getOutputLocation() {
        return outputLocation;
    }

    public void setOutputLocation(String outputLocation) {
        this.outputLocation = outputLocation;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public long getSleepTime() {
        return sleepTime;
    }

    public void setSleepTime(long sleepTime) {
        this.sleepTime = sleepTime;
    }

    public RecordProcessor getRecordProcessor() {
        return recordProcessor;
    }

    public void setRecordProcessor(RecordProcessor recordProcessor) {
        this.recordProcessor = recordProcessor;
    }
}
