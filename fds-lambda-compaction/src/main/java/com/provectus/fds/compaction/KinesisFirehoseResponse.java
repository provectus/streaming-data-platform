package com.provectus.fds.compaction;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

public class KinesisFirehoseResponse {
    /// The record was transformed successfully.
    public final static String TRANSFORMED_STATE_OK = "Ok";

    /// The record was dropped intentionally by your processing logic.
    public final static String TRANSFORMED_STATE_DROPPED = "Dropped";

    /// The record could not be transformed.
    public final static String TRANSFORMED_STATE_PROCESSINGFAILED = "ProcessingFailed";

    private List<FirehoseRecord> records = new ArrayList<>();

    public List<FirehoseRecord> getRecords() {
        return records;
    }

    public void setRecords(List<FirehoseRecord> records) {
        this.records = records;
    }

    public KinesisFirehoseResponse(List<FirehoseRecord> records) {
        this.records = records;
    }

    public KinesisFirehoseResponse append(FirehoseRecord record) {
        this.records.add(record);
        return this;
    }

    public int size() {
        return records.size();
    }


    public static class FirehoseRecord {
        private final String recordId;
        private final String Result;
        private final String data;

        public FirehoseRecord(String recordId, String data) {
            this(recordId, TRANSFORMED_STATE_OK, data);
        }

        public FirehoseRecord(String recordId, String result, String data) {
            this.recordId = recordId;
            Result = result;
            this.data = data;
        }

        public static FirehoseRecord appendNewLine(String recordId, ByteBuffer data) {
            byte[] result = Arrays.copyOf(data.array(), data.array().length+1);
            result[data.array().length] = '\n';
            return new FirehoseRecord(
                    recordId,
                    Base64.getEncoder().encodeToString(result)
            );
        }

        public String getRecordId() {
            return recordId;
        }

        public String getResult() {
            return Result;
        }

        public String getData() {
            return data;
        }
    }

}
