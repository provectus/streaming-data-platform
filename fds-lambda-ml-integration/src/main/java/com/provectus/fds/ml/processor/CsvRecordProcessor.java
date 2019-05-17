package com.provectus.fds.ml.processor;

import com.amazonaws.services.athena.model.Row;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.provectus.fds.ml.JobRunner;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;

public class CsvRecordProcessor implements RecordProcessor {

    private String s3bucket;
    private String s3key;

    private int trainingFactor;
    private int verificationFactor;

    private PrintWriter trainStream;
    private PrintWriter verifyStream;

    private int handledTrainingRecords = 0;
    private int handledVerificationRecords = 0;

    private Path trainPath;
    private Path verifyPath;

    private int totalTrainRecordsProcessed = 0;
    private int totalVerifyRecordsProcessed = 0;

    private Map<String, String> statistic = new HashMap<>();

    public static final String TOTAL_TRAIN_RECORDS_PROCESSED = "total_train_records_processed";
    public static final String TOTAL_VERIFY_RECORDS_PROCESSED = "total_verify_records_processed";

    public CsvRecordProcessor(String s3bucket, String s3key, int trainingFactor, int verificationFactor) {
        this.s3bucket = s3bucket;
        this.s3key = s3key;
        this.trainingFactor = trainingFactor;
        this.verificationFactor = verificationFactor;
    }

    @Override
    public void initialize() throws Exception {
        trainPath = Files.createTempFile("train_", ".csv");
        trainStream = new PrintWriter(Files.newOutputStream(trainPath));

        verifyPath = Files.createTempFile("verify_", ".csv");
        verifyStream = new PrintWriter(Files.newOutputStream(verifyPath));
    }


    public static class CvsRecordProcessorException extends RuntimeException {
        CvsRecordProcessorException(String message) {
            super(message);
        }
    }

    @Override
    public void process(Row row) {

        if (handledTrainingRecords == trainingFactor && handledVerificationRecords == verificationFactor) {
            handledTrainingRecords = handledVerificationRecords = 0;
        }

        StringBuilder buffer = new StringBuilder();
        buffer
                .append("\"").append(row.getData().get(0).getVarCharValue()).append("\",")
                .append("\"").append(row.getData().get(1).getVarCharValue()).append("\",")
                .append("\"").append(row.getData().get(2).getVarCharValue()).append("\",")
                .append("\"").append(row.getData().get(3).getVarCharValue()).append("\",")
                .append("\"").append(row.getData().get(4).getVarCharValue()).append("\",")
                .append("\"").append(row.getData().get(5).getVarCharValue()).append("\"");


        if (handledTrainingRecords < trainingFactor) {

            trainStream.println(buffer.toString());

            handledTrainingRecords++;
            totalTrainRecordsProcessed++;
        } else if (handledVerificationRecords < verificationFactor) {

            verifyStream.println(buffer.toString());

            handledVerificationRecords++;
            totalVerifyRecordsProcessed++;
        } else {
            throw new CvsRecordProcessorException("Collision appears while splitting data");
        }
    }

    @Override
    public void close() throws Exception {
        // close streams before copy to s3
        trainStream.close();
        verifyStream.close();

        AmazonS3 s3clinet = AmazonS3ClientBuilder.defaultClient();

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("text/csv");

        copyToS3(s3clinet, metadata, trainPath, JobRunner.TRAIN);
        copyToS3(s3clinet, metadata, verifyPath, JobRunner.VALIDATION);
    }

    @Override
    public Map<String, String> getStatistic() {

        statistic.put(TOTAL_TRAIN_RECORDS_PROCESSED, Integer.toString(totalTrainRecordsProcessed));
        statistic.put(TOTAL_VERIFY_RECORDS_PROCESSED, Integer.toString(totalVerifyRecordsProcessed));

        return statistic;
    }

    private void copyToS3(AmazonS3 s3clinet, ObjectMetadata metadata, Path path, String name) throws IOException {

        metadata.setContentLength(getSize(path));

        try (InputStream is = Files.newInputStream(path)) {
            PutObjectRequest objectRequest
                    = new PutObjectRequest(s3bucket, s3key + path.getFileName(),
                    is, metadata);
            s3clinet.putObject(objectRequest);

            statistic.put(name + "_bucket", s3bucket);
            statistic.put(name + "_key", s3key + path.getFileName());
            statistic.put(name, "s3://" + s3bucket + "/" + s3key + path.getFileName());
        }
    }

    private long getSize(Path startPath) throws IOException {
        BasicFileAttributes attributes = Files.readAttributes(startPath, BasicFileAttributes.class);
        return attributes.size();
    }
}
