package com.provectus.fds.ml;

import com.amazonaws.services.athena.model.Row;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

public class CsvRecordProcessor implements RecordProcessor {

    private String s3bucket = "newfdsb";
    private String s3key = "athena/";

    private int trainingFactor = 9;
    private int verificationFactor = 1;

    private PrintWriter trainStream;
    private PrintWriter verifyStream;

    private int handledTrainingRecords = 0;
    private int handledVerificationRecords = 0;

    private Path trainPath;
    private Path verifyPath;

    // with default values
    public CsvRecordProcessor() {
    }

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

    @Override
    public void process(Row row) {

        if (handledTrainingRecords == trainingFactor && handledVerificationRecords == verificationFactor) {
            handledTrainingRecords = handledVerificationRecords = 0;
        }

        StringBuffer buffer = new StringBuffer();
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
        } else if (handledVerificationRecords < verificationFactor) {
            verifyStream.println(buffer.toString());
            handledVerificationRecords++;
        } else {
            throw new RuntimeException("Collision appears while splitting data");
        }
    }

    @Override
    public void complete() throws Exception {
        // close streams before copy to s3
        trainStream.close();
        verifyStream.close();

        AmazonS3 s3clinet = AmazonS3ClientBuilder.defaultClient();

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("text/csv");

        copyToS3(s3clinet, metadata, trainPath);
        copyToS3(s3clinet, metadata, verifyPath);
    }

    private void copyToS3(AmazonS3 s3clinet, ObjectMetadata metadata, Path path) throws IOException {

        metadata.setContentLength(getSize(path));

        try (InputStream is = Files.newInputStream(path)) {
            PutObjectRequest objectRequest
                    = new PutObjectRequest(s3bucket, s3key + path.getFileName(),
                    is, metadata);
            s3clinet.putObject(objectRequest);
        }
    }

    long getSize(Path startPath) throws IOException {
        BasicFileAttributes attributes = Files.readAttributes(startPath, BasicFileAttributes.class);
        return attributes.size();
    }
}
