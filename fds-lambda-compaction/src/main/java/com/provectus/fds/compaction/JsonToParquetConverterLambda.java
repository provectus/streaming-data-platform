package com.provectus.fds.compaction;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.provectus.fds.compaction.utils.ParquetUtils;
import com.provectus.fds.compaction.utils.PathFormatter;
import com.provectus.fds.compaction.utils.S3Utils;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.UUID;


public class JsonToParquetConverterLambda implements RequestHandler<S3Event, S3Event> {



    public static final String S3_PARQUET_PREFIX = "parquet";
    public static final String MERGED_PARQUET_FILE_NAME = "data.parquet";

    private final ParquetUtils parquetUtils = new ParquetUtils();
    private final File tmpDir = Files.createTempDirectory("s3").toFile();


    public JsonToParquetConverterLambda() throws IOException {
    }

    @Override
    public S3Event handleRequest(S3Event s3Event,Context context) {
        context.getLogger().log("Received: "+s3Event.toString());
        AmazonS3 s3clinet = AmazonS3ClientBuilder.defaultClient();

        for (S3EventNotification.S3EventNotificationRecord record : s3Event.getRecords()) {
            String s3Key = record.getS3().getObject().getKey();
            String s3Bucket = record.getS3().getBucket().getName();

            if (s3Key.startsWith("/raw/")) {

                S3Object jsonObject = s3clinet.getObject(new GetObjectRequest(s3Bucket, s3Key));

                File s3jsonFile = null;
                File dataParquetFile = null;
                File tmpParquetFile = null;
                File targetParquetFile = null;

                try {

                    s3jsonFile = S3Utils.downloadFile(jsonObject);
                    tmpParquetFile = parquetUtils.convert(tmpDir,s3jsonFile, s3Bucket);
                    targetParquetFile = new File(tmpDir, UUID.randomUUID().toString());

                    PathFormatter formatter = PathFormatter.fromS3Path(s3Key);

                    String parquetPath = formatter.pathWithFile(S3_PARQUET_PREFIX, MERGED_PARQUET_FILE_NAME);

                    if (s3clinet.doesObjectExist(s3Bucket, parquetPath)) {

                        S3Object parquetObject = s3clinet.getObject(new GetObjectRequest(s3Bucket, parquetPath));
                        dataParquetFile = S3Utils.downloadFile(parquetObject);

                        parquetUtils.mergeFiles(
                                ParquetWriter.DEFAULT_BLOCK_SIZE,
                                Arrays.asList(
                                        new Path("file://" + tmpParquetFile.getAbsolutePath()),
                                        new Path("file://" + dataParquetFile.getAbsolutePath())
                                ),
                                new Path("file://" + targetParquetFile.getAbsolutePath())
                        );

                    } else {
                        targetParquetFile = tmpParquetFile;
                    }

                    PutObjectResult result = s3clinet.putObject(
                            new PutObjectRequest(
                                    s3Bucket,
                                    parquetPath,
                                    targetParquetFile
                            )
                    );
                    context.getLogger().log("Parquet uploaded: "+targetParquetFile.toString());

                } catch (IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    if (s3jsonFile != null) s3jsonFile.delete();
                    if (dataParquetFile != null) dataParquetFile.delete();
                    if (tmpParquetFile !=null) tmpParquetFile.delete();
                    if (targetParquetFile!=null) targetParquetFile.delete();
                }
            }
        }
        return s3Event;
    }
}
