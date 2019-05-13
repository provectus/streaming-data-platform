package com.provectus.fds.compaction;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.provectus.fds.compaction.utils.ParquetUtils;
import com.provectus.fds.compaction.utils.PathFormatter;
import com.provectus.fds.compaction.utils.S3Utils;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.FileMetaData;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;


public class JsonToParquetConverterLambda implements RequestHandler<KinesisEvent, List<S3Event>> {
    private final static String GLUE_CATALOG_ID_KEY = "GLUE_CATALOG_ID";
    public static final String GLUE_CATALOG_ID_KEY_DEFAULT = "";

    private final static String GLUE_DATABASE_KEY = "GLUE_DATABASE_NAME";
    public static final String GLUE_DATABASE_KEY_DEFAULT = "fds";


    public static final String S3_PARQUET_PREFIX = "parquet";
    public static final String MERGED_PARQUET_FILE_NAME = "data.parquet";

    private final ParquetUtils parquetUtils = new ParquetUtils();
    private final File tmpDir = Files.createTempDirectory("s3").toFile();
    private final GlueDAO glueDAO = new GlueDAO(System.getenv().getOrDefault(GLUE_CATALOG_ID_KEY, GLUE_CATALOG_ID_KEY_DEFAULT));
    private final String dataBaseName = System.getenv().getOrDefault(GLUE_DATABASE_KEY, GLUE_DATABASE_KEY_DEFAULT);


    public JsonToParquetConverterLambda() throws IOException {
    }

    @Override
    public List<S3Event> handleRequest(KinesisEvent input, Context context) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        context.getLogger().log(String.format("Processing Kinesis input: %s", input));

        List<S3Event> results = new ArrayList<>();

        for (KinesisEvent.KinesisEventRecord r : input.getRecords()) {
            try {
                S3Event s3Event = mapper.readerFor(S3Event.class)
                        .readValue(r.getKinesis().getData().array());
                results.add(handleRequest(s3Event, context));
            } catch (IOException e) {
                context.getLogger().log(e.getMessage());
            }
        }
        return results;
    }


    public S3Event handleRequest(S3Event s3Event,Context context) {
        context.getLogger().log("Received: "+s3Event.toString());
        AmazonS3 s3clinet = AmazonS3ClientBuilder.defaultClient();

        for (S3EventNotification.S3EventNotificationRecord record : s3Event.getRecords()) {
            String s3Key = record.getS3().getObject().getKey();
            String s3Bucket = record.getS3().getBucket().getName();

            if (s3Key.startsWith("raw/")) {

                S3Object jsonObject = s3clinet.getObject(new GetObjectRequest(s3Bucket, s3Key));

                File s3jsonFile = null;
                File dataParquetFile = null;
                File tmpParquetFile = null;
                File targetParquetFile = null;

                try {
                    PathFormatter formatter = PathFormatter.fromS3Path(s3Key);
                    s3jsonFile = S3Utils.downloadFile(jsonObject, formatter.getFilename());
                    tmpParquetFile = parquetUtils.convert(tmpDir,s3jsonFile, s3Bucket);

                    FileMetaData metaData = parquetUtils.fileMetaData(tmpParquetFile);

                    targetParquetFile = new File(tmpDir, UUID.randomUUID().toString());

                    String parquetPath = formatter.pathWithFile(S3_PARQUET_PREFIX, MERGED_PARQUET_FILE_NAME);

                    if (s3clinet.doesObjectExist(s3Bucket, parquetPath)) {

                        S3Object parquetObject = s3clinet.getObject(new GetObjectRequest(s3Bucket, parquetPath));
                        dataParquetFile = S3Utils.downloadFile(parquetObject, MERGED_PARQUET_FILE_NAME);

                        metaData = parquetUtils.mergeFiles(
                                ParquetWriter.DEFAULT_BLOCK_SIZE,
                                Arrays.asList(
                                        new Path("file://" + tmpParquetFile.getAbsolutePath()),
                                        new Path("file://" + dataParquetFile.getAbsolutePath())
                                ),
                                new Path("file://" + targetParquetFile.getAbsolutePath())
                        );
                        context.getLogger().log(String.format("Parquet %s has been merged", targetParquetFile.toString() ));

                    } else {
                        targetParquetFile = tmpParquetFile;
                        context.getLogger().log(String.format("Parquet %s has not been merged", targetParquetFile.toString() ));
                    }

                    PutObjectResult result = s3clinet.putObject(
                            new PutObjectRequest(
                                    s3Bucket,
                                    parquetPath,
                                    targetParquetFile
                            )
                    );

                    glueDAO.addPartition(
                            dataBaseName,
                            String.format("parquet_%s", formatter.getMsgtype()),
                            formatter.getPeriod().toList(),
                            metaData
                    );

                    context.getLogger().log(String.format("Parquet uploaded: %s (%s)", targetParquetFile.toString(), result.getMetadata().getContentDisposition()));

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
