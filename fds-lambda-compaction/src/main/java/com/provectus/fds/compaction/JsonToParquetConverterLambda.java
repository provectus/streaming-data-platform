package com.provectus.fds.compaction;

import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.spi.JsonUtil;
import org.kitesdk.data.spi.filesystem.JSONFileReader;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;


public class JsonToParquetConverterLambda implements Function<S3Event, S3Event> {
    private AmazonS3 amazonS3;

    public String convertPath(String source, String filename) {
        /*
            Convert source path of json file to path for parquet in next format:
                parquet/bcns/year=2018/day=17891/
         */
        String type, year, month, day, file;
        List<String> list = Arrays.asList(source.split("/"));
        type = list.get(2);
        year = list.get(3);
        month = list.get(4);
        day = list.get(5);
        if (filename.isEmpty()) {
            file = list.get(6).replace(".gz", ".parquet");
        } else {
            file = filename;
        }
        return String.format("/parquet/%s/year=%s/month=%s/day=%s/%s", type, year, month, day, file);
    }

    public String convertPath(String source) {
        return convertPath(source, "");
    }

    @Override
    public S3Event apply(S3Event s3Event) {
        for (S3EventNotification.S3EventNotificationRecord record : s3Event.getRecords()) {
            String s3Key = record.getS3().getObject().getKey();
            String s3Bucket = record.getS3().getBucket().getName();
            if (s3Key.contains(".parquet")) {
                return s3Event;
            }
            S3Object jsonObject = amazonS3.getObject(new GetObjectRequest(s3Bucket, s3Key));
            try {
                JsonNode rawJson = JsonUtil.parse(jsonObject.getObjectContent().toString());
                Schema jsonSchema = JsonUtil.inferSchema(rawJson, "myRecord");
                JSONFileReader<Record> reader = new JSONFileReader<>(jsonObject.getObjectContent(), jsonSchema, Record.class);
                reader.initialize();
                Path tmpParquet = new Path(String.format("s3a://%s/%s/", s3Bucket, convertPath(s3Key).concat("")));
                ParquetWriter<Record> writer = new AvroParquetWriter<>(
                        tmpParquet,
                        jsonSchema,
                        CompressionCodecName.SNAPPY,
                        ParquetWriter.DEFAULT_BLOCK_SIZE,
                        ParquetWriter.DEFAULT_PAGE_SIZE
                );
                for (Record r : reader) {
                    writer.write(r);
                }
                writer.close();
                S3Object parquetObject = amazonS3.getObject(new GetObjectRequest(s3Bucket, convertPath(s3Key, "data.parquet")));

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return s3Event;
    }
}
