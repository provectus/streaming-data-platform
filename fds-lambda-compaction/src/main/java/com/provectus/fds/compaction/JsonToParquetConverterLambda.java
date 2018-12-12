package com.provectus.fds.compaction;

import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.spi.JsonUtil;
import org.kitesdk.data.spi.filesystem.JSONFileReader;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;


public class JsonToParquetConverterLambda implements Function<S3Event, S3Event> {
    private AmazonS3 amazonS3;

    public String convertPath ( String source) {
        /*
            Convert source path of json file to path for parquet in next format:
                parquet/bcns/year=2018/day=17891/
         */

        return source;
    }

    @Override
    public S3Event apply(S3Event s3Event) {
        for (S3EventNotification.S3EventNotificationRecord record : s3Event.getRecords()) {
            String s3Key = record.getS3().getObject().getKey();
            String s3Bucket = record.getS3().getBucket().getName();
            S3Object object = amazonS3.getObject(new GetObjectRequest(s3Bucket, s3Key));
            try {
                byte[] bytes = object.getObjectContent().readNBytes((int) object.getObjectMetadata().getContentLength());
                GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(bytes));
                Schema jsonSchema = JsonUtil.inferSchema(gzipInputStream, "Record", 1);
                try (JSONFileReader<Record> reader = new JSONFileReader<>(
                        gzipInputStream, jsonSchema, Record.class)) {

                    reader.initialize();

                    org.apache.hadoop.fs.Path path = new Path(convertPath(s3Key).concat(""));
                    ParquetWriter<Record> writer = new AvroParquetWriter<>(
                            path,
                            jsonSchema,
                            CompressionCodecName.SNAPPY,
                            ParquetWriter.DEFAULT_BLOCK_SIZE,
                            ParquetWriter.DEFAULT_PAGE_SIZE
                    );
                    for (Record r : reader)
                        writer.write(r);
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return s3Event;
    }
}
