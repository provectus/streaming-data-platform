package com.provectus.fds.flink;

import com.provectus.fds.flink.config.StreamingProperties;
import com.provectus.fds.flink.schemas.AggregationSchema;
import com.provectus.fds.flink.schemas.BcnSchema;
import com.provectus.fds.models.bcns.Bcn;
import com.provectus.fds.models.events.Aggregation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.util.Properties;

public class StreamingApp {
    private static StreamingProperties properties = StreamingProperties.getInstance();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        StreamingJob job = new StreamingJob(getInputStream(environment), getSink(), properties);

        environment.execute();
    }

    private static DataStream<Bcn> getInputStream(StreamExecutionEnvironment environment) {
        Properties config = new Properties();
        config.setProperty(ConsumerConfigConstants.AWS_REGION, properties.getSourceAwsRegion());
        config.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, properties.getSourceStreamInitPos());

        return environment.addSource(new FlinkKinesisConsumer<>(properties.getSourceStreamName(), new BcnSchema(), config));
    }

    private static FlinkKinesisProducer<Aggregation> getSink() {
        Properties config = new Properties();
        config.setProperty(AWSConfigConstants.AWS_REGION, properties.getSinkAwsRegion());
        config.setProperty("AggregationEnabled", "false");

        FlinkKinesisProducer<Aggregation> sink = new FlinkKinesisProducer<>(new AggregationSchema(), config);
        sink.setDefaultStream(properties.getSinkStreamName());
        sink.setDefaultPartition("0");

        return sink;
    }
}
