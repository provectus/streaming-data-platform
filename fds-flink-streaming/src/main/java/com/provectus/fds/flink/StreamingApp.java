package com.provectus.fds.flink;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.provectus.fds.flink.schemas.AggregationSchema;
import com.provectus.fds.flink.schemas.BcnSchema;
import com.provectus.fds.models.bcns.Bcn;
import com.provectus.fds.models.events.Aggregation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;

import java.io.IOException;
import java.util.Properties;

public class StreamingApp {
    private static final String SOURCE_CONFIG_PROPERTIES = "SourceConfigProperties";
    private static final String SINK_CONFIG_PROPERTIES = "SinkConfigProperties";

    private static final String INPUT_STREAM_NAME = "input.stream.name";
    private static final String OUTPUT_STREAM_NAME = "output.stream.name";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamingJob job = new StreamingJob(getInputStream(environment), getSink());

        environment.execute();
    }

    private static DataStream<Bcn> getInputStream(StreamExecutionEnvironment environment) {
        Properties sourceProperties = getProperties(SOURCE_CONFIG_PROPERTIES);

        return environment.addSource(new FlinkKinesisConsumer<>(
                sourceProperties.getProperty(INPUT_STREAM_NAME), new BcnSchema(), sourceProperties));
    }

    private static FlinkKinesisProducer<Aggregation> getSink() {
        Properties sinkProperties = getProperties(SINK_CONFIG_PROPERTIES);

        FlinkKinesisProducer<Aggregation> sink = new FlinkKinesisProducer<>(new AggregationSchema(), sinkProperties);
        sink.setDefaultStream(sinkProperties.getProperty(OUTPUT_STREAM_NAME));
        sink.setDefaultPartition("0");

        return sink;
    }

    private static Properties getProperties(String name) {
        Properties properties;

        try {
            properties = KinesisAnalyticsRuntime.getApplicationProperties().get(name);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Can't get '%s'", name), e);
        }

        if (properties == null) {
            throw new IllegalStateException(String.format("Properties '%s' are absent", name));
        }

        return properties;
    }
}
