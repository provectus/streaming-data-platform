package com.provectus.fds.flink;

import com.provectus.fds.flink.config.StreamingProperties;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingApp {
    private static final Logger log = LoggerFactory.getLogger(StreamingApp.class);

    private static StreamingProperties properties = StreamingProperties.fromRuntime();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        new StreamingJob(environment, properties);

        environment.execute("Streaming Data Platform");

        log.info("StreamingApp has started");
    }
}