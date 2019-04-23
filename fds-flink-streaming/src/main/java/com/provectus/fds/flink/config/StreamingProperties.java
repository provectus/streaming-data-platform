package com.provectus.fds.flink.config;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import lombok.Getter;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.IOException;
import java.util.Properties;

public class StreamingProperties {
    private static final String SOURCE_CONFIG_PROPERTIES = "SourceConfigProperties";
    private static final String SINK_CONFIG_PROPERTIES = "SinkConfigProperties";
    private static final String AGGREGATION_PROPERTIES = "AggregationProperties";

    private static final String SOURCE_STREAM_NAME = "source.stream.name";
    private static final String SOURCE_STREAM_INIT_POS = "source.stream.init.pos";
    private static final String SOURCE_AWS_REGION = "source.aws.region";

    private static final String SINK_STREAM_NAME = "sink.stream.name";
    private static final String SINK_AWS_REGION = "sink.aws.region";

    private static final String AGGREGATION_BIDS_SESSION_TIMEOUT = "aggregation.bids.session.timeout";
    private static final String AGGREGATION_CLICKS_SESSION_TIMEOUT = "aggregation.clicks.session.timeout";
    private static final String AGGREGATION_PERIOD = "aggregation.period";

    private String sourceStreamName;
    private String sourceStreamInitPos;
    private String sourceAwsRegion;

    private String sinkStreamName;
    private String sinkAwsRegion;

    private Time bidsSessionTimeout;
    private Time clicksSessionTimeout;
    private Time aggregationPeriod;

    private StreamingProperties(Properties sourceProperties, Properties sinkProperties, Properties aggregationProperties) {
        sourceStreamName = (String) sourceProperties.get(SOURCE_STREAM_NAME);
        sourceStreamInitPos = (String) sourceProperties.get(SOURCE_STREAM_INIT_POS);
        sourceAwsRegion = (String) sourceProperties.get(SOURCE_AWS_REGION);

        sinkStreamName = (String) sinkProperties.get(SINK_STREAM_NAME);
        sinkAwsRegion = (String) sinkProperties.get(SINK_AWS_REGION);

        bidsSessionTimeout = Time.minutes(Long.parseLong(aggregationProperties.get(AGGREGATION_BIDS_SESSION_TIMEOUT).toString()));
        clicksSessionTimeout = Time.minutes(Long.parseLong(aggregationProperties.get(AGGREGATION_CLICKS_SESSION_TIMEOUT).toString()));
        aggregationPeriod = Time.minutes(Long.parseLong(aggregationProperties.get(AGGREGATION_PERIOD).toString()));
    }

    public static StreamingProperties fromRuntime() {
        Properties sourceProperties = getProperties(SOURCE_CONFIG_PROPERTIES);
        Properties sinkProperties = getProperties(SINK_CONFIG_PROPERTIES);
        Properties aggregationProperties = getProperties(AGGREGATION_PROPERTIES);

        return new StreamingProperties(sourceProperties, sinkProperties, aggregationProperties);
    }

    public static StreamingProperties fromProperties(
            Properties sourceProperties,
            Properties sinkProperties,
            Properties aggregationProperties) {
        return new StreamingProperties(sourceProperties, sinkProperties, aggregationProperties);
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

    public String getSourceStreamName() {
        return sourceStreamName;
    }

    public String getSourceStreamInitPos() {
        return sourceStreamInitPos;
    }

    public String getSourceAwsRegion() {
        return sourceAwsRegion;
    }

    public String getSinkStreamName() {
        return sinkStreamName;
    }

    public String getSinkAwsRegion() {
        return sinkAwsRegion;
    }

    public Time getBidsSessionTimeout() {
        return bidsSessionTimeout;
    }

    public Time getClicksSessionTimeout() {
        return clicksSessionTimeout;
    }

    public Time getAggregationPeriod() {
        return aggregationPeriod;
    }
}