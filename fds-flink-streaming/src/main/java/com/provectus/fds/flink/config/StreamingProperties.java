package com.provectus.fds.flink.config;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.IOException;
import java.util.Properties;

public class StreamingProperties {
    private static final String SOURCE_CONFIG_PROPERTIES = "SourceConfigProperties";
    private static final String SINK_CONFIG_PROPERTIES = "SinkConfigProperties";
    private static final String AGGREGATION_PROPERTIES = "AggregationProperties";

    private static final String SOURCE_STREAM_INIT_POS = "source.stream.init.pos";
    private static final String SOURCE_AWS_REGION = "source.aws.region";
    private static final String SOURCE_BCN_STREAM_NAME = "source.bcn.stream.name";
    private static final String SOURCE_LOCATION_STREAM_NAME = "source.location.stream.name";


    private static final String SINK_AGGREGATE_STREAM_NAME = "sink.aggregate.stream.name";
    private static final String SINK_BID_STREAM_NAME = "sink.bid.stream.name";
    private static final String SINK_IMPRESSION_STREAM_NAME = "sink.impression.stream.name";
    private static final String SINK_CLICK_STREAM_NAME = "sink.click.stream.name";
    private static final String SINK_WALKIN_STREAM_NAME = "sink.walkin.stream.name";
    private static final String SINK_WALKIN_CLICK_STREAM_NAME = "sink.walkin.click.stream.name";
    private static final String SINK_AWS_REGION = "sink.aws.region";

    private static final String AGGREGATION_BIDS_SESSION_TIMEOUT = "aggregation.bids.session.timeout";
    private static final String AGGREGATION_CLICKS_SESSION_TIMEOUT = "aggregation.clicks.session.timeout";
    private static final String AGGREGATION_LOCATIONS_SESSION_TIMEOUT = "aggregation.locations.session.timeout";
    private static final String AGGREGATION_PERIOD = "aggregation.period";

    private String sourceStreamInitPos;
    private String sourceBcnStreamName;
    private String sourceLocationStreamName;
    private String sourceAwsRegion;

    private String sinkAggregateStreamName;
    private String sinkBidStreamName;
    private String sinkImpressionStreamName;
    private String sinkClickStreamName;
    private String sinkWalkinStreamName;
    private String sinkWalkinClickStreamName;
    private String sinkAwsRegion;

    private Time bidsSessionTimeout;
    private Time clicksSessionTimeout;
    private Time locationsSessionTimeout;
    private Time aggregationPeriod;

    private StreamingProperties(Properties sourceProperties, Properties sinkProperties, Properties aggregationProperties) {
        sourceStreamInitPos = (String) sourceProperties.get(SOURCE_STREAM_INIT_POS);
        sourceAwsRegion = (String) sourceProperties.get(SOURCE_AWS_REGION);
        sourceBcnStreamName = (String) sourceProperties.get(SOURCE_BCN_STREAM_NAME);
        sourceLocationStreamName = (String) sourceProperties.get(SOURCE_LOCATION_STREAM_NAME);

        sinkAggregateStreamName = (String) sinkProperties.get(SINK_AGGREGATE_STREAM_NAME);
        sinkBidStreamName = (String) sinkProperties.get(SINK_BID_STREAM_NAME);
        sinkImpressionStreamName = (String) sinkProperties.get(SINK_IMPRESSION_STREAM_NAME);
        sinkClickStreamName = (String) sinkProperties.get(SINK_CLICK_STREAM_NAME);
        sinkWalkinStreamName = (String) sinkProperties.get(SINK_WALKIN_STREAM_NAME);
        sinkWalkinClickStreamName = (String) sinkProperties.get(SINK_WALKIN_CLICK_STREAM_NAME);
        sinkAwsRegion = (String) sinkProperties.get(SINK_AWS_REGION);

        bidsSessionTimeout = Time.minutes(Long.parseLong(aggregationProperties.get(AGGREGATION_BIDS_SESSION_TIMEOUT).toString()));
        clicksSessionTimeout = Time.minutes(Long.parseLong(aggregationProperties.get(AGGREGATION_CLICKS_SESSION_TIMEOUT).toString()));
        locationsSessionTimeout = Time.minutes(Long.parseLong(aggregationProperties.get(AGGREGATION_LOCATIONS_SESSION_TIMEOUT).toString()));
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

    public String getSourceBcnStreamName() {
        return sourceBcnStreamName;
    }

    public String getSourceLocationStreamName() {
        return sourceLocationStreamName;
    }

    public String getSourceStreamInitPos() {
        return sourceStreamInitPos;
    }

    public String getSourceAwsRegion() {
        return sourceAwsRegion;
    }

    public String getSinkAggregateStreamName() {
        return sinkAggregateStreamName;
    }

    public String getSinkBidStreamName() {
        return sinkBidStreamName;
    }

    public String getSinkImpressionStreamName() {
        return sinkImpressionStreamName;
    }

    public String getSinkClickStreamName() {
        return sinkClickStreamName;
    }

    public String getSinkWalkinStreamName() {
        return sinkWalkinStreamName;
    }

    public String getSinkWalkinClickStreamName() {
        return sinkWalkinClickStreamName;
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

    public Time getLocationsSessionTimeout() {
        return locationsSessionTimeout;
    }

    public Time getAggregationPeriod() {
        return aggregationPeriod;
    }
}