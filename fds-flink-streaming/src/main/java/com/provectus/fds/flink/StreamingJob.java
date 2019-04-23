package com.provectus.fds.flink;

import com.provectus.fds.flink.aggregators.AggregationWindowProcessor;
import com.provectus.fds.flink.aggregators.MetricsAggregator;
import com.provectus.fds.flink.config.StreamingProperties;
import com.provectus.fds.flink.schemas.*;
import com.provectus.fds.flink.selectors.EventSelector;
import com.provectus.fds.models.bcns.*;
import com.provectus.fds.models.events.Aggregation;
import com.provectus.fds.models.events.Click;
import com.provectus.fds.models.events.Impression;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

class StreamingJob {
    private static final String BID_BCN_STREAM = "BidBcn stream";
    private static final String IMPRESSION_BCN_STREAM = "ImpressionBcn stream";
    private static final String CLICKS_BCN_STREAM = "ClickBcn stream";

    private static final String AGGREGATED_BIDS_STREAM = "Aggregated bids";
    private static final String AGGREGATED_IMPRESSIONS_STREAM = "Aggregated impressions";
    private static final String AGGREGATED_CLICKS_STREAM = "Aggregated clicks";

    private static final Logger log = LoggerFactory.getLogger(StreamingJob.class);

    private final StreamExecutionEnvironment environment;
    private final StreamingProperties properties;

    private final DataStream<Bcn> bcnStream;
    private final DataStream<BidBcn> bidBcnStream;
    private final DataStream<ImpressionBcn> impressionBcnStream;
    private final DataStream<ClickBcn> clickBcnStream;

    private final SinkFunction<Aggregation> aggregateSink;
    private final SinkFunction<BidBcn> bidSink;
    private final SinkFunction<ImpressionBcn> impressionSink;
    private final SinkFunction<ClickBcn> clickSink;

    StreamingJob(StreamExecutionEnvironment environment, StreamingProperties properties) {
        this.environment = environment;
        this.properties = properties;

        // Create sinks
        aggregateSink = getSink(properties.getSinkAggregateStreamName(), new AggregationSchema());
        bidSink = getSink(properties.getSinkBidStreamName(), new BidBcnSchema());
        impressionSink = getSink(properties.getSinkImpressionStreamName(), new ImpressionBcnSchema());
        clickSink = getSink(properties.getSinkClickStreamName(), new ClickBcnSchema());

        // Create streams
        bcnStream = getInputStream(properties.getSourceStreamName(), new BcnSchema());
        bidBcnStream = getInputStream(properties.getSinkBidStreamName(), new BidBcnSchema());
        impressionBcnStream = getInputStream(properties.getSinkImpressionStreamName(), new ImpressionBcnSchema());
        clickBcnStream = getInputStream(properties.getSinkClickStreamName(), new ClickBcnSchema());

        configure();

        log.info("Pipeline has configured");
    }

    private void configure() {
        splitInput();

        // Joining logic
        DataStream<Impression> impressionStream = createImpressionStream(bidBcnStream, impressionBcnStream,
                properties.getBidsSessionTimeout());
        DataStream<Click> clickStream = createClickStream(impressionStream, clickBcnStream,
                properties.getClicksSessionTimeout());

        // Aggregation streams
        Time period = properties.getAggregationPeriod();
        DataStream<Aggregation> bidsAggregation = aggregateBidsBcn(bidBcnStream, period);
        DataStream<Aggregation> clicksAggregation = aggregateClicks(clickStream, period);
        DataStream<Aggregation> impressionsAggregation = aggregateImpressions(impressionStream, period);

        // Union all aggregations and add sink
        bidsAggregation.union(clicksAggregation, impressionsAggregation)
                .addSink(aggregateSink)
                .name(String.format("Kinesis stream: %s", properties.getSinkAggregateStreamName()));
    }

    static DataStream<Impression> createImpressionStream(DataStream<BidBcn> bidBcnStream,
                                                         DataStream<ImpressionBcn> impressionBcnStream,
                                                         Time sessionTimeout) {
        return bidBcnStream
                .join(impressionBcnStream)
                .where(BidBcn::getPartitionKey)
                .equalTo(ImpressionBcn::getPartitionKey)
                .window(EventTimeSessionWindows.withGap(sessionTimeout))
                .apply(Impression::from);
    }

    static DataStream<Click> createClickStream(DataStream<Impression> impressionStream,
                                               DataStream<ClickBcn> clickBcnStream,
                                               Time sessionTimeout) {
        return impressionStream
                .join(clickBcnStream)
                .where(Impression::getPartitionKey)
                .equalTo(ClickBcn::getPartitionKey)
                .window(EventTimeSessionWindows.withGap(sessionTimeout))
                .apply(Click::from);
    }

    static DataStream<Aggregation> aggregateBidsBcn(DataStream<BidBcn> bidBcnStream, Time aggregationPeriod) {
        return bidBcnStream
                .keyBy(BidBcn::getCampaignItemId)
                .window(TumblingEventTimeWindows.of(aggregationPeriod))
                .aggregate(new MetricsAggregator<>(), new AggregationWindowProcessor())
                .name(AGGREGATED_BIDS_STREAM);
    }

    static DataStream<Aggregation> aggregateImpressions(DataStream<Impression> impressionStream, Time aggregationPeriod) {
        return impressionStream
                .keyBy(imp -> imp.getBidBcn().getCampaignItemId())
                .window(TumblingEventTimeWindows.of(aggregationPeriod))
                .aggregate(new MetricsAggregator<>(), new AggregationWindowProcessor())
                .name(AGGREGATED_IMPRESSIONS_STREAM);
    }

    static DataStream<Aggregation> aggregateClicks(DataStream<Click> clickStream, Time aggregationPeriod) {
        return clickStream
                .keyBy(click -> click.getImpression().getBidBcn().getCampaignItemId())
                .window(TumblingEventTimeWindows.of(aggregationPeriod))
                .aggregate(new MetricsAggregator<>(), new AggregationWindowProcessor())
                .name(AGGREGATED_CLICKS_STREAM);
    }


    private void splitInput() {
        SplitStream<Bcn> splitStream = bcnStream.split(new EventSelector());

        splitStream
                .select(BcnType.BID.getCode())
                .map(BidBcn::from)
                .name(BID_BCN_STREAM)
                .keyBy(BidBcn::getPartitionKey)
                .addSink(bidSink);

        splitStream
                .select(BcnType.IMPRESSION.getCode())
                .map(ImpressionBcn::from)
                .name(IMPRESSION_BCN_STREAM)
                .keyBy(ImpressionBcn::getPartitionKey)
                .addSink(impressionSink);

        splitStream
                .select(BcnType.CLICK.getCode())
                .map(ClickBcn::from)
                .name(CLICKS_BCN_STREAM)
                .keyBy(ClickBcn::getPartitionKey)
                .addSink(clickSink);
    }

    private <T> DataStream<T> getInputStream(String streamName, DeserializationSchema<T> schema) {
        Properties config = new Properties();
        config.setProperty(ConsumerConfigConstants.AWS_REGION, properties.getSourceAwsRegion());
        config.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, properties.getSourceStreamInitPos());

        return environment.addSource(
                new FlinkKinesisConsumer<>(streamName, schema, config),
                String.format("Kinesis stream: %s", streamName));
    }

    private <T> FlinkKinesisProducer<T> getSink(String streamName, SerializationSchema<T> schema) {
        Properties config = new Properties();
        config.setProperty(AWSConfigConstants.AWS_REGION, properties.getSinkAwsRegion());
        config.setProperty("AggregationEnabled", "false");

        FlinkKinesisProducer<T> sink = new FlinkKinesisProducer<>(schema, config);
        sink.setDefaultStream(streamName);
        sink.setDefaultPartition("0");

        return sink;
    }
}