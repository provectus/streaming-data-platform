package com.provectus.fds.flink;

import com.provectus.fds.flink.aggregators.AggregationWindowProcessor;
import com.provectus.fds.flink.aggregators.Metrics;
import com.provectus.fds.flink.aggregators.MetricsAggregator;
import com.provectus.fds.flink.config.StreamingProperties;
import com.provectus.fds.flink.selectors.EventSelector;
import com.provectus.fds.models.bcns.*;
import com.provectus.fds.models.events.Aggregation;
import com.provectus.fds.models.events.Click;
import com.provectus.fds.models.events.Impression;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StreamingJob {
    private static final String BID_BCN_STREAM = "BidBcn stream";
    private static final String IMPRESSION_BCN_STREAM = "ImpressionBcn stream";
    private static final String CLICKS_BCN_STREAM = "ClickBcn stream";

    private static final String JOINED_AGGREGATIONS_STREAM = "Joined aggregations";

    private static final Logger log = LoggerFactory.getLogger(StreamingJob.class);

    private final DataStream<Bcn> inputStream;
    private final SinkFunction<Aggregation> sink;
    private final StreamingProperties properties;

    private DataStream<BidBcn> bidBcnStream;
    private DataStream<ClickBcn> clickBcnStream;
    private DataStream<ImpressionBcn> impressionBcnStream;

    StreamingJob(DataStream<Bcn> inputStream, SinkFunction<Aggregation> sink, StreamingProperties properties) {
        this.inputStream = inputStream;
        this.sink = sink;
        this.properties = properties;

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

        // Join all aggregations and add sink
        joinMetrics(bidBcnStream, impressionStream, clickStream, period)
                .addSink(sink)
                .name(String.format("Kinesis stream: %s", properties.getSinkStreamName()));
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

    static DataStream<Aggregation> joinMetrics(
            DataStream<BidBcn> bids,
            DataStream<Impression> imps,
            DataStream<Click> clicks,
            Time aggregationPeriod) {

        DataStream<Metrics> allMetrics = bids.map(Metrics::of).union(imps.map(Metrics::of), clicks.map(Metrics::of));
        return aggregateMetrics(allMetrics, aggregationPeriod);
    }

    private static DataStream<Aggregation> aggregateMetrics(DataStream<Metrics> metrics, Time aggregationPeriod) {
        return metrics
                .keyBy(Metrics::getCampaignItemId)
                .window(EventTimeSessionWindows.withGap(aggregationPeriod))
                .aggregate(new MetricsAggregator(), new AggregationWindowProcessor())
                .name(JOINED_AGGREGATIONS_STREAM);
    }

    private void splitInput() {
        SplitStream<Bcn> splitStream = inputStream.split(new EventSelector());

        bidBcnStream = splitStream
                .select(BcnType.BID.getCode())
                .map(BidBcn::from)
                .name(BID_BCN_STREAM)
                .keyBy(BidBcn::getPartitionKey);

        impressionBcnStream = splitStream
                .select(BcnType.IMPRESSION.getCode())
                .map(ImpressionBcn::from)
                .name(IMPRESSION_BCN_STREAM)
                .keyBy(ImpressionBcn::getPartitionKey);

        clickBcnStream = splitStream
                .select(BcnType.CLICK.getCode())
                .map(ClickBcn::from)
                .name(CLICKS_BCN_STREAM)
                .keyBy(ClickBcn::getPartitionKey);
    }
}