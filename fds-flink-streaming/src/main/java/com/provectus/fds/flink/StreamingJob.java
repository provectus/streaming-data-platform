package com.provectus.fds.flink;

import com.provectus.fds.flink.aggregators.AggregationWindowProcessor;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

class StreamingJob {
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
        union(bidsAggregation, clicksAggregation, impressionsAggregation).addSink(sink);
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
                .aggregate(new MetricsAggregator<>(), new AggregationWindowProcessor());
    }

    static DataStream<Aggregation> aggregateImpressions(DataStream<Impression> impressionStream, Time aggregationPeriod) {
        return impressionStream
                .keyBy(imp -> imp.getBidBcn().getCampaignItemId())
                .window(TumblingEventTimeWindows.of(aggregationPeriod))
                .aggregate(new MetricsAggregator<>(), new AggregationWindowProcessor());
    }

    static DataStream<Aggregation> aggregateClicks(DataStream<Click> clickStream, Time aggregationPeriod) {
        return clickStream
                .keyBy(click -> click.getImpression().getBidBcn().getCampaignItemId())
                .window(TumblingEventTimeWindows.of(aggregationPeriod))
                .aggregate(new MetricsAggregator<>(), new AggregationWindowProcessor());
    }

    @SafeVarargs
    static DataStream<Aggregation> union(DataStream<Aggregation> base, DataStream<Aggregation>... other) {
        return base.union(other);
    }

    private void splitInput() {
        SplitStream<Bcn> splitStream = inputStream.split(new EventSelector());

        bidBcnStream = splitStream
                .select(BcnType.BID.getCode())
                .map(BidBcn::from)
                .keyBy(BidBcn::getPartitionKey);

        clickBcnStream = splitStream
                .select(BcnType.CLICK.getCode())
                .map(ClickBcn::from)
                .keyBy(ClickBcn::getPartitionKey);

        impressionBcnStream = splitStream
                .select(BcnType.IMPRESSION.getCode())
                .map(ImpressionBcn::from)
                .keyBy(ImpressionBcn::getPartitionKey);
    }
}