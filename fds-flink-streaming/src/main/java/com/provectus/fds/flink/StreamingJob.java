package com.provectus.fds.flink;

import com.provectus.fds.flink.selectors.EventSelector;
import com.provectus.fds.models.bcns.*;
import com.provectus.fds.models.events.Aggregation;
import com.provectus.fds.models.events.Click;
import com.provectus.fds.models.events.Impression;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

class StreamingJob {
    private final DataStream<Bcn> inputStream;
    private final SinkFunction<Aggregation> sink;

    private DataStream<BidBcn> bidBcnStream;
    private DataStream<ClickBcn> clickBcnStream;
    private DataStream<ImpressionBcn> impressionBcnStream;

    StreamingJob(DataStream<Bcn> inputStream, SinkFunction<Aggregation> sink) {
        this.inputStream = inputStream;
        this.sink = sink;
        configure();
    }

    private void configure() {
        splitInput();

        // Joining logic
    }

    DataStream<BidBcn> getBidBcnStream() {
        return bidBcnStream;
    }

    DataStream<ClickBcn> getClickBcnStream() {
        return clickBcnStream;
    }

    DataStream<ImpressionBcn> getImpressionBcnStream() {
        return impressionBcnStream;
    }

    static DataStream<Impression> createImpressionStream(DataStream<BidBcn> bidBcnStream,
                                                         DataStream<ImpressionBcn> impressionBcnStream,
                                                         Time windowSize,
                                                         Time windowSlide) {
        return bidBcnStream
                .join(impressionBcnStream)
                .where(BidBcn::getPartitionKey)
                .equalTo(ImpressionBcn::getPartitionKey)
                .window(SlidingEventTimeWindows.of(windowSize, windowSlide))
                .apply(Impression::from);
    }

    static DataStream<Click> createClickStream(DataStream<Impression> impressionStream,
                                               DataStream<ClickBcn> clickBcnStream,
                                               Time windowSize,
                                               Time windowSlide) {
        return impressionStream
                .join(clickBcnStream)
                .where(Impression::getPartitionKey)
                .equalTo(ClickBcn::getPartitionKey)
                .window(SlidingEventTimeWindows.of(windowSize, windowSlide))
                .apply(Click::from);
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