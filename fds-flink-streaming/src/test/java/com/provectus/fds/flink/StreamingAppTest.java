package com.provectus.fds.flink;

import com.provectus.fds.models.bcns.*;
import com.provectus.fds.models.events.Aggregation;
import com.provectus.fds.models.events.Impression;
import io.flinkspector.datastream.DataStreamTestBase;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.hasItems;


public class StreamingAppTest extends DataStreamTestBase {
    @Test
    public void testBidsImpressionsJoining() {
        testEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        BidBcn bidBcn = BidBcn.from(getBcn(1, BcnType.BID));
        ImpressionBcn impressionBcn = ImpressionBcn.from(getBcn(1, BcnType.IMPRESSION));

        DataStream<BidBcn> bidBcnStream = createTestStream(Collections.singletonList(bidBcn))
                .assignTimestampsAndWatermarks(new TestWatermarkGenerator<>());
        DataStream<ImpressionBcn> impressionBcnStream = createTestStream(Collections.singletonList(impressionBcn))
                .assignTimestampsAndWatermarks(new TestWatermarkGenerator<>());

        SinkFunction<Impression> testSink = createTestSink(hasItems(Impression.from(bidBcn, impressionBcn)));

        DataStream<Impression> resultStream = StreamingJob
                .createImpressionStream(bidBcnStream, impressionBcnStream, Time.minutes(1), Time.milliseconds(1));

        resultStream.addSink(testSink);
    }

    private Bcn getBcn(int id, BcnType type) {
        return Bcn.builder()
                .txId("tx_" + id)
                .appUID("app_" + id)
                .campaignItemId(id)
                .creativeCategory("creative_category_" + id)
                .creativeId("creative_id_" + id)
                .domain("test_domain")
                .timestamp(System.currentTimeMillis() - 10_000)
                .winPrice(1L)
                .type(type.getCode())
                .build();
    }

    private Aggregation getAggregation() {
        return Aggregation.builder()
                .campaignItemId(1)
                .period("")
                .bids(1L)
                .clicks(1L)
                .imps(1L)
                .build();
    }

    private static class TestWatermarkGenerator<T extends Partitioned> implements AssignerWithPeriodicWatermarks<T> {
        private final long maxTimeLag = 5000;

        @Override
        public long extractTimestamp(T element, long previousElementTimestamp) {
            return element.extractTimestamp();
        }

        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(System.currentTimeMillis() - maxTimeLag);
        }
    }
}