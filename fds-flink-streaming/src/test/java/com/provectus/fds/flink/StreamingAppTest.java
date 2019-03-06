package com.provectus.fds.flink;

import com.provectus.fds.models.bcns.*;
import com.provectus.fds.models.events.Aggregation;
import com.provectus.fds.models.events.Click;
import com.provectus.fds.models.events.Impression;
import com.provectus.fds.models.utils.DateTimeUtils;
import io.flinkspector.datastream.DataStreamTestBase;
import io.flinkspector.datastream.input.EventTimeInputBuilder;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.hasItems;


public class StreamingAppTest extends DataStreamTestBase {
    private static final Time TEST_WINDOW_SIZE = Time.seconds(20);
    private static final Time TEST_WINDOW_SLIDE = Time.seconds(5);

    @Before
    public void configureEnvironment() {
        testEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    }

    @Test
    public void testBidsAndImpressionsJoining() {
        // Prepare sources
        BidBcn bidBcn1 = BidBcn.from(getBcn(1, BcnType.BID));
        BidBcn bidBcn2 = BidBcn.from(getBcn(2, BcnType.BID));

        ImpressionBcn impressionBcn1 = ImpressionBcn.from(getBcn(1, BcnType.IMPRESSION));
        ImpressionBcn impressionBcn2 = ImpressionBcn.from(getBcn(2, BcnType.IMPRESSION));

        DataStream<BidBcn> bidBcnStream = createTestStream(EventTimeInputBuilder
                .startWith(bidBcn1, after(5, TimeUnit.SECONDS))
                .emit(bidBcn2, after(25, TimeUnit.SECONDS)));

        DataStream<ImpressionBcn> impressionBcnStream = createTestStream(EventTimeInputBuilder
                .startWith(impressionBcn1, after(5, TimeUnit.SECONDS))
                .emit(impressionBcn2, after(40, TimeUnit.SECONDS)));


        // Expected results
        SinkFunction<Impression> testSink = createTestSink(hasItems(Impression.from(bidBcn1, impressionBcn1)));

        // Configure chain
        StreamingJob
                .createImpressionStream(bidBcnStream, impressionBcnStream, TEST_WINDOW_SIZE, TEST_WINDOW_SLIDE)
                .addSink(testSink);
    }

    @Test
    public void testImpressionsAndClicksJoining() {
        // Prepare sources
        ClickBcn clickBcn1 = ClickBcn.from(getBcn(1, BcnType.CLICK));
        ClickBcn clickBcn2 = ClickBcn.from(getBcn(2, BcnType.CLICK));

        Impression imp1 = Impression.from(
                BidBcn.from(getBcn(1, BcnType.BID)),
                ImpressionBcn.from(getBcn(1, BcnType.IMPRESSION)));
        Impression imp2 = Impression.from(
                BidBcn.from(getBcn(2, BcnType.BID)),
                ImpressionBcn.from(getBcn(2, BcnType.IMPRESSION)));

        DataStream<Impression> impStream = createTestStream(EventTimeInputBuilder
                .startWith(imp1, after(5, TimeUnit.SECONDS))
                .emit(imp2, after(30, TimeUnit.SECONDS)));

        DataStream<ClickBcn> clickBcnStream = createTestStream(EventTimeInputBuilder
                .startWith(clickBcn1, after(15, TimeUnit.SECONDS))
                .emit(clickBcn2, after(60, TimeUnit.SECONDS)));


        // Expected results
        SinkFunction<Click> testSink = createTestSink(
                hasItems(Click.from(imp1, clickBcn1)));

        // Configure chain
        StreamingJob
                .createClickStream(impStream, clickBcnStream, TEST_WINDOW_SIZE, TEST_WINDOW_SLIDE)
                .addSink(testSink);
    }

    @Test
    public void testBidsAggregation() {
        long timestamp1 = System.currentTimeMillis() + 5_000L;  // window 1
        long timestamp2 = timestamp1 + 20_000;                  // window 2
        long timestamp3 = timestamp2 + 5_000;                   // window 2

        // Prepare sources
        BidBcn bidBcn1 = BidBcn.from(getBcn(1, 1, BcnType.BID));
        BidBcn bidBcn2 = BidBcn.from(getBcn(2, 2, BcnType.BID));
        BidBcn bidBcn3 = BidBcn.from(getBcn(3, 2, BcnType.BID));

        DataStream<BidBcn> bidBcnStream = createTestStream(EventTimeInputBuilder
                .startWith(bidBcn1, timestamp1)
                .emitWithTimestamp(bidBcn2, timestamp2)
                .emitWithTimestamp(bidBcn3, timestamp3));

        // Expected results
        SinkFunction<Aggregation> testSink = createTestSink(hasItems(
                getAggregation(1, 1, 0, 0, timestamp1),
                getAggregation(2, 2, 0, 0, timestamp2)));

        // Configure chain
        StreamingJob
                .aggregateBidsBcn(bidBcnStream, TEST_WINDOW_SIZE)
                .addSink(testSink);
    }

    @Test
    public void testImpressionsAggregation() {
        long timestamp1 = System.currentTimeMillis() + 5_000L;  // window 1
        long timestamp2 = timestamp1 + 20_000;                  // window 2
        long timestamp3 = timestamp2 + 5_000;                   // window 2

        // Prepare sources
        Impression imp1 = Impression.from(
                BidBcn.from(getBcn(1, BcnType.BID)),
                ImpressionBcn.from(getBcn(1, BcnType.IMPRESSION)));
        Impression imp2 = Impression.from(
                BidBcn.from(getBcn(2, BcnType.BID)),
                ImpressionBcn.from(getBcn(2, BcnType.IMPRESSION)));
        Impression imp3 = Impression.from(
                BidBcn.from(getBcn(3, 2, BcnType.BID)),
                ImpressionBcn.from(getBcn(3, 2, BcnType.IMPRESSION)));

        DataStream<Impression> impStream = createTestStream(EventTimeInputBuilder
                .startWith(imp1, timestamp1)
                .emitWithTimestamp(imp2, timestamp2)
                .emitWithTimestamp(imp3, timestamp3));

        // Expected results
        SinkFunction<Aggregation> testSink = createTestSink(hasItems(
                getAggregation(1, 0, 0, 1, timestamp1),
                getAggregation(2, 0, 0, 2, timestamp2)));

        // Configure chain
        StreamingJob
                .aggregateImpressions(impStream, TEST_WINDOW_SIZE)
                .addSink(testSink);
    }

    @Test
    public void testClicksAggregation() {
        long timestamp1 = System.currentTimeMillis() + 5_000L;  // window 1
        long timestamp2 = timestamp1 + 20_000;                  // window 2
        long timestamp3 = timestamp2 + 1_000;                   // window 2

        // Prepare sources
        ClickBcn clickBcn1 = ClickBcn.from(getBcn(1, BcnType.CLICK));
        ClickBcn clickBcn2 = ClickBcn.from(getBcn(2, BcnType.CLICK));
        ClickBcn clickBcn3 = ClickBcn.from(getBcn(3, 2, BcnType.CLICK));

        Impression imp1 = Impression.from(BidBcn.from(getBcn(1, BcnType.BID)),
                ImpressionBcn.from(getBcn(1, BcnType.IMPRESSION)));
        Impression imp2 = Impression.from(BidBcn.from(getBcn(2, BcnType.BID)),
                ImpressionBcn.from(getBcn(2, BcnType.IMPRESSION)));
        Impression imp3 = Impression.from(BidBcn.from(getBcn(3, 2, BcnType.BID)),
                ImpressionBcn.from(getBcn(3, 2, BcnType.IMPRESSION)));

        DataStream<Click> clickStream = createTestStream(EventTimeInputBuilder
                .startWith(Click.from(imp1, clickBcn1), timestamp1)
                .emitWithTimestamp(Click.from(imp2, clickBcn2), timestamp2)
                .emitWithTimestamp(Click.from(imp3, clickBcn3), timestamp3));

        // Expected results
        SinkFunction<Aggregation> testSink = createTestSink(hasItems(
                getAggregation(1, 0, 1, 0, timestamp1),
                getAggregation(2, 0, 2, 0, timestamp2)));

        // Configure chain
        StreamingJob
                .aggregateClicks(clickStream, TEST_WINDOW_SIZE)
                .addSink(testSink);
    }

    @Test
    public void testAggregationUnion() {
        long timestamp1 = System.currentTimeMillis() + 10_000L;  // window 1
        long timestamp2 = timestamp1 + 20_000;                   // window 2
        long timestamp3 = timestamp2 + 30_000;                   // window 3

        // Prepare sources
        Aggregation agg1 = getAggregation(1, 10, 10, 10, timestamp1);
        Aggregation agg2 = getAggregation(2, 5, 5, 5, timestamp2);
        Aggregation agg3 = getAggregation(3, 1, 1, 1, timestamp3);

        DataStream<Aggregation> aggStream1 = createTestStream(EventTimeInputBuilder.startWith(agg1, timestamp1));
        DataStream<Aggregation> aggStream2 = createTestStream(EventTimeInputBuilder.startWith(agg2, timestamp2));
        DataStream<Aggregation> aggStream3 = createTestStream(EventTimeInputBuilder.startWith(agg3, timestamp3));

        // Expected results
        SinkFunction<Aggregation> testSink = createTestSink(hasItems(agg1, agg2, agg3));

        // Configure chain
        StreamingJob
                .union(aggStream1, aggStream2, aggStream3)
                .addSink(testSink);
    }

    private Bcn getBcn(int id, BcnType type) {
        return getBcn(id, id, type);
    }

    private Bcn getBcn(int id, long campaignItemId, BcnType type) {
        return Bcn.builder()
                .txId("tx_" + id)
                .appUID("app_" + id)
                .campaignItemId(campaignItemId)
                .creativeCategory("creative_category_" + id)
                .creativeId("creative_id_" + id)
                .domain("test_domain")
                .winPrice(1L)
                .type(type.getCode())
                .build();
    }

    private Aggregation getAggregation(int campaignItemId,
                                       long bids,
                                       long clicks,
                                       long imps,
                                       long timestamp) {
        return Aggregation.builder()
                .campaignItemId(campaignItemId)
                .period(DateTimeUtils.format(DateTimeUtils
                        .truncate(timestamp, TEST_WINDOW_SIZE.toMilliseconds())))
                .bids(bids)
                .clicks(clicks)
                .imps(imps)
                .build();
    }
}