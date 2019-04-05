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

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;


public class StreamingAppTest extends DataStreamTestBase {
    private static final Time TEST_AGGREGATION_PERIOD = Time.seconds(20);

    private static final long WINDOW_START_1 = LocalDate.now().atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
    private static final long WINDOW_START_2 = WINDOW_START_1 + TEST_AGGREGATION_PERIOD.toMilliseconds();
    private static final long WINDOW_START_3 = WINDOW_START_2 + TEST_AGGREGATION_PERIOD.toMilliseconds();

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
        SinkFunction<Impression> testSink = createTestSink(allOf(
                hasItems(Impression.from(bidBcn1, impressionBcn1)),
                iterableWithSize(2)));

        // Configure chain
        StreamingJob
                .createImpressionStream(bidBcnStream, impressionBcnStream, TEST_AGGREGATION_PERIOD)
                .addSink(testSink);
    }

    @Test
    public void testBidsAndImpressionsJoiningOnDuplicates() {
        // Prepare sources
        BidBcn bidBcn1 = BidBcn.from(getBcn(1, BcnType.BID));
        BidBcn bidBcn2 = BidBcn.from(getBcn(2, BcnType.BID));

        ImpressionBcn impressionBcn1 = ImpressionBcn.from(getBcn(1, BcnType.IMPRESSION));
        ImpressionBcn impressionBcn2 = ImpressionBcn.from(getBcn(2, BcnType.IMPRESSION));

        DataStream<BidBcn> bidBcnStream = createTestStream(EventTimeInputBuilder
                .startWith(bidBcn1, after(5, TimeUnit.SECONDS))
                .emit(bidBcn2, after(5, TimeUnit.SECONDS)));

        DataStream<ImpressionBcn> impressionBcnStream = createTestStream(EventTimeInputBuilder
                .startWith(impressionBcn1, after(5, TimeUnit.SECONDS))
                .emit(impressionBcn2, after(5, TimeUnit.SECONDS)));


        // Expected results
        SinkFunction<Impression> testSink = createTestSink(allOf(
                hasItems(Impression.from(bidBcn1, impressionBcn1), Impression.from(bidBcn2, impressionBcn2)),
                iterableWithSize(2))
        );

        // Configure chain
        StreamingJob
                .createImpressionStream(bidBcnStream, impressionBcnStream, TEST_AGGREGATION_PERIOD)
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
        SinkFunction<Click> testSink = createTestSink(allOf(
                hasItems(Click.from(imp1, clickBcn1)),
                iterableWithSize(1)));

        // Configure chain
        StreamingJob
                .createClickStream(impStream, clickBcnStream, TEST_AGGREGATION_PERIOD)
                .addSink(testSink);
    }

    @Test
    public void testBidsAggregation() {
        // Prepare sources
        BidBcn bidBcn1 = BidBcn.from(getBcn(1, 1, BcnType.BID));
        BidBcn bidBcn2 = BidBcn.from(getBcn(2, 2, BcnType.BID));
        BidBcn bidBcn3 = BidBcn.from(getBcn(3, 2, BcnType.BID));

        DataStream<BidBcn> bidBcnStream = createTestStream(EventTimeInputBuilder
                .startWith(bidBcn1, WINDOW_START_1)
                .emitWithTimestamp(bidBcn2, WINDOW_START_2)
                .emitWithTimestamp(bidBcn3, WINDOW_START_2 + 5_000));

        // Expected results
        SinkFunction<Aggregation> testSink = createTestSink(allOf(
                hasItems(
                        getAggregation(1, 1, 0, 0, WINDOW_START_1),
                        getAggregation(2, 2, 0, 0, WINDOW_START_2)),
                iterableWithSize(2)));

        // Configure chain
        StreamingJob
                .aggregateBidsBcn(bidBcnStream, TEST_AGGREGATION_PERIOD)
                .addSink(testSink);
    }

    @Test
    public void testImpressionsAggregation() {
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
                .startWith(imp1, WINDOW_START_1)
                .emitWithTimestamp(imp2, WINDOW_START_2)
                .emitWithTimestamp(imp3, WINDOW_START_2 + 5_000));

        // Expected results
        SinkFunction<Aggregation> testSink = createTestSink(allOf(
                hasItems(
                        getAggregation(1, 0, 0, 1, WINDOW_START_1),
                        getAggregation(2, 0, 0, 2, WINDOW_START_2)),
                iterableWithSize(2)));

        // Configure chain
        StreamingJob
                .aggregateImpressions(impStream, TEST_AGGREGATION_PERIOD)
                .addSink(testSink);
    }

    @Test
    public void testClicksAggregation() {
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
                .startWith(Click.from(imp1, clickBcn1), WINDOW_START_1)
                .emitWithTimestamp(Click.from(imp2, clickBcn2), WINDOW_START_2)
                .emitWithTimestamp(Click.from(imp3, clickBcn3), WINDOW_START_2 + 5_000));

        // Expected results
        SinkFunction<Aggregation> testSink = createTestSink(allOf(
                hasItems(
                        getAggregation(1, 0, 1, 0, WINDOW_START_1),
                        getAggregation(2, 0, 2, 0, WINDOW_START_2)),
                iterableWithSize(2)));

        // Configure chain
        StreamingJob
                .aggregateClicks(clickStream, TEST_AGGREGATION_PERIOD)
                .addSink(testSink);
    }

    @Test
    public void testAggregationUnionWithOverlapping() {
        // Prepare sources
        Aggregation bids = getAggregation(1, 10, 0, 0, WINDOW_START_1);
        Aggregation imps = getAggregation(1, 0, 10, 0, WINDOW_START_1);
        Aggregation clicks = getAggregation(1, 0, 0, 10, WINDOW_START_1);

        Aggregation expected = getAggregation(1, 10, 10, 10, WINDOW_START_1);

        DataStream<Aggregation> bidStream1 = createTestStream(EventTimeInputBuilder.startWith(bids, WINDOW_START_1));
        DataStream<Aggregation> impStream2 = createTestStream(EventTimeInputBuilder.startWith(imps, WINDOW_START_1));
        DataStream<Aggregation> clickStream3 = createTestStream(EventTimeInputBuilder.startWith(clicks, WINDOW_START_1));

        // Expected results
        SinkFunction<Aggregation> testSink = createTestSink(allOf(hasItems(expected), iterableWithSize(1)));

        // Configure chain
        StreamingJob.joinAggregations(bidStream1, impStream2, clickStream3, TEST_AGGREGATION_PERIOD)
                .addSink(testSink);
    }

    @Test
    public void testAggregationUnionWithoutOverlapping() {
        // Prepare sources
        Aggregation agg1 = getAggregation(1, 10, 10, 10, WINDOW_START_1);
        Aggregation agg2 = getAggregation(2, 10, 10, 10, WINDOW_START_2);
        Aggregation agg3 = getAggregation(3, 10, 10, 10, WINDOW_START_3);

        DataStream<Aggregation> aggStream1 = createTestStream(EventTimeInputBuilder.startWith(agg1, WINDOW_START_1));
        DataStream<Aggregation> aggStream2 = createTestStream(EventTimeInputBuilder.startWith(agg2, WINDOW_START_2));
        DataStream<Aggregation> aggStream3 = createTestStream(EventTimeInputBuilder.startWith(agg3, WINDOW_START_3));

        // Expected results
        SinkFunction<Aggregation> testSink = createTestSink(allOf(
                hasItems(agg1, agg2, agg3),
                iterableWithSize(3)));

        // Configure chain
        StreamingJob.joinAggregations(aggStream1, aggStream2, aggStream3, TEST_AGGREGATION_PERIOD)
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
                        .truncate(timestamp, TEST_AGGREGATION_PERIOD.toMilliseconds())))
                .bids(bids)
                .clicks(clicks)
                .imps(imps)
                .build();
    }
}