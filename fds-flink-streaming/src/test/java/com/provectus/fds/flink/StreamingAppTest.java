package com.provectus.fds.flink;

import com.provectus.fds.flink.selectors.EventSelector;
import com.provectus.fds.models.bcns.*;
import com.provectus.fds.models.events.Aggregation;
import com.provectus.fds.models.events.Click;
import com.provectus.fds.models.events.Impression;
import com.provectus.fds.models.events.Location;
import com.provectus.fds.models.utils.DateTimeUtils;
import io.flinkspector.datastream.DataStreamTestBase;
import io.flinkspector.datastream.input.EventTimeInputBuilder;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Before;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;

public class StreamingAppTest extends DataStreamTestBase {
    private static final Time TEST_AGGREGATION_PERIOD = Time.minutes(10);

    private static final long WINDOW_START_1 = ZonedDateTime.now().truncatedTo(ChronoUnit.DAYS).toInstant().toEpochMilli();
    private static final long WINDOW_START_2 = WINDOW_START_1 + TEST_AGGREGATION_PERIOD.toMilliseconds();

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
                hasItems(
                        Click.from(imp1, clickBcn1),
                        Click.from(imp2, clickBcn2)
                ),
                iterableWithSize(2)));

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
                .emitWithTimestamp(imp2, WINDOW_START_2 + Time.minutes(1).toMilliseconds())
                .emitWithTimestamp(imp3, WINDOW_START_2 + Time.minutes(2).toMilliseconds()));

        // Expected results
        SinkFunction<Aggregation> testSink = createTestSink(allOf(
                hasItems(
                        getAggregation(1, 0, 1, 0, WINDOW_START_1),
                        getAggregation(2, 0, 2, 0, WINDOW_START_2)
                ),
                iterableWithSize(2))
        );

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
                .emitWithTimestamp(Click.from(imp2, clickBcn2), WINDOW_START_2 + Time.minutes(2).toMilliseconds())
                .emitWithTimestamp(Click.from(imp3, clickBcn3), WINDOW_START_2 + Time.minutes(8).toMilliseconds()));

        // Expected results
        SinkFunction<Aggregation> testSink = createTestSink(allOf(
                hasItems(
                        getAggregation(1, 0, 0, 1, WINDOW_START_1),
                        getAggregation(2, 0, 0, 2, WINDOW_START_2)),
                iterableWithSize(2)));

        // Configure chain
        StreamingJob
                .aggregateClicks(clickStream, TEST_AGGREGATION_PERIOD)
                .addSink(testSink);
    }

    @Test
    public void testLocationsJoin() {
        // Prepare sources
        Impression impression = Impression.from(BidBcn.from(getBcn(1, BcnType.BID)),
                ImpressionBcn.from(getBcn(1, BcnType.IMPRESSION)));
        Click click = Click.from(impression, ClickBcn.from(getBcn(1, BcnType.CLICK)));
        Location location = Location.builder()
                .appUID(click.getImpression().getBidBcn().getAppUID())
                .timestamp(WINDOW_START_1)
                .latitude(1.123456)
                .longitude(1.123456)
                .build();

        DataStream<Location> locationStream = createTestStream(EventTimeInputBuilder.startWith(location, WINDOW_START_1));
        DataStream<Click> clickStream = createTestStream(EventTimeInputBuilder.startWith(click, WINDOW_START_1));
        DataStream<Impression> impressionStream = createTestStream(EventTimeInputBuilder
                .startWith(impression, WINDOW_START_1 + 5 * 60 * 1000));

        // Expected results
        SinkFunction<Walkin> walkinTestSink = createTestSink(allOf(
                hasItems(Walkin.builder()
                        .txId(impression.getPartitionKey())
                        .winPrice(impression.getImpressionBcn().getWinPrice())
                        .appUID(location.getAppUID())
                        .timestamp(location.getTimestamp())
                        .latitude(location.getLatitude())
                        .longitude(location.getLongitude())
                        .build()),
                iterableWithSize(1)));

        SinkFunction<WalkinClick> walkinClickTest = createTestSink(allOf(
                hasItems(WalkinClick.builder()
                        .txId(impression.getPartitionKey())
                        .appUID(location.getAppUID())
                        .timestamp(location.getTimestamp())
                        .latitude(location.getLatitude())
                        .longitude(location.getLongitude())
                        .build()),
                iterableWithSize(1)));

        // Configure chain
        StreamingJob.createWalkinStream(impressionStream, locationStream, TEST_AGGREGATION_PERIOD).addSink(walkinTestSink);
        StreamingJob.createWalkinClickStream(clickStream, locationStream, TEST_AGGREGATION_PERIOD).addSink(walkinClickTest);
    }

    @Test
    public void testBcnSplit() {
        Bcn bcn1 = getBcn(1, BcnType.BID);
        Bcn bcn2 = getBcn(1, BcnType.CLICK);
        Bcn bcn3 = getBcn(1, BcnType.IMPRESSION);

        // Prepare sources
        DataStream<Bcn> bcnStream = createTestStream(EventTimeInputBuilder
                .startWith(bcn1, WINDOW_START_1)
                .emitWithTimestamp(bcn2, WINDOW_START_1)
                .emitWithTimestamp(bcn3, WINDOW_START_1));

        // Expected results
        SinkFunction<BidBcn> bidTestSink = createTestSink(allOf(hasItems(BidBcn.from(bcn1)), iterableWithSize(1)));
        SinkFunction<ClickBcn> clickTestSink = createTestSink(allOf(hasItems(ClickBcn.from(bcn2)), iterableWithSize(1)));
        SinkFunction<ImpressionBcn> impressionTestSink = createTestSink(allOf(hasItems(ImpressionBcn.from(bcn3)), iterableWithSize(1)));

        // Configure chain
        splitBcns(bcnStream, bidTestSink, clickTestSink, impressionTestSink);
    }

    private void splitBcns(
            DataStream<Bcn> bcnStream,
            SinkFunction<BidBcn> bidSink,
            SinkFunction<ClickBcn> clickSink,
            SinkFunction<ImpressionBcn> impressionSink) {
        SplitStream<Bcn> splitStream = bcnStream.split(new EventSelector());

        splitStream
                .select(BcnType.BID.getCode())
                .map(BidBcn::from)
                .keyBy(BidBcn::getPartitionKey)
                .addSink(bidSink);

        splitStream
                .select(BcnType.IMPRESSION.getCode())
                .map(ImpressionBcn::from)
                .keyBy(ImpressionBcn::getPartitionKey)
                .addSink(impressionSink);

        splitStream
                .select(BcnType.CLICK.getCode())
                .map(ClickBcn::from)
                .keyBy(ClickBcn::getPartitionKey)
                .addSink(clickSink);
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
                                       long imps,
                                       long clicks,
                                       long timestamp) {
        return new Aggregation(
                campaignItemId,
                DateTimeUtils.format(timestamp),
                bids,
                imps,
                clicks);
    }
}