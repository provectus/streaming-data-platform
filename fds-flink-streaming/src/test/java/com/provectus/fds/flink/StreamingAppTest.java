package com.provectus.fds.flink;

import com.provectus.fds.flink.config.StreamingProperties;
import com.provectus.fds.models.bcns.*;
import com.provectus.fds.models.events.Aggregation;
import com.provectus.fds.models.events.Click;
import com.provectus.fds.models.events.Impression;
import com.provectus.fds.models.utils.DateTimeUtils;
import io.flinkspector.datastream.DataStreamTestBase;
import io.flinkspector.datastream.input.EventTimeInputBuilder;
import io.flinkspector.datastream.input.time.InWindow;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;


public class StreamingAppTest extends DataStreamTestBase {
    private static final Time TEST_AGGREGATION_PERIOD = Time.seconds(20);

    private static final long WINDOW_START_1 = LocalDate.now().atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();

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
    public void testAggregationUnionWithOverlapping() {
        // Prepare sources
        BidBcn bidBcn = BidBcn.from(getBcn(1, BcnType.BID));
        Impression impression = Impression.from(bidBcn, ImpressionBcn.from(getBcn(1, BcnType.IMPRESSION)));
        Click click = Click.from(impression, ClickBcn.from(getBcn(1, BcnType.CLICK)));

        Aggregation agg = getAggregation(1, 1, 1, 1, WINDOW_START_1);

        DataStream<BidBcn> bidStream = createTestStream(EventTimeInputBuilder.startWith(bidBcn, WINDOW_START_1));
        DataStream<Impression> impStream = createTestStream(EventTimeInputBuilder.startWith(impression, WINDOW_START_1 + 1000));
        DataStream<Click> clickStream = createTestStream(EventTimeInputBuilder.startWith(click, WINDOW_START_1 + 2000));

        // Expected results
        SinkFunction<Aggregation> testSink = createTestSink(allOf(
                hasItems(agg),
                iterableWithSize(1)));

        // Configure chain
        StreamingJob.joinMetrics(bidStream, impStream, clickStream, TEST_AGGREGATION_PERIOD)
                .addSink(testSink);
    }

    @Test
    public void testAggregationUnionWithoutOverlapping() {
        // Prepare sources
        BidBcn bidBcn = BidBcn.from(getBcn(1, BcnType.BID));
        Impression impression = Impression.from(bidBcn, ImpressionBcn.from(getBcn(1, BcnType.IMPRESSION)));
        Click click = Click.from(impression, ClickBcn.from(getBcn(1, BcnType.CLICK)));

        Aggregation agg1 = getAggregation(1, 1, 0, 0, WINDOW_START_1);
        Aggregation agg2 = getAggregation(1, 0, 0, 1, WINDOW_START_1 + 60_000);
        Aggregation agg3 = getAggregation(1, 0, 1, 0, WINDOW_START_1 + 120_000);

        DataStream<BidBcn> bidStream = createTestStream(EventTimeInputBuilder.startWith(bidBcn, WINDOW_START_1));
        DataStream<Impression> impStream = createTestStream(EventTimeInputBuilder.startWith(impression, WINDOW_START_1 + 60_000));
        DataStream<Click> clickStream = createTestStream(EventTimeInputBuilder.startWith(click, WINDOW_START_1 + 120_000));

        // Expected results
        SinkFunction<Aggregation> testSink = createTestSink(allOf(
                hasItems(agg1, agg2, agg3),
                iterableWithSize(3)));

        // Configure chain
        StreamingJob.joinMetrics(bidStream, impStream, clickStream, TEST_AGGREGATION_PERIOD)
                .addSink(testSink);
    }

    @Test
    public void fullPipelineTest() {
        Properties sourceProperties = new Properties() {{
            setProperty("", "");
            setProperty("", "");
            setProperty("", "");
        }};
        Properties sinkProperties = new Properties() {{
            setProperty("sink.stream.name", "Test sink");
        }};
        Properties aggregationProperties = new Properties() {{
            setProperty("aggregation.bids.session.timeout", "20");
            setProperty("aggregation.clicks.session.timeout", "20");
            setProperty("aggregation.period", "20");
        }};

        StreamingProperties streamingProperties = StreamingProperties.fromProperties(
                sourceProperties, sinkProperties, aggregationProperties);


        // Prepare sources
        Bcn bcn1 = getBcn(1, BcnType.BID);
        Bcn bcn2 = getBcn(1, BcnType.IMPRESSION);
        Bcn bcn3 = getBcn(1, BcnType.CLICK);

        Aggregation agg = getAggregation(1, 1, 1, 1, WINDOW_START_1);
        agg.setPeriod(DateTimeUtils.format(WINDOW_START_1 - 1));

        DataStream<Bcn> bcnStream = createTestStream(EventTimeInputBuilder
                .startWith(bcn1, InWindow.to(WINDOW_START_1, TimeUnit.MILLISECONDS))
                .emit(bcn2, InWindow.to(WINDOW_START_1, TimeUnit.MILLISECONDS))
                .emit(bcn3, InWindow.to(WINDOW_START_1, TimeUnit.MILLISECONDS)));

        // Expected results
        SinkFunction<Aggregation> testSink = createTestSink(allOf(hasItems(agg), iterableWithSize(1)));

        // Configure chain
        new StreamingJob(bcnStream, testSink, streamingProperties);
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