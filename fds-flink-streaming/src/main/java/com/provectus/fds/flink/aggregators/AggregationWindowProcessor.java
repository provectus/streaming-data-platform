package com.provectus.fds.flink.aggregators;

import com.provectus.fds.models.events.Aggregation;
import com.provectus.fds.models.utils.DateTimeUtils;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AggregationWindowProcessor extends ProcessWindowFunction<Metrics, Aggregation, Long, TimeWindow> {
    @Override
    public void process(Long key, Context context, Iterable<Metrics> result, Collector<Aggregation> out) {
        Metrics metrics = result.iterator().next();

        out.collect(Aggregation.builder()
                .campaignItemId(key)
                .period(DateTimeUtils.format(context.window().getStart()))
                .bids(metrics.getBids())
                .imps(metrics.getImpressions())
                .clicks(metrics.getClicks())
                .build());
    }
}