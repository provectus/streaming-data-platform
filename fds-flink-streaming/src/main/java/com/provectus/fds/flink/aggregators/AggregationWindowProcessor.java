package com.provectus.fds.flink.aggregators;

import com.provectus.fds.models.events.Aggregation;
import com.provectus.fds.models.utils.DateTimeUtils;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AggregationWindowProcessor extends ProcessWindowFunction<Metrics, Aggregation, Long, TimeWindow> {
    private long aggregationPeriod;

    public AggregationWindowProcessor() {
    }

    public AggregationWindowProcessor(long aggregationPeriod) {
        this.aggregationPeriod = aggregationPeriod;
    }

    public AggregationWindowProcessor setAggregationPeriod(long aggregationPeriod) {
        this.aggregationPeriod = aggregationPeriod;
        return this;
    }

    public long getAggregationPeriod() {
        return aggregationPeriod;
    }

    @Override
    public void process(Long key, Context context, Iterable<Metrics> result, Collector<Aggregation> out) {
        Metrics metrics = result.iterator().next();

        out.collect(new Aggregation(key,
                DateTimeUtils.format(DateTimeUtils.truncate(context.window().getStart(), aggregationPeriod)),
                metrics.getBids(),
                metrics.getImpressions(),
                metrics.getClicks()));
    }
}