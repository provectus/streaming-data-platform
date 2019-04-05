package com.provectus.fds.flink.aggregators;

import org.apache.flink.api.common.functions.AggregateFunction;

public class MetricsAggregator implements AggregateFunction<Metrics, MetricsAccumulator, Metrics> {
    @Override
    public MetricsAccumulator createAccumulator() {

        return new MetricsAccumulator();
    }

    @Override
    public MetricsAccumulator add(Metrics value, MetricsAccumulator accumulator) {
        return accumulator.add(value);
    }

    @Override
    public Metrics getResult(MetricsAccumulator accumulator) {
        return accumulator.build();
    }

    @Override
    public MetricsAccumulator merge(MetricsAccumulator a, MetricsAccumulator b) {
        return a.merge(b);
    }
}