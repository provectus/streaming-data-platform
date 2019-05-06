package com.provectus.fds.flink.aggregators;

import org.apache.flink.api.common.functions.AggregateFunction;

public class MetricsAggregator<T> implements AggregateFunction<T, AggregationAccumulator<T>, Metrics> {
    @Override
    public AggregationAccumulator<T> createAccumulator() {

        return new AggregationAccumulator<>();
    }

    @Override
    public AggregationAccumulator<T> add(T value, AggregationAccumulator<T> accumulator) {
        return accumulator.add(value);
    }

    @Override
    public Metrics getResult(AggregationAccumulator<T> accumulator) {
        return accumulator.build();
    }

    @Override
    public AggregationAccumulator<T> merge(AggregationAccumulator<T> a, AggregationAccumulator<T> b) {
        return a.merge(b);
    }
}