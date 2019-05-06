package com.provectus.fds.flink.aggregators;

import com.provectus.fds.models.bcns.BidBcn;
import com.provectus.fds.models.events.Click;
import com.provectus.fds.models.events.Impression;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class AggregationAccumulator<T> {
    private long clicks;
    private long impressions;
    private long bids;

    public AggregationAccumulator() {
    }

    public AggregationAccumulator(AggregationAccumulator<T> acc) {
        this.bids = acc.bids;
        this.impressions = acc.impressions;
        this.clicks = acc.clicks;
    }

    public AggregationAccumulator<T> add(T value) {
        Class<?> tClass = value.getClass();

        if (BidBcn.class.isAssignableFrom(tClass)) {
            bids++;
        } else if (Impression.class.isAssignableFrom(tClass)) {
            impressions++;
        } else if (Click.class.isAssignableFrom(tClass)) {
            clicks++;
        } else {
            throw new IllegalStateException(String.format("Unsupported metric class: %s", tClass));
        }

        return this;
    }

    public AggregationAccumulator<T> merge(AggregationAccumulator<T> other) {
        AggregationAccumulator<T> newAcc = new AggregationAccumulator<>(this);
        newAcc.bids += other.bids;
        newAcc.impressions += other.impressions;
        newAcc.clicks += other.clicks;

        return newAcc;
    }

    public Metrics build() {
        return new Metrics(bids, impressions, clicks);
    }
}