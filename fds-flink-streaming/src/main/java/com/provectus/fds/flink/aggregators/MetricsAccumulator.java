package com.provectus.fds.flink.aggregators;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class MetricsAccumulator {
    private long bids;
    private long impressions;
    private long clicks;

    public MetricsAccumulator() {
    }

    public MetricsAccumulator(MetricsAccumulator acc) {
        this.bids = acc.bids;
        this.impressions = acc.impressions;
        this.clicks = acc.clicks;
    }

    public MetricsAccumulator add(Metrics metrics) {
        bids += metrics.getBids();
        impressions += metrics.getImpressions();
        clicks += metrics.getClicks();

        return this;
    }

    public MetricsAccumulator merge(MetricsAccumulator other) {
        MetricsAccumulator newAcc = new MetricsAccumulator(this);
        newAcc.bids += other.bids;
        newAcc.impressions += other.impressions;
        newAcc.clicks += other.clicks;

        return newAcc;
    }

    public Metrics build() {
        return new Metrics.MetricsBuilder()
                .bids(bids)
                .impressions(impressions)
                .clicks(clicks)
                .build();
    }
}