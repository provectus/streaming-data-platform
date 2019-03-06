package com.provectus.fds.flink.aggregators;

class Metrics {
    private final long clicks;
    private final long impressions;
    private final long bids;

    Metrics(long clicks, long impressions, long bids) {
        this.clicks = clicks;
        this.impressions = impressions;
        this.bids = bids;
    }

    long getClicks() {
        return clicks;
    }

    long getImpressions() {
        return impressions;
    }

    long getBids() {
        return bids;
    }
}