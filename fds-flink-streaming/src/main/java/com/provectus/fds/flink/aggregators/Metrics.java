package com.provectus.fds.flink.aggregators;

import com.provectus.fds.models.bcns.BidBcn;
import com.provectus.fds.models.events.Click;
import com.provectus.fds.models.events.Impression;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Metrics {
    private long campaignItemId;
    private long bids;
    private long impressions;
    private long clicks;

    public Metrics(long bids, long impressions, long clicks) {
        this.bids = bids;
        this.impressions = impressions;
        this.clicks = clicks;
    }

    public static Metrics of(BidBcn bidBcn) {
        return Metrics.builder()
                .campaignItemId(getCampaignItemId(bidBcn))
                .bids(1)
                .build();
    }

    public static Metrics of(Impression impression) {
        return Metrics.builder()
                .campaignItemId(getCampaignItemId(impression))
                .impressions(1)
                .build();
    }

    public static Metrics of(Click click) {
        return Metrics.builder()
                .campaignItemId(getCampaignItemId(click))
                .clicks(1)
                .build();
    }

    private static long getCampaignItemId(BidBcn bidBcn) {
        return bidBcn == null ? 0 : bidBcn.getCampaignItemId();
    }

    private static long getCampaignItemId(Impression impression) {
        return impression == null ? 0 : getCampaignItemId(impression.getBidBcn());
    }

    private static long getCampaignItemId(Click click) {
        return click == null ? 0 : getCampaignItemId(click.getImpression());
    }
}