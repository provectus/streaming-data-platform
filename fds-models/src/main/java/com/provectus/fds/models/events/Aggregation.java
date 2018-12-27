package com.provectus.fds.models.events;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

public class Aggregation {
    @JsonProperty("campaign_item_id")
    private long campaignItemId;
    private String period;
    private Long clicks;
    private Long imps;
    private Long bids;

    public Aggregation(long campaignItemId, String period, Long clicks, Long imps, Long bids) {
        this.campaignItemId = campaignItemId;
        this.period = period;
        this.clicks = clicks;
        this.imps = imps;
        this.bids = bids;
    }

    public long getCampaignItemId() {
        return campaignItemId;
    }

    public void setCampaignItemId(long campaignItemId) {
        this.campaignItemId = campaignItemId;
    }

    public long getClicks() {
        return Optional.ofNullable(clicks).orElse(0L);
    }

    public void setClicks(long clicks) {
        this.clicks = clicks;
    }

    public long getImps() {
        return Optional.ofNullable(imps).orElse(0L);
    }

    public void setImps(long imps) {
        this.imps = imps;
    }

    public long getBids() {
        return Optional.ofNullable(bids).orElse(0L);
    }

    public void setBids(long bids) {
        this.bids = bids;
    }

    public String getPeriod() {
        return period;
    }

    public void setPeriod(String period) {
        this.period = period;
    }
}
