package com.provectus.fds.models.events;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Aggregation {
    @JsonProperty("campaign_item_id")
    private long campaignItemId;
    private String period;
    private long clicks;
    private long imps;
    private long bids;

    public long getCampaignItemId() {
        return campaignItemId;
    }

    public void setCampaignItemId(long campaignItemId) {
        this.campaignItemId = campaignItemId;
    }

    public long getClicks() {
        return clicks;
    }

    public void setClicks(long clicks) {
        this.clicks = clicks;
    }

    public long getImps() {
        return imps;
    }

    public void setImps(long imps) {
        this.imps = imps;
    }

    public long getBids() {
        return bids;
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
