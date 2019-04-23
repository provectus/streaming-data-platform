package com.provectus.fds.models.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.util.Optional;
import java.util.StringJoiner;

public class Aggregation {
    @JsonProperty("campaign_item_id")
    private long campaignItemId;
    private String period;
    private Long clicks;
    private Long imps;
    private Long bids;

    public Aggregation() {
    }

    @JsonCreator
    public Aggregation(
            @JsonProperty("campaign_item_id") long campaignItemId,
            @JsonProperty("period") String period,
            @JsonProperty("clicks") Long clicks,
            @JsonProperty("imps") Long imps,
            @JsonProperty("bids") Long bids) {
        this.campaignItemId = campaignItemId;
        this.period = period;
        this.clicks = clicks;
        this.imps = imps;
        this.bids = bids;
    }

    public Long getCampaignItemId() {
        return campaignItemId;
    }

    public String getPeriod() {
        return period;
    }

    public Long getClicks() {
        return Optional.ofNullable(clicks).orElse(0L);
    }

    public Long getImps() {
        return Optional.ofNullable(imps).orElse(0L);
    }

    public Long getBids() {
        return Optional.ofNullable(bids).orElse(0L);
    }

    public Aggregation setCampaignItemId(long campaignItemId) {
        this.campaignItemId = campaignItemId;
        return this;
    }

    public Aggregation setPeriod(String period) {
        this.period = period;
        return this;
    }

    public Aggregation setClicks(Long clicks) {
        this.clicks = clicks;
        return this;
    }

    public Aggregation setImps(Long imps) {
        this.imps = imps;
        return this;
    }

    public Aggregation setBids(Long bids) {
        this.bids = bids;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Aggregation)) return false;

        Aggregation that = (Aggregation) o;

        if (getCampaignItemId() != that.getCampaignItemId()) return false;
        if (getPeriod() != null ? !getPeriod().equals(that.getPeriod()) : that.getPeriod() != null) return false;
        if (getClicks() != null ? !getClicks().equals(that.getClicks()) : that.getClicks() != null) return false;
        if (getImps() != null ? !getImps().equals(that.getImps()) : that.getImps() != null) return false;
        return getBids() != null ? getBids().equals(that.getBids()) : that.getBids() == null;
    }

    @Override
    public int hashCode() {
        int result = (int) (getCampaignItemId() ^ (getCampaignItemId() >>> 32));
        result = 31 * result + (getPeriod() != null ? getPeriod().hashCode() : 0);
        result = 31 * result + (getClicks() != null ? getClicks().hashCode() : 0);
        result = 31 * result + (getImps() != null ? getImps().hashCode() : 0);
        result = 31 * result + (getBids() != null ? getBids().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", Aggregation.class.getSimpleName() + "[", "]")
                .add("campaignItemId=" + campaignItemId)
                .add("period='" + period + "'")
                .add("clicks=" + clicks)
                .add("imps=" + imps)
                .add("bids=" + bids)
                .toString();
    }
}