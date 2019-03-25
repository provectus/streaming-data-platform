package com.provectus.fds.models.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Setter;

import java.util.Optional;
import java.util.StringJoiner;

@Setter
@Builder
public class Aggregation {
    @JsonProperty("campaign_item_id")
    private long campaignItemId;
    private String period;
    private Long clicks;
    private Long imps;
    private Long bids;

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

    public long getCampaignItemId() {
        return campaignItemId;
    }

    public long getClicks() {
        return Optional.ofNullable(clicks).orElse(0L);
    }

    public long getImps() {
        return Optional.ofNullable(imps).orElse(0L);
    }

    public long getBids() {
        return Optional.ofNullable(bids).orElse(0L);
    }

    public String getPeriod() {
        return period;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Aggregation)) return false;

        Aggregation that = (Aggregation) o;

        if (getCampaignItemId() != that.getCampaignItemId()) return false;
        if (getClicks() != that.getClicks()) return false;
        if (getImps() != that.getImps()) return false;
        if (getBids() != that.getBids()) return false;
        return getPeriod().equals(that.getPeriod());
    }

    @Override
    public int hashCode() {
        int result = (int) (getCampaignItemId() ^ (getCampaignItemId() >>> 32));
        result = 31 * result + getPeriod().hashCode();
        result = 31 * result + (int) (getClicks() ^ (getClicks() >>> 32));
        result = 31 * result + (int) (getImps() ^ (getImps() >>> 32));
        result = 31 * result + (int) (getBids() ^ (getBids() >>> 32));
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