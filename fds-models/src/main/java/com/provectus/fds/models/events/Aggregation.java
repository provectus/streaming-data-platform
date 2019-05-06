package com.provectus.fds.models.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.util.Optional;

@Builder
@Setter
@NoArgsConstructor
@EqualsAndHashCode
@ToString
public class Aggregation {
    @JsonProperty("campaign_item_id")
    private long campaignItemId;
    private String period;
    private Long bids;
    private Long imps;
    private Long clicks;

    @JsonCreator
    public Aggregation(
            @JsonProperty("campaign_item_id") long campaignItemId,
            @JsonProperty("period") String period,
            @JsonProperty("bids") Long bids,
            @JsonProperty("imps") Long imps,
            @JsonProperty("clicks") Long clicks) {
        this.campaignItemId = campaignItemId;
        this.period = period;
        this.bids = bids;
        this.imps = imps;
        this.clicks = clicks;
    }

    public Long getCampaignItemId() {
        return campaignItemId;
    }

    public String getPeriod() {
        return period;
    }

    public Long getBids() {
        return Optional.ofNullable(bids).orElse(0L);
    }

    public Long getImps() {
        return Optional.ofNullable(imps).orElse(0L);
    }

    public Long getClicks() {
        return Optional.ofNullable(clicks).orElse(0L);
    }
}