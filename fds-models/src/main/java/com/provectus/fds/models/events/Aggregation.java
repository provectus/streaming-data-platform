package com.provectus.fds.models.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.util.Optional;
import java.util.StringJoiner;

@Builder
@Setter
@NoArgsConstructor
@EqualsAndHashCode
@ToString
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

    public static Aggregation reduce(Aggregation left, Aggregation right) {
        if (left == null || right == null) {
            throw new IllegalStateException("Arguments should not be null");
        }

        if (left.campaignItemId != right.campaignItemId) {
            throw new IllegalStateException(String.format("Can't reduce aggregations for different campaigns: %d and %d",
                    left.campaignItemId, right.campaignItemId));
        }

        if (left.period != null && !left.period.equals(right.period)) {
            throw new IllegalStateException(String.format("Can't reduce aggregations for different periods: %s and %s",
                    left.period, right.period));
        }

        return new AggregationBuilder()
                .campaignItemId(left.campaignItemId)
                .period(left.period)
                .bids(left.bids + right.bids)
                .imps(left.imps + right.imps)
                .clicks(left.clicks + right.clicks)
                .build();
    }
}