package com.provectus.fds.dynamodb.models;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.provectus.fds.dynamodb.ZoneDateTimeUtils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Optional;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Aggregation {
    private long campaignItemId = 0L;
    private long period = 0L;
    private Long clicks = 0L;
    private Long imps = 0L;
    private Long bids = 0L;

    public Aggregation() {
    }

    public Aggregation(long campaignItemId) {
        this.campaignItemId = campaignItemId;
    }

    public Aggregation(long campaignItemId, long period, Long clicks, Long imps, Long bids) {
        this.campaignItemId = campaignItemId;
        this.period = period;
        this.clicks = clicks;
        this.imps = imps;
        this.bids = bids;
    }

    @DynamoDBHashKey(attributeName="campaign_item_id")
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

    @DynamoDBRangeKey(attributeName = "period")
    public long getPeriod() {
        return period;
    }

    public void setPeriod(long period) {
        this.period = period;
    }

    public ZonedDateTime getDateTime(ZoneId zoneId) {
        return Instant.ofEpochSecond(this.period).atZone(zoneId);
    }

    public ZonedDateTime getDateTime(ZoneId zoneId, ChronoUnit unit) {
        return ZoneDateTimeUtils.truncatedTo(Instant.ofEpochSecond(this.period).atZone(zoneId), unit);
    }


    public Aggregation addAggregation(Aggregation other) {
        this.bids+=other.bids;
        this.clicks+=other.clicks;
        this.imps+=other.imps;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Aggregation that = (Aggregation) o;
        return campaignItemId == that.campaignItemId &&
                period == that.period &&
                Objects.equals(clicks, that.clicks) &&
                Objects.equals(imps, that.imps) &&
                Objects.equals(bids, that.bids);
    }

    @Override
    public int hashCode() {
        return Objects.hash(campaignItemId, period, clicks, imps, bids);
    }

    public Aggregation clone() {
        return new Aggregation(campaignItemId, period, clicks, imps, bids);
    }

    public Aggregation withPeriod(long period) {
        Aggregation that = this.clone();
        that.setPeriod(period);
        return that;
    }

    @Override
    public String toString() {
        return "Aggregation{" +
                "campaignItemId=" + campaignItemId +
                ", period=" + period +
                ", clicks=" + clicks +
                ", imps=" + imps +
                ", bids=" + bids +
                '}';
    }
}
