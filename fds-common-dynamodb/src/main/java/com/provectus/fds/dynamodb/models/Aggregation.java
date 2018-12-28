package com.provectus.fds.dynamodb.models;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.provectus.fds.dynamodb.ZoneDateTimeUtils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

public class Aggregation {
    private long campaignItemId;
    private long period;
    private Long clicks;
    private Long imps;
    private Long bids;

    public Aggregation(long campaignItemId) {
        this.campaignItemId = campaignItemId;
        this.period = 0L;
        this.imps = 0L;
        this.clicks = 0L;
        this.bids = 0L;
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


    public void addAggregation(Aggregation other) {
        this.bids+=other.bids;
        this.clicks+=other.clicks;
        this.imps+=other.imps;
    }
}
