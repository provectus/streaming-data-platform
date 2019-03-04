package com.provectus.fds.models.bcns;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.provectus.fds.models.utils.JsonUtils;
import lombok.Builder;
import lombok.Getter;

import java.io.IOException;

@Getter
@Builder
public class Bcn implements Partitioned {
    private final String txId;
    private final long campaignItemId;
    private final String domain;
    private final String creativeId;
    private final String creativeCategory;
    private final String appUID;
    private final long winPrice;
    private final long timestamp;
    private final String type;

    @JsonCreator
    public Bcn(
            @JsonProperty("tx_id") String txid,
            @JsonProperty("campaign_item_id") long campaignItemId,
            @JsonProperty("domain") String domain,
            @JsonProperty("creative_id") String creativeId,
            @JsonProperty("creative_category") String creativeCategory,
            @JsonProperty("app_uid") String appUID,
            @JsonProperty("win_price") long winPrice,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("type") String type) {
        this.txId = txid;
        this.campaignItemId = campaignItemId;
        this.domain = domain;
        this.creativeId = creativeId;
        this.creativeCategory = creativeCategory;
        this.appUID = appUID;
        this.timestamp = timestamp;
        this.winPrice = winPrice;
        this.type = type;
    }

    @Override
    public String getPartitionKey() {
        return txId;
    }

    @Override
    public byte[] getBytes() throws IOException {
        return JsonUtils.write(this);
    }

    @Override
    public long extractTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Bcn)) return false;

        Bcn bcn = (Bcn) o;

        if (getCampaignItemId() != bcn.getCampaignItemId()) return false;
        if (getWinPrice() != bcn.getWinPrice()) return false;
        if (getTimestamp() != bcn.getTimestamp()) return false;
        if (!getTxId().equals(bcn.getTxId())) return false;
        if (!getDomain().equals(bcn.getDomain())) return false;
        if (!getCreativeId().equals(bcn.getCreativeId())) return false;
        if (!getCreativeCategory().equals(bcn.getCreativeCategory())) return false;
        if (!getAppUID().equals(bcn.getAppUID())) return false;
        return getType().equals(bcn.getType());
    }

    @Override
    public int hashCode() {
        int result = getTxId().hashCode();
        result = 31 * result + (int) (getCampaignItemId() ^ (getCampaignItemId() >>> 32));
        result = 31 * result + getDomain().hashCode();
        result = 31 * result + getCreativeId().hashCode();
        result = 31 * result + getCreativeCategory().hashCode();
        result = 31 * result + getAppUID().hashCode();
        result = 31 * result + (int) (getWinPrice() ^ (getWinPrice() >>> 32));
        result = 31 * result + (int) (getTimestamp() ^ (getTimestamp() >>> 32));
        result = 31 * result + getType().hashCode();
        return result;
    }
}