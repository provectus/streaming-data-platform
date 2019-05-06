package com.provectus.fds.models.bcns;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.provectus.fds.models.utils.JsonUtils;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.IOException;
import java.util.StringJoiner;

@Getter
@Builder
@Setter
@NoArgsConstructor
public class Bcn implements Partitioned {
    @JsonProperty("tx_id")
    private String txId;

    @JsonProperty("campaign_item_id")
    private long campaignItemId;

    private String domain;

    @JsonProperty("creative_id")
    private String creativeId;

    @JsonProperty("creative_category")
    private String creativeCategory;

    @JsonProperty("app_uid")
    private String appUID;

    @JsonProperty("win_price")
    private long winPrice;

    private String type;

    @JsonCreator
    public Bcn(
            @JsonProperty("tx_id") String txid,
            @JsonProperty("campaign_item_id") long campaignItemId,
            @JsonProperty("domain") String domain,
            @JsonProperty("creative_id") String creativeId,
            @JsonProperty("creative_category") String creativeCategory,
            @JsonProperty("app_uid") String appUID,
            @JsonProperty("win_price") long winPrice,
            @JsonProperty("type") String type) {
        this.txId = txid;
        this.campaignItemId = campaignItemId;
        this.domain = domain;
        this.creativeId = creativeId;
        this.creativeCategory = creativeCategory;
        this.appUID = appUID;
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Bcn)) return false;

        Bcn bcn = (Bcn) o;

        if (getCampaignItemId() != bcn.getCampaignItemId()) return false;
        if (getWinPrice() != bcn.getWinPrice()) return false;
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
        result = 31 * result + getType().hashCode();
        return result;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", Bcn.class.getSimpleName() + "[", "]")
                .add("txId='" + txId + "'")
                .add("campaignItemId=" + campaignItemId)
                .add("domain='" + domain + "'")
                .add("creativeId='" + creativeId + "'")
                .add("creativeCategory='" + creativeCategory + "'")
                .add("appUID='" + appUID + "'")
                .add("winPrice=" + winPrice)
                .add("type='" + type + "'")
                .toString();
    }
}