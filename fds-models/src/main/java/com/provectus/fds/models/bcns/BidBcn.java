package com.provectus.fds.models.bcns;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.provectus.fds.models.utils.JsonUtils;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.IOException;
import java.util.StringJoiner;

@Builder
@Getter
@Setter
@NoArgsConstructor
public class BidBcn implements Partitioned {
    @JsonProperty("tx_id")
    private String txId;

    @JsonProperty("campaign_item_id")
    private long campaignItemId;

    @JsonProperty("creative_id")
    private String creativeId;

    @JsonProperty("creative_category")
    private String creativeCategory;

    @JsonProperty("app_uid")
    private String appUID;

    private String domain;

    public BidBcn(
            @JsonProperty("tx_id") String txId,
            @JsonProperty("campaign_item_id") long campaignItemId,
            @JsonProperty("creative_id") String creativeId,
            @JsonProperty("creative_category") String creativeCategory,
            @JsonProperty("app_uid") String appUID,
            @JsonProperty("domain") String domain) {
        this.txId = txId;
        this.campaignItemId = campaignItemId;
        this.creativeId = creativeId;
        this.creativeCategory = creativeCategory;
        this.appUID = appUID;
        this.domain = domain;
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
        if (!(o instanceof BidBcn)) return false;

        BidBcn bidBcn = (BidBcn) o;

        if (getCampaignItemId() != bidBcn.getCampaignItemId()) return false;
        if (!getTxId().equals(bidBcn.getTxId())) return false;
        if (!getCreativeId().equals(bidBcn.getCreativeId())) return false;
        if (!getDomain().equals(bidBcn.getDomain())) return false;
        if (!getCreativeCategory().equals(bidBcn.getCreativeCategory())) return false;
        return getAppUID().equals(bidBcn.getAppUID());
    }

    @Override
    public int hashCode() {
        int result = getTxId().hashCode();
        result = 31 * result + (int) (getCampaignItemId() ^ (getCampaignItemId() >>> 32));
        result = 31 * result + getCreativeId().hashCode();
        result = 31 * result + getDomain().hashCode();
        result = 31 * result + getCreativeCategory().hashCode();
        result = 31 * result + getAppUID().hashCode();
        return result;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", BidBcn.class.getSimpleName() + "[", "]")
                .add("txId='" + txId + "'")
                .add("campaignItemId=" + campaignItemId)
                .add("creativeId='" + creativeId + "'")
                .add("domain='" + domain + "'")
                .add("creativeCategory='" + creativeCategory + "'")
                .add("appUID='" + appUID + "'")
                .toString();
    }

    public static BidBcn from(Bcn bcn) {
        return BidBcn.builder()
                .txId(bcn.getTxId())
                .campaignItemId(bcn.getCampaignItemId())
                .creativeId(bcn.getCreativeId())
                .domain(bcn.getDomain())
                .creativeCategory(bcn.getCreativeCategory())
                .appUID(bcn.getAppUID())
                .build();
    }
}