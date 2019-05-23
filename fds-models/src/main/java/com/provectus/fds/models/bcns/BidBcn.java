package com.provectus.fds.models.bcns;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.provectus.fds.models.utils.JsonUtils;
import lombok.*;

import java.io.IOException;

@Builder
@Getter
@Setter
@NoArgsConstructor
@ToString
@EqualsAndHashCode
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