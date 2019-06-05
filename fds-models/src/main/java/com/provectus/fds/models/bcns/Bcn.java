package com.provectus.fds.models.bcns;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.provectus.fds.models.utils.JsonUtils;
import lombok.*;

import java.io.IOException;

@Getter
@Builder
@Setter
@NoArgsConstructor
@ToString
@EqualsAndHashCode
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
}