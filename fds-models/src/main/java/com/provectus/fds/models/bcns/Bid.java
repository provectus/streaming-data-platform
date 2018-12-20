package com.provectus.fds.models.bcns;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.provectus.fds.models.utils.JsonUtils;

import java.io.IOException;

public class Bid implements Bcn {
    private final String txid;
    private final long campaignItemId;
    private final String domain;
    private final String creativeId;
    private final String creativeCategory;
    private final String appuid;
    private final String type = "bid";

    public Bid(String txid, long campaignItemId, String domain, String creativeId, String creativeCategory, String appuid) {
        this.txid = txid;
        this.campaignItemId = campaignItemId;
        this.domain = domain;
        this.creativeId = creativeId;
        this.creativeCategory = creativeCategory;
        this.appuid = appuid;
    }

    public String getTxid() {
        return txid;
    }

    @JsonProperty("campaign_item_id")
    public long getCampaignItemId() {
        return campaignItemId;
    }

    public String getDomain() {
        return domain;
    }

    @JsonProperty("creative_id")
    public String getCreativeId() {
        return creativeId;
    }

    @JsonProperty("creative_category")
    public String getCreativeCategory() {
        return creativeCategory;
    }

    public String getAppuid() {
        return appuid;
    }

    @Override
    public String getPartitionKey() {
        return txid;
    }

    @Override
    public byte[] getBytes() throws IOException {
        return JsonUtils.write(this);
    }

    public String getType() {
        return type;
    }
}
