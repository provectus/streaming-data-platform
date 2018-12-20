package com.provectus.fds.models.bcns;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.provectus.fds.models.utils.JsonUtils;

import java.io.IOException;

public class ImpressionBcn implements Bcn {
    private final String txid;
    private final long timestamp;
    private final long winPrice;
    private final String type = "imp";

    @JsonCreator
    public ImpressionBcn(
            @JsonProperty("txid") String txid,
            @JsonProperty("timestamp") Long timestamp,
            @JsonProperty("win_price") Long winPrice) {
        this.txid = txid;
        this.timestamp = timestamp;
        this.winPrice = winPrice;
    }

    @JsonProperty("txid")
    public String getTxid() {
        return txid;
    }

    @JsonProperty("timestamp")
    public long getTimestamp() {
        return timestamp;
    }

    @JsonProperty("win_price")
    public long getWinPrice() {
        return winPrice;
    }

    public String getType() {
        return type;
    }

    @Override
    public String getPartitionKey() {
        return txid;
    }

    @Override
    public byte[] getBytes() throws IOException {
        return JsonUtils.write(this);
    }
}
