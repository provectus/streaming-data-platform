package com.provectus.fds.models.bcns;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.provectus.fds.models.utils.JsonUtils;

import java.io.IOException;

public class ClickBcn implements Bcn {
    private final String txid;
    private final long timestamp;
    private final String type = "click";

    @JsonCreator
    public ClickBcn(
              @JsonProperty("txid") String txid
            , @JsonProperty("timestamp") long timestamp
    ) {
        this.txid = txid;
        this.timestamp = timestamp;
    }

    public String getTxid() {
        return txid;
    }

    public long getTimestamp() {
        return timestamp;
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
