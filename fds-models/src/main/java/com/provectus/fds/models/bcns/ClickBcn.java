package com.provectus.fds.models.bcns;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.provectus.fds.models.utils.JsonUtils;
import lombok.Builder;

import java.io.IOException;

@Builder
public class ClickBcn implements Partitioned {
    private final String txId;
    private final long timestamp;

    @JsonProperty("tx_id")
    public String getTxId() {
        return txId;
    }

    @JsonProperty("timestamp")
    public long getTimestamp() {
        return timestamp;
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
        if (!(o instanceof ClickBcn)) return false;

        ClickBcn clickBcn = (ClickBcn) o;

        if (getTimestamp() != clickBcn.getTimestamp()) return false;
        return getTxId().equals(clickBcn.getTxId());
    }

    @Override
    public int hashCode() {
        int result = getTxId().hashCode();
        result = 31 * result + (int) (getTimestamp() ^ (getTimestamp() >>> 32));
        return result;
    }

    public static ClickBcn from(Bcn bcn) {
        return ClickBcn.builder()
                .txId(bcn.getTxId())
                .timestamp(bcn.getTimestamp())
                .build();
    }
}