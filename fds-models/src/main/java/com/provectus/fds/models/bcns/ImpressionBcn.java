package com.provectus.fds.models.bcns;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.provectus.fds.models.utils.JsonUtils;
import lombok.Builder;

import java.io.IOException;

@Builder
public class ImpressionBcn implements Partitioned {
    private final String txId;
    private final long winPrice;
    private final long timestamp;

    @JsonProperty("tx_id")
    public String getTxId() {
        return txId;
    }

    @JsonProperty("win_price")
    public long getWinPrice() {
        return winPrice;
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
        if (!(o instanceof ImpressionBcn)) return false;

        ImpressionBcn that = (ImpressionBcn) o;

        if (getWinPrice() != that.getWinPrice()) return false;
        if (getTimestamp() != that.getTimestamp()) return false;
        return getTxId().equals(that.getTxId());
    }

    @Override
    public int hashCode() {
        int result = getTxId().hashCode();
        result = 31 * result + (int) (getWinPrice() ^ (getWinPrice() >>> 32));
        result = 31 * result + (int) (getTimestamp() ^ (getTimestamp() >>> 32));
        return result;
    }

    public static ImpressionBcn from(Bcn bcn) {
        return ImpressionBcn.builder()
                .txId(bcn.getTxId())
                .winPrice(bcn.getWinPrice())
                .timestamp(bcn.getTimestamp())
                .build();
    }
}