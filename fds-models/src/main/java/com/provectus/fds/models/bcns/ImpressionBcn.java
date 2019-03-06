package com.provectus.fds.models.bcns;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.provectus.fds.models.utils.JsonUtils;
import lombok.Builder;

import java.io.IOException;
import java.util.StringJoiner;

@Builder
public class ImpressionBcn implements Partitioned {
    private final String txId;
    private final long winPrice;

    @JsonProperty("tx_id")
    public String getTxId() {
        return txId;
    }

    @JsonProperty("win_price")
    public long getWinPrice() {
        return winPrice;
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
        if (!(o instanceof ImpressionBcn)) return false;

        ImpressionBcn that = (ImpressionBcn) o;

        if (getWinPrice() != that.getWinPrice()) return false;
        return getTxId().equals(that.getTxId());
    }

    @Override
    public int hashCode() {
        int result = getTxId().hashCode();
        result = 31 * result + (int) (getWinPrice() ^ (getWinPrice() >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ImpressionBcn.class.getSimpleName() + "[", "]")
                .add("txId='" + txId + "'")
                .add("winPrice=" + winPrice)
                .toString();
    }

    public static ImpressionBcn from(Bcn bcn) {
        return ImpressionBcn.builder()
                .txId(bcn.getTxId())
                .winPrice(bcn.getWinPrice())
                .build();
    }
}