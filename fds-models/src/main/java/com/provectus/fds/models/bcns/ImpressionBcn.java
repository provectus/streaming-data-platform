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
public class ImpressionBcn implements Partitioned {
    @JsonProperty("tx_id")
    private String txId;

    @JsonProperty("win_price")
    private long winPrice;

    public ImpressionBcn(
            @JsonProperty("tx_id") String txId,
            @JsonProperty("win_price") long winPrice) {
        this.txId = txId;
        this.winPrice = winPrice;
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