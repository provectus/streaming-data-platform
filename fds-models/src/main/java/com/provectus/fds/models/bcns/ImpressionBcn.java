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

    public static ImpressionBcn from(Bcn bcn) {
        return ImpressionBcn.builder()
                .txId(bcn.getTxId())
                .winPrice(bcn.getWinPrice())
                .build();
    }
}