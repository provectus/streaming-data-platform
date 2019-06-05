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
public class ClickBcn implements Partitioned {
    @JsonProperty("tx_id")
    private String txId;

    public ClickBcn(@JsonProperty("tx_id") String txId) {
        this.txId = txId;
    }

    @Override
    public String getPartitionKey() {
        return txId;
    }

    @Override
    public byte[] getBytes() throws IOException {
        return JsonUtils.write(this);
    }

    public static ClickBcn from(Bcn bcn) {
        return ClickBcn.builder()
                .txId(bcn.getTxId())
                .build();
    }
}