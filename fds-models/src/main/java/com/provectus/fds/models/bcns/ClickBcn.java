package com.provectus.fds.models.bcns;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.provectus.fds.models.utils.JsonUtils;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.IOException;

@Builder
@Getter
@Setter
@NoArgsConstructor
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ClickBcn)) return false;

        ClickBcn clickBcn = (ClickBcn) o;

        return getTxId().equals(clickBcn.getTxId());
    }

    @Override
    public int hashCode() {
        return getTxId().hashCode();
    }

    public static ClickBcn from(Bcn bcn) {
        return ClickBcn.builder()
                .txId(bcn.getTxId())
                .build();
    }
}