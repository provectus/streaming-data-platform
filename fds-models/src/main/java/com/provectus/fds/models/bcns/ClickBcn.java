package com.provectus.fds.models.bcns;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.provectus.fds.models.utils.JsonUtils;
import lombok.Builder;

import java.io.IOException;

@Builder
public class ClickBcn implements Partitioned {
    private final String txId;

    @JsonProperty("tx_id")
    public String getTxId() {
        return txId;
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