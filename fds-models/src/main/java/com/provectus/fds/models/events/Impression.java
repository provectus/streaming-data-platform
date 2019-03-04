package com.provectus.fds.models.events;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.provectus.fds.models.bcns.BidBcn;
import com.provectus.fds.models.bcns.ImpressionBcn;
import com.provectus.fds.models.bcns.Partitioned;
import com.provectus.fds.models.utils.JsonUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.io.IOException;


@Builder
@AllArgsConstructor
public class Impression implements Partitioned {
    private final BidBcn bidBcn;
    private final ImpressionBcn impressionBcn;

    @JsonProperty("bid_bcn")
    public BidBcn getBidBcn() {
        return bidBcn;
    }

    @JsonProperty("impression_bcn")
    public ImpressionBcn getImpressionBcn() {
        return impressionBcn;
    }

    @Override
    public String getPartitionKey() {
        return bidBcn.getPartitionKey();
    }

    @Override
    public byte[] getBytes() throws IOException {
        return JsonUtils.write(this);
    }

    @Override
    public long extractTimestamp() {
        return bidBcn.getTimestamp();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Impression)) return false;

        Impression that = (Impression) o;

        if (!getBidBcn().equals(that.getBidBcn())) return false;
        return getImpressionBcn().equals(that.getImpressionBcn());
    }

    @Override
    public int hashCode() {
        int result = getBidBcn().hashCode();
        result = 31 * result + getImpressionBcn().hashCode();
        return result;
    }

    public static Impression from(BidBcn bidBcn, ImpressionBcn impressionBcn) {
        return Impression.builder()
                .bidBcn(bidBcn)
                .impressionBcn(impressionBcn)
                .build();
    }
}