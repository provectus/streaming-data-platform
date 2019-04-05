package com.provectus.fds.models.events;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.provectus.fds.models.bcns.BidBcn;
import com.provectus.fds.models.bcns.ImpressionBcn;
import com.provectus.fds.models.bcns.Partitioned;
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
public class Impression implements Partitioned {
    @JsonProperty("bid_bcn")
    private BidBcn bidBcn;

    @JsonProperty("impression_bcn")
    private ImpressionBcn impressionBcn;

    public Impression(
            @JsonProperty("bid_bcn") BidBcn bidBcn,
            @JsonProperty("impression_bcn") ImpressionBcn impressionBcn) {
        this.bidBcn = bidBcn;
        this.impressionBcn = impressionBcn;
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

    @Override
    public String toString() {
        return new StringJoiner(", ", Impression.class.getSimpleName() + "[", "]")
                .add("bidBcn=" + bidBcn)
                .add("impressionBcn=" + impressionBcn)
                .toString();
    }
}