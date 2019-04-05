package com.provectus.fds.models.events;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.provectus.fds.models.bcns.ClickBcn;
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
public class Click implements Partitioned {
    private Impression impression;

    public Click(@JsonProperty("impression") Impression impression) {
        this.impression = impression;
    }

    @Override
    public String getPartitionKey() {
        return impression.getBidBcn().getPartitionKey();
    }

    @Override
    public byte[] getBytes() throws IOException {
        return JsonUtils.write(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Click)) return false;

        Click click = (Click) o;

        return getImpression().equals(click.getImpression());
    }

    @Override
    public int hashCode() {
        return getImpression().hashCode();
    }

    public static Click from(Impression impression, ClickBcn clickBcn) {
        return Click.builder().impression(impression).build();
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", Click.class.getSimpleName() + "[", "]")
                .add("impression=" + impression)
                .toString();
    }
}