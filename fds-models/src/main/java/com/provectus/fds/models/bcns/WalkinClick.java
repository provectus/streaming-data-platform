package com.provectus.fds.models.bcns;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.provectus.fds.models.events.Click;
import com.provectus.fds.models.events.Location;
import com.provectus.fds.models.utils.JsonUtils;
import lombok.*;

import java.io.IOException;

@Builder
@Getter
@Setter
@NoArgsConstructor
@ToString
@EqualsAndHashCode
public class WalkinClick implements Partitioned {
    @JsonProperty("tx_id")
    private String txId;

    @JsonProperty("app_uid")
    private String appUID;

    private long timestamp;
    private double longitude;
    private double latitude;

    public WalkinClick(
            @JsonProperty("tx_id") String txId,
            @JsonProperty("app_uid") String appUID,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("longitude") double longitude,
            @JsonProperty("latitude") double latitude) {
        this.txId = txId;
        this.appUID = appUID;
        this.timestamp = timestamp;
        this.longitude = longitude;
        this.latitude = latitude;
    }

    @Override
    public String getPartitionKey() {
        return txId;
    }

    @Override
    public byte[] getBytes() throws IOException {
        return JsonUtils.write(this);
    }

    public static WalkinClick from(Click click, Location location) {
        return builder()
                .txId(click.getPartitionKey())
                .appUID(location.getAppUID())
                .timestamp(location.getTimestamp())
                .longitude(location.getLongitude())
                .latitude(location.getLatitude())
                .build();
    }
}