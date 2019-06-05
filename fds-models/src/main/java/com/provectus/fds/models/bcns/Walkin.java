package com.provectus.fds.models.bcns;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.provectus.fds.models.events.Impression;
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
public class Walkin implements Partitioned {
    @JsonProperty("tx_id")
    private String txId;

    @JsonProperty("win_price")
    private long winPrice;

    @JsonProperty("app_uid")
    private String appUID;

    private long timestamp;
    private double longitude;
    private double latitude;

    public Walkin(
            @JsonProperty("tx_id") String txId,
            @JsonProperty("win_price") long winPrice,
            @JsonProperty("app_uid") String appUID,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("longitude") double longitude,
            @JsonProperty("latitude") double latitude) {
        this.txId = txId;
        this.winPrice = winPrice;
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

    public static Walkin from(Impression impression, Location location) {
        return builder()
                .txId(impression.getPartitionKey())
                .winPrice(impression.getImpressionBcn() == null ? 0 : impression.getImpressionBcn().getWinPrice())
                .appUID(location.getAppUID())
                .timestamp(location.getTimestamp())
                .longitude(location.getLongitude())
                .latitude(location.getLatitude())
                .build();
    }
}