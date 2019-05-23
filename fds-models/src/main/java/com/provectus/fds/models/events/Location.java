package com.provectus.fds.models.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Builder
@Setter
@Getter
@NoArgsConstructor
@EqualsAndHashCode
@ToString
public class Location {
    private static final int FIELDS_COUNT = 4;

    @JsonProperty("app_uid")
    private String appUID;

    private long timestamp;
    private double longitude;
    private double latitude;

    @JsonCreator
    public Location(
            @JsonProperty("app_uid") String appUID,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("longitude") double longitude,
            @JsonProperty("latitude") double latitude) {
        this.appUID = appUID;
        this.timestamp = timestamp;
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public static Location from(String[] elements) {
        if (elements == null || elements.length == 0) {
            throw new IllegalArgumentException("No elements");
        }

        if (elements.length != 4) {
            throw new IllegalArgumentException(String.format("Wrong format. Fields count: %s. Expected: %s",
                    elements.length, FIELDS_COUNT));
        }

        return Location.builder()
                .appUID(elements[0])
                .timestamp(Long.parseLong(elements[1]))
                .longitude(Double.parseDouble(elements[2]))
                .latitude(Double.parseDouble(elements[3]))
                .build();
    }
}