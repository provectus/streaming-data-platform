package com.provectus.fds.ingestion;

import com.provectus.fds.models.events.Location;
import com.provectus.fds.models.utils.JsonUtils;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class LocationsHandlerTest {
    @Test
    public void shouldParseLocation() {
        String row = "app1,1557705600,55.789609,49.124116";
        Location expected = getLocation();

        assertEquals(expected, Location.from(row.split(",")));
    }

    @Test
    public void shouldConvertLocation() throws IOException {
        Location expected = getLocation();

        assertEquals(expected, JsonUtils.read(JsonUtils.write(expected), Location.class));
    }

    private Location getLocation() {
        return Location.builder()
                .appUID("app1")
                .timestamp(1557705600)
                .longitude(55.789609)
                .latitude(49.124116)
                .build();
    }
}