package com.provectus.fds.dynamodb;

import org.junit.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import static org.junit.Assert.assertEquals;

public class ZoneDateTimeUtilsTest {
    @Test
    public void convert() {
        ZonedDateTime zdt =
                ZoneDateTimeUtils.truncatedTo(Instant.ofEpochSecond(1546041600).atZone(ZoneOffset.UTC), ChronoUnit.DAYS);

        assertEquals(1546041600, zdt.toInstant().getEpochSecond());
    }

}