package com.provectus.fds.compaction.utils;

import avro.shaded.com.google.common.collect.ImmutableMap;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Period {
    private final int year;
    private final int day;

    public Period(int year, int day) {
        this.year = year;
        this.day = day;
    }


    public String path() {
        return String.format("year=%d/day=%d", year,day);
    }

    public List<Map.Entry<String,String>> toList() {
        return Arrays.asList(
                new AbstractMap.SimpleEntry<>("year", Integer.toString(year)),
                new AbstractMap.SimpleEntry<>("day", Integer.toString(day))
        );
    }

    public static Period fromJsonPath(String path) {
        String[] parts = path.split("/");
        if (parts.length>=4) {
            int year = Integer.parseInt(parts[1]);
            int month = Integer.parseInt(parts[2]);
            int dayOfMonth = Integer.parseInt(parts[3]);
            LocalDateTime ldt = LocalDateTime.of(year, month, dayOfMonth, 0, 0);
            int unixday = (int)((ldt.toInstant(ZoneOffset.UTC).getEpochSecond() / (24 * 60 * 60)));
            return new Period(year, unixday);
        } else {
            throw new IllegalArgumentException("Invalid path");
        }
    }
}
