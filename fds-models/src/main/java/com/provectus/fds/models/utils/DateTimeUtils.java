package com.provectus.fds.models.utils;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.Locale;

import static java.time.temporal.ChronoField.*;

public class DateTimeUtils {
    public static final DateTimeFormatter AWS_DATE_TIME =
            new DateTimeFormatterBuilder().appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
                    .appendLiteral('-')
                    .appendValue(MONTH_OF_YEAR, 2)
                    .appendLiteral('-')
                    .appendValue(DAY_OF_MONTH, 2)
                    .appendLiteral(' ')
                    .appendValue(HOUR_OF_DAY, 2)
                    .appendLiteral(':')
                    .appendValue(MINUTE_OF_HOUR, 2)
                    .appendLiteral(':')
                    .appendValue(SECOND_OF_MINUTE, 2)
                    .optionalStart()
                    .appendLiteral('.')
                    .appendValue(MILLI_OF_SECOND, 3)
                    .optionalEnd()
                    .toFormatter(Locale.ENGLISH)
                    .withZone(ZoneOffset.UTC);

    public static String format(long epochMillis) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC).format(AWS_DATE_TIME);
    }

    public static long truncate(long epochMillis, long period) {
        //return shiftBack * (epochMillis / shiftBack);
        return epochMillis - (epochMillis % period);
    }
}