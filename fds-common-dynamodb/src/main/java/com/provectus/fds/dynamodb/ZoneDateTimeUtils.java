package com.provectus.fds.dynamodb;

import java.time.DayOfWeek;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

public class ZoneDateTimeUtils {
    public static ZonedDateTime truncatedTo(ZonedDateTime zonedDateTime, ChronoUnit chronoUnit) {
        switch (chronoUnit) {
            case WEEKS: return zonedDateTime.truncatedTo(ChronoUnit.DAYS).with(DayOfWeek.MONDAY);
            case MONTHS: return zonedDateTime.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1);
            case YEARS:  return zonedDateTime.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1).withMonth(1);
            default: return zonedDateTime.truncatedTo(chronoUnit);
        }
    }
}
