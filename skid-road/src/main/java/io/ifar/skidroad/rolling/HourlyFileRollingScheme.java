package io.ifar.skidroad.rolling;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class HourlyFileRollingScheme extends BasicFileRollingScheme {
    private final static DateTimeFormatter FORMATTER = ISODateTimeFormat.dateHour();

    public HourlyFileRollingScheme(String basePath, String namePrefix, String nameSuffix, int secondsToWaitBeforeClosing) {
        super(basePath, namePrefix, nameSuffix, secondsToWaitBeforeClosing, FORMATTER);
    }

    public DateTime getStartTime(long currentTimeMillis) {
        DateTime rawDateTime = new DateTime(currentTimeMillis, DateTimeZone.UTC);
        return new DateTime(
                rawDateTime.getYear(),
                rawDateTime.getMonthOfYear(),
                rawDateTime.getDayOfMonth(),
                rawDateTime.getHourOfDay(),
                0,
                0,
                DateTimeZone.UTC);
    }

    public DateTime getSubsequentStartTime(DateTime startTime) {
        return startTime.plusHours(1);
    }
}
