package io.ifar.skidroad.rolling;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.ISODateTimeFormat;

public class MinutelyFileRollingScheme extends BasicFileRollingScheme {
    //ISODateTimeFormat.dateHourMinute yields yyyy-MM-dd'T'HH:mm and : makes for unhappy file names and URIs. Use - instead.
    private final static DateTimeFormatter FORMATTER = new DateTimeFormatterBuilder()
            .append(ISODateTimeFormat.date())
            .appendLiteral('T')
            .append(ISODateTimeFormat.hour())
            .appendLiteral('-')
            .appendMinuteOfHour(2)
            .toFormatter();

    public MinutelyFileRollingScheme(String basePath, String namePrefix, String nameSuffix, int secondsToWaitBeforeClosing) {
        super(basePath, namePrefix, nameSuffix, secondsToWaitBeforeClosing, FORMATTER);
    }

    public DateTime getStartTime(long currentTimeMillis) {
        DateTime rawDateTime = new DateTime(currentTimeMillis, DateTimeZone.UTC);
        return new DateTime(
                rawDateTime.getYear(),
                rawDateTime.getMonthOfYear(),
                rawDateTime.getDayOfMonth(),
                rawDateTime.getHourOfDay(),
                rawDateTime.getMinuteOfHour(),
                0,
                DateTimeZone.UTC);
    }

    public DateTime getSubsequentStartTime(DateTime startTime) {
        return startTime.plusMinutes(1);
    }
}
