package io.ifar.skidroad.rolling;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class DailyFileRollingScheme extends BasicFileRollingScheme {
    private final static DateTimeFormatter FORMATTER = ISODateTimeFormat.date();

    public DailyFileRollingScheme(String basePath, String namePrefix, String nameSuffix, int secondsToWaitBeforeClosing) {
        super(basePath, namePrefix, nameSuffix, secondsToWaitBeforeClosing, FORMATTER);
    }

    public DateTime getStartTime(long currentTimeMillis) {
        DateTime rawDateTime = new DateTime(currentTimeMillis, DateTimeZone.UTC);
        return new DateTime(
                rawDateTime.getYear(),
                rawDateTime.getMonthOfYear(),
                rawDateTime.getDayOfMonth(),
                0,
                0,
                0,
                DateTimeZone.UTC);
    }

    public DateTime getSubsequentStartTime(DateTime startTime) {
        return startTime.plusDays(1);
    }
}
