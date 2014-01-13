package io.ifar.skidroad.rolling;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.ISODateTimeFormat;

import java.io.File;
import java.nio.file.Paths;

/**
 * Helper for implementing FileRollingScheme.
 */
public class BasicFileRollingScheme implements FileRollingScheme {

    private final static DateTimeFormatter DAILY_FORMATTER = ISODateTimeFormat.date();
    private final static DateTimeFormatter HOURLY_FORMATTER = ISODateTimeFormat.dateHour();
    private final static DateTimeFormatter MINUTELY_FORMATTER = new DateTimeFormatterBuilder()
            .append(ISODateTimeFormat.date())
            .appendLiteral('T')
            .append(ISODateTimeFormat.hour())
            .appendLiteral('-')
            .appendMinuteOfHour(2)
            .toFormatter();

    private final int secondsToWaitBeforeClosing;
    private final String basePath;
    private final String namePrefix;
    private final String nameSuffix;
    private final DateTimeFormatter startTimeFormatter;
    private final Duration duration;
    private final boolean minutes;
    private final boolean hours;
    private final boolean days;

    public BasicFileRollingScheme(String basePath, String namePrefix, String nameSuffix, int secondsToWaitBeforeClosing,
                                     Duration duration)
    {
        this.basePath = basePath;
        this.namePrefix = namePrefix;
        this.nameSuffix = nameSuffix;
        this.secondsToWaitBeforeClosing = secondsToWaitBeforeClosing;
        if (duration.getStandardDays() > 0) {
            days = true;
            hours = minutes = false;
            startTimeFormatter = DAILY_FORMATTER;
        } else if (duration.getStandardHours() > 0) {
            days = minutes = false;
            hours = true;
            startTimeFormatter = HOURLY_FORMATTER;
        } else if (duration.getStandardMinutes() > 0) {
            days = hours = false;
            minutes = true;
            startTimeFormatter = MINUTELY_FORMATTER;
        } else {
            throw new IllegalArgumentException("Durations should be in whole days, whole hours, or a number of minutes that evenly divides an hour.");
        }
        if ((days && ((duration.getStandardHours() % 24 != 0) || (duration.getStandardMinutes() % 60 != 0)))
                || (hours && (duration.getStandardDays() > 0 || (duration.getStandardMinutes() % 60 != 0)))
                || (minutes && ((duration.getStandardDays() > 0) || (duration.getStandardHours() > 0)))
                || (minutes && (60 != duration.getStandardMinutes() * (60 / duration.getStandardMinutes()))))
        {
            throw new IllegalArgumentException("Durations should be in whole days, whole hours, or a number of minutes that evenly divides an hour.");
        }
        this.duration = duration;
    }

    @Override
    public boolean isTimeToClose(DateTime startTime) {
        return secondsSinceEnd(startTime) > secondsToWaitBeforeClosing;
    }

    @Override
    public DateTime getCurrentStartTime() {
        return getStartTime(System.currentTimeMillis());
    }

    @Override
    public String getRepresentation(DateTime startTime) {
        return startTimeFormatter.print(startTime);
    }

    public String makeOutputPathPattern(DateTime startTime) {
        String fileNamePattern = namePrefix + getRepresentation(startTime) + "_%d" + nameSuffix;
        return Paths.get(basePath, fileNamePattern).toString();
    }

    @Override
    public File getBaseDirectory() {
        return new File(basePath);
    }

    public DateTime getStartTime(long currentTimeMillis) {
        DateTime rawDateTime = new DateTime(currentTimeMillis, DateTimeZone.UTC);

        if (days) {
            return new DateTime(
                    rawDateTime.getYear(),
                    rawDateTime.getMonthOfYear(),
                    rawDateTime.getDayOfMonth(),
                    0,
                    0,
                    0,
                    DateTimeZone.UTC);
        } else if (hours) {
            return new DateTime(
                    rawDateTime.getYear(),
                    rawDateTime.getMonthOfYear(),
                    rawDateTime.getDayOfMonth(),
                    rawDateTime.getHourOfDay(),
                    0,
                    DateTimeZone.UTC);
        } else if (minutes) {
            return new DateTime(
                    rawDateTime.getYear(),
                    rawDateTime.getMonthOfYear(),
                    rawDateTime.getDayOfMonth(),
                    rawDateTime.getHourOfDay(),
                    rawDateTime.getMinuteOfHour() - (rawDateTime.getMinuteOfHour() % (int) duration.getStandardMinutes()),
                    0,
                    DateTimeZone.UTC);
        } else {
            throw new IllegalStateException("Durations smaller than one minute are not supported.");
        }
    }

    public DateTime getSubsequentStartTime(DateTime startTime) {
        return startTime.plus(duration);
    }

    protected int secondsSinceEnd(DateTime startTime) {
        DateTime endsAt = getSubsequentStartTime(startTime);
        Duration duration = new Duration(endsAt, DateTime.now(DateTimeZone.UTC));
        return (int) duration.getStandardSeconds();
    }
}
