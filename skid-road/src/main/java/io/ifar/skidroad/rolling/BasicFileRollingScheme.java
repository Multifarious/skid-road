package io.ifar.skidroad.rolling;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;
import java.nio.file.Paths;

/**
 * Helper for implementing FileRollingScheme.
 */
abstract public class BasicFileRollingScheme implements FileRollingScheme {
    protected final int secondsToWaitBeforeClosing;
    protected final String basePath;
    protected final String namePrefix;
    protected final String nameSuffix;
    protected final DateTimeFormatter startTimeFormatter;

    protected BasicFileRollingScheme(String basePath, String namePrefix, String nameSuffix, int secondsToWaitBeforeClosing, DateTimeFormatter startTimeFormatter) {
        this.basePath = basePath;
        this.namePrefix = namePrefix;
        this.nameSuffix = nameSuffix;
        this.secondsToWaitBeforeClosing = secondsToWaitBeforeClosing;
        this.startTimeFormatter = startTimeFormatter;
    }

    @Override
    public boolean isTimeToClose(DateTime startTime) {
        return secondsSinceEnd(startTime) > secondsToWaitBeforeClosing;
    }

    @Override
    public DateTime getCurrentStartTime() {
        return getStartTime(System.currentTimeMillis());
    }

    abstract public DateTime getSubsequentStartTime(DateTime startTime);

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

    protected int secondsSinceEnd(DateTime startTime) {
        DateTime endsAt = getSubsequentStartTime(startTime);
        Duration duration = new Duration(endsAt, DateTime.now(DateTimeZone.UTC));
        return (int) duration.getStandardSeconds();
    }
}
