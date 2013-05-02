package io.ifar.skidroad.writing;

import io.ifar.skidroad.rolling.FileRollingScheme;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A FileRollingScheme which rolls files when setNextRoll is called. Grace period extends from when file is rolled until
 * the next file is rolled.
*/
public class ManualRollingScheme implements FileRollingScheme {
    Path basePath;
    List<DateTime> rolls;

    public ManualRollingScheme() throws IOException {
        basePath = Files.createTempDirectory(ManualRollingScheme.class.getSimpleName());
        rolls = new ArrayList<>();
        rolls.add(DateTime.now(DateTimeZone.UTC));
    }

    public void setNextRoll(DateTime when) {
        rolls.add(when);
    }

    @Override
    public File getBaseDirectory() {
        return basePath.toFile();
    }

    /**
     * Anything that has been rolled is ready to close in this simple implementation
     */
    @Override
    public boolean isTimeToClose(DateTime startTime) {
        return startTime.getMillis() < getPenultimateRollMillis();
    }

    @Override
    public DateTime getCurrentStartTime() {
        return getLastRoll();
    }

    @Override
    public DateTime getStartTime(long currentTimeMillis) {
        //Walk chronological list of rolls until we find one that immediately preceded provided time.
        DateTime result = null;
        Iterator<DateTime> i = rolls.iterator();
        while (i.hasNext()) {
            DateTime roll = i.next();
            if (roll.getMillis() > currentTimeMillis)
                return result;
            result = roll;
        }
        return result;
    }

    @Override
    public String makeOutputPathPattern(DateTime startTime) {
        return getRepresentation(startTime);
    }

    @Override
    public String getRepresentation(DateTime startTime) {
        return Long.toString(startTime.getMillis());
    }

    private DateTime getLastRoll() {
        return rolls.isEmpty() ? null : rolls.get(rolls.size() - 1);
    }
    private DateTime getPenultimateRoll() {
        return (rolls.size() < 2) ? null : rolls.get(rolls.size() - 2);
    }
    private long getLastRollMillis() {
        DateTime lastRoll = getLastRoll();
        return lastRoll == null ? 0 : lastRoll.getMillis();
    }
    private long getPenultimateRollMillis() {
        DateTime penultimateRoll = getPenultimateRoll();
        return penultimateRoll == null ? 0 : penultimateRoll.getMillis();
    }
}
