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
 * A FileRollingScheme which rolls files when setNextRoll is called. There is no grace period; anything which has been
 * rolled is eligible for close.
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
        return startTime.getMillis() < getLastRollMillis();
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
        if (rolls.isEmpty())
            return null;
        else
            return rolls.get(rolls.size() - 1);
    }
    private long getLastRollMillis() {
        DateTime lastRoll = getLastRoll();
        return lastRoll == null ? 0 : lastRoll.getMillis();
    }
}
