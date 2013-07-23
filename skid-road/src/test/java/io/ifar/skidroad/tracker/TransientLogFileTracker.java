package io.ifar.skidroad.tracker;

import com.google.common.collect.ImmutableSet;
import io.ifar.goodies.AutoCloseableIterator;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.AbstractLogFileTracker;
import io.ifar.skidroad.tracking.LogFileState;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static io.ifar.skidroad.tracking.LogFileState.*;

/**
 * A simplistic LogFileTracker that merely generates LogFile records with unique serial numbers and does not actually
 * track anything.
 */
public class TransientLogFileTracker extends AbstractLogFileTracker {
    private AtomicInteger counter;
    private List<LogFile> logFiles;

    public TransientLogFileTracker() throws URISyntaxException {
        super(URI.create("http://127.0.0.1/" + TransientLogFileTracker.class.getSimpleName()));
        counter = new AtomicInteger(0);
        logFiles = new ArrayList<>();
    }

    public LogFile open(String rollingCohort, String pathPattern, DateTime startTime) {
        int serial = counter.incrementAndGet();
        Path originPath = Paths.get(String.format(pathPattern, serial));
        LogFile result = new LogFile(
                rollingCohort,
                serial,
                startTime,
                originPath,
                null,
                null,
                null,
                null,
                WRITING,
                localUri,
                null,
                DateTime.now(DateTimeZone.UTC),
                null
                );
        logFiles.add(result);
        return result;
    }

    @Override
    public AutoCloseableIterator<LogFile> findMine(LogFileState state) {
        return findMine(ImmutableSet.of(state));
    }

    @Override
    public AutoCloseableIterator<LogFile> findMine(Set<LogFileState> states) {
        List<LogFile> result = new LinkedList<>();
        for (LogFile logFile : logFiles)
            if (states.contains(logFile.getState()) && logFile.getOwnerURI().equals(localUri))
                result.add(logFile);

        return new AutoCloseableIterator<>(result.iterator());
    }

    @Override
    public LogFile findByRollingCohortAndSerial(String rollingCohort, int serial) {
        return null;
    }

    @Override
    protected int recordStateChange(LogFile logFile) {
        return 1;
    }

    @Override
    public int updatePrepPath(LogFile logFile) {
        return 1;
    }

    @Override
    public int updateArchiveKey(LogFile logFile) {
        return 1;
    }

    @Override
    public int updateArchiveLocation(LogFile logFile) {
        return 1;
    }

    @Override
    protected int updateSize(LogFile logFile) {
        return 1;
    }

    @Override
    public int getCount(LogFileState state) {
        return 0;
    }

    @Override
    public int getCount(Set<LogFileState> states) {
        return 0;
    }
}
