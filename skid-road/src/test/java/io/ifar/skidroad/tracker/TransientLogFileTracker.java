package io.ifar.skidroad.tracker;

import com.google.common.collect.ImmutableSet;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.AbstractLogFileTracker;
import io.ifar.skidroad.tracking.LogFileState;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.skife.jdbi.v2.ResultIterator;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.ifar.skidroad.tracking.LogFileState.WRITING;

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
    public ResultIterator<LogFile> findMine(LogFileState state) {
        return findMine(ImmutableSet.of(state));
    }

    @Override
    public ResultIterator<LogFile> findMine(Set<LogFileState> states) {
        List<LogFile> result = new LinkedList<>();
        for (LogFile logFile : logFiles) {
            if (states.contains(logFile.getState()) &&
                    (logFile.getOwnerURI().toASCIIString().equals(localUri.toASCIIString())))
                result.add(logFile);
        }
        final Iterator<LogFile> resultIterator = result.iterator();
        return new ResultIterator<LogFile>() {
            @Override
            public void close() {
                // no op
            }

            @Override
            public boolean hasNext() {
                return resultIterator.hasNext();
            }

            @Override
            public LogFile next() {
                return resultIterator.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public ResultIterator<LogFile> findMine(LogFileState state, DateTime start, DateTime end) {
        return findMine(ImmutableSet.of(state),start,end);
    }

    @Override
    public ResultIterator<LogFile> findMine(Set<LogFileState> states, DateTime start, DateTime end) {
        List<LogFile> results = new LinkedList<>();
        ResultIterator<LogFile> mine = findMine(states);
        while (mine.hasNext()) {
            LogFile lf = mine.next();
            if (lf.getUpdatedAt() != null && lf.getUpdatedAt().isBefore(end)
                    && (lf.getUpdatedAt().isAfter(start) || lf.getUpdatedAt().equals(start)))
            {
                results.add(lf);
            }
        }
        final Iterator<LogFile> resultIterator = results.iterator();
        return new ResultIterator<LogFile>() {
            @Override
            public void close() {
                // no op
            }

            @Override
            public boolean hasNext() {
                return resultIterator.hasNext();
            }

            @Override
            public LogFile next() {
                return resultIterator.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
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
