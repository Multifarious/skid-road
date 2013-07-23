package io.ifar.skidroad.jdbi;

import com.google.common.collect.ImmutableSet;
import io.ifar.goodies.AutoCloseableIterator;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.AbstractLogFileTracker;
import io.ifar.skidroad.tracking.LogFileState;
import io.ifar.skidroad.tracking.LogFileStateListener;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;

import static io.ifar.skidroad.tracking.LogFileState.*;

/**
 * A JDBI-based implementation of LogFileTracker that stores LogFile state is in a database table.
 *
 * TODO (future): generate audit history as well in log_file_audit table.
 */
public class JDBILogFileTracker extends AbstractLogFileTracker {
    private final static Logger LOG = LoggerFactory.getLogger(JDBILogFileTracker.class);
    private final JDBILogFileDAO dao;
    private final Set<LogFileStateListener> listeners;

    public JDBILogFileTracker(URI localUri, JDBILogFileDAO dao) {
        super(localUri);
        this.dao = dao;
        this.listeners = new HashSet<>();
    }

    @Override
    public LogFile open(String rollingCohort, String pathPattern, DateTime startTime) {
        while(true) {
            Timestamp stamp = now();
            int serial = dao.determineNextSerial(rollingCohort) + 1;
            Path originPath = Paths.get(String.format(pathPattern, serial));
            int rows = dao.claimIndex(rollingCohort, serial, new Timestamp(startTime.getMillis()), originPath.toUri().toString(), localUri.toString(), stamp);
            if (rows == 1) {
                return new LogFile(rollingCohort, serial, startTime, originPath, null, null, null, null, WRITING, localUri, null, new DateTime(stamp.getTime()), null);
            } else {
                LOG.debug("Another instance claimed {} serial {}. Will retry.", rollingCohort, serial);
            }
        }
    }

    @Override
    protected int recordStateChange(LogFile logFile) {
        return dao.updateState(logFile.getRollingCohort(), logFile.getSerial(), logFile.getState().toString(), localUri.toString(), now());
    }

    @Override
    public int updatePrepPath(LogFile logFile) {
        int rows = dao.updatePrepPath(logFile.getRollingCohort(), logFile.getSerial(), logFile.getPrepPath().toUri().toString(), localUri.toString(), now());
        logIfBadRowCount(rows, logFile, "set prep path");
        return rows;
    }

    @Override
    public int updateArchiveKey(LogFile logFile) {
        int rows = dao.updateArchiveKey(logFile.getRollingCohort(), logFile.getSerial(),
                logFile.getArchiveKey(), localUri.toString(), now());
        logIfBadRowCount(rows, logFile, "set archive key");
        return rows;
    }

    @Override
    public int updateArchiveLocation(LogFile logFile) {
        int rows = dao.updateArchiveLocation(logFile.getRollingCohort(), logFile.getSerial(),
                logFile.getArchiveGroup(), logFile.getArchiveURI().toString(), localUri.toString(), now());
        logIfBadRowCount(rows, logFile, "set archive location");
        return rows;
    }

    private void logIfBadRowCount(int rows, LogFile logFile, String actionDescription) {
        switch (rows) {
            case 1:
                //happy case
                break;
            case 0:
                LOG.warn("{} not found or no longer owned by {}; cannot {}.", logFile, localUri, actionDescription);
                break;
            default:
                LOG.error("Database corruption! Rolling cohort and serial for {} should be unique, but {} rows affected trying to {}.", logFile, rows, actionDescription);
        }
    }

    @Override
    public AutoCloseableIterator<LogFile> findMine(LogFileState state) {
        return findMine(ImmutableSet.of(state));
    }

    @Override
    public AutoCloseableIterator<LogFile> findMine(Set<LogFileState> states) {
        return JDBILogFileDAOHelper.findByOwnerAndState(dao, localUri, states);
    }

    @Override
    protected int updateSize(LogFile logFile) {
        return dao.updateSize(logFile.getRollingCohort(), logFile.getSerial(), logFile.getByteSize(), localUri.toString(), now());
    }

    @Override
    public int getCount(LogFileState state) {
        return dao.count(state.toString());
    }

    @Override
    public int getCount(Set<LogFileState> states) {
        Set<String> stateStrings = new HashSet<>(states.size());
        for (LogFileState state : states) {
            stateStrings.add(state.toString());
        }
        return JDBILogFileDAOHelper.count(dao, stateStrings);
    }

    @Override
    public LogFile findByRollingCohortAndSerial(String rollingCohort, int serial) {
        return dao.findByRollingCohortAndSerial(rollingCohort, serial);
    }

    protected Timestamp now() {
        return new Timestamp(System.currentTimeMillis());
    }
}
