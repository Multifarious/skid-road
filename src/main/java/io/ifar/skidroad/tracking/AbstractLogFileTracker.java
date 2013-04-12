package io.ifar.skidroad.tracking;

import io.ifar.skidroad.LogFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.ifar.skidroad.tracking.LogFileState.*;

/**
 * Handles listener interaction and runs all state changes though a single method.
 */
public abstract class AbstractLogFileTracker implements LogFileTracker {
    private final static Logger LOG = LoggerFactory.getLogger(AbstractLogFileTracker.class);
    protected final URI localUri;
    private final Set<LogFileStateListener> listeners;

    public AbstractLogFileTracker(URI localUri) {
        this.localUri = localUri;
        this.listeners = new HashSet<>();
    }

    @Override
    public int written(LogFile logFile) {
        int result = updateState(logFile, WRITTEN);
        updateSize(logFile);
        return result;
    }

    @Override
    public int writeError(LogFile logFile) {
        return updateState(logFile, WRITE_ERROR);
    }


    @Override
    public int prepared(LogFile logFile) {
        return updateState(logFile, PREPARED);
    }

    @Override
    public int preparing(LogFile logFile) {
        return updateState(logFile, PREPARING);
    }

    @Override
    public int prepError(LogFile logFile) {
        return updateState(logFile, PREP_ERROR);
    }

    @Override
    public int uploaded(LogFile logFile) {
        return updateState(logFile, UPLOADED);
    }

    @Override
    public int uploading(LogFile logFile) {
        return updateState(logFile, UPLOADING);
    }

    @Override
    public int uploadError(LogFile logFile) {
        return updateState(logFile, UPLOAD_ERROR);
    }

    protected int updateState(LogFile logFile, LogFileState newState) {
        logFile.setState(newState);
        int rows = recordStateChange(logFile);
        if (rows != 1)
            logIfBadRowCount(rows, logFile, String.format("set state to %s", logFile.getState()));
        else
            notifyListeners(logFile);
        return rows;
    }

    abstract protected int recordStateChange(LogFile logFile);

    abstract protected int updateSize(LogFile logFile);

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

    protected void notifyListeners(final LogFile logFile) {
        List<LogFileStateListener> toNotify = new ArrayList<>();
        synchronized (this.listeners) {
            toNotify.addAll(this.listeners);
        }
        if (LOG.isTraceEnabled())
            LOG.trace("There are {} listeners to inform about {} state change to {}.",
                    toNotify.size(),
                    logFile,
                    logFile.getState());
        for (final LogFileStateListener listener : toNotify) {
            listener.stateChanged(logFile);
        }
    }

    @Override
    public boolean addListener(LogFileStateListener listener) {
        synchronized (this.listeners) {
            return this.listeners.add(listener);
        }
    }

    @Override
    public boolean removeListener(LogFileStateListener listener) {
        synchronized (this.listeners) {
            return this.listeners.remove(listener);
        }
    }

    @Override
    public void start() {
        LOG.info("Starting {}.",getClass().getSimpleName());
    }

    @Override
    public void stop() {
        LOG.info("Stopping {}.",getClass().getSimpleName());
    }
}
