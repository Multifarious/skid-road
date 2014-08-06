package io.ifar.skidroad.tracking;

import io.ifar.skidroad.LogFile;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.ResultIterator;

import java.util.Set;

/**
 * API for maintaining LogFile state.
 */
public interface LogFileTracker {
    /**
     * Request creation of a new log file. Selects an available serial number for the new log file, generates a name
     * for it using the serial number and pathPattern, and returns a LogFile instance.
     * @param rollingCohort Cohort LogFile is part of. E.g. "2009-03-12T03"
     * @param pathPattern A String.format compatible pattern with a single %d into which the serial number can be interpolated, yielding path for new LogFile.
     * @return LogFile in specified rollingCohort with unique serial number in WRITING state.
     */
    LogFile open(String rollingCohort, String pathPattern, DateTime startTime);

    /**
     * Mark the specified LogFile as WRITTEN. Also store byteSize.
     *
     * @param logFile the log file to mark.
     * @return number of LogFiles updated. 0 indicates LogFile was not found in the tracker or its ownerURI has been changed. More than 1 indicates faulty tracker implementation.
     */
    int written(LogFile logFile);

    /**
     * Mark the specified LogFile as WRITE_ERROR
     *
     * @param logFile the logfile to mark
     * @return number of LogFiles updated. 0 indicates LogFile was not found in the tracker or its ownerURI has been changed. More than 1 indicates faulty tracker implementation.
     */
    int writeError(LogFile logFile);

    /**
     * Record the prepPath for the specified LogFile
     *
     * @param logFile the log file to mark.
     * @return number of LogFiles updated. 0 indicates LogFile was not found in the tracker or its ownerURI has been changed. More than 1 indicates faulty tracker implementation.
     */
    int updatePrepPath(LogFile logFile);

    /**
     * Record the archiveKey for the specified LogFile
     *
     * @param logFile the log file to mark.
     * @return number of LogFiles updated. 0 indicates LogFile was not found in the tracker or its ownerURI has been changed. More than 1 indicates faulty tracker implementation.
     */
    int updateArchiveKey(LogFile logFile);

    /**
     * Record the archiveGroup and archiveURI for the specified LogFile
     *
     * @param logFile the log file to mark.
     * @return number of LogFiles updated. 0 indicates LogFile was not found in the tracker or its ownerURI has been changed. More than 1 indicates faulty tracker implementation.
     */
    int updateArchiveLocation(LogFile logFile);

    /**
     * Mark the specified LogFile as PREPARING
     *
     * @param logFile the log file to mark.
     * @return number of LogFiles updated. 0 indicates LogFile was not found in the tracker or its ownerURI has been changed. More than 1 indicates faulty tracker implementation.
     */
    int preparing(LogFile logFile);

    /**
     * Mark the specified LogFile as PREPARED
     *
     * @param logFile the log file to mark.
     * @return number of LogFiles updated. 0 indicates LogFile was not found in the tracker or its ownerURI has been changed. More than 1 indicates faulty tracker implementation.
     */
    int prepared(LogFile logFile);

    /**
     * Mark the specified LogFile as PREP_ERROR
     *
     * @param logFile the log file to mark.
     * @return number of LogFiles updated. 0 indicates LogFile was not found in the tracker or its ownerURI has been changed. More than 1 indicates faulty tracker implementation.
     */
    int prepError(LogFile logFile);

    /**
     * Mark the specified LogFile as UPLOADING
     *
     * @param logFile the log file to mark.
     * @return number of LogFiles updated. 0 indicates LogFile was not found in the tracker or its ownerURI has been changed. More than 1 indicates faulty tracker implementation.
     */
    int uploading(LogFile logFile);

    /**
     * Mark the specified LogFile as UPLOADED
     *
     * @param logFile the log file to mark.
     * @return number of LogFiles updated. 0 indicates LogFile was not found in the tracker or its ownerURI has been changed. More than 1 indicates faulty tracker implementation.
     */
    int uploaded(LogFile logFile);

    /**
     * Mark the specified LogFile as UPLOAD_ERROR
     *
     * @param logFile the log file to mark.
     * @return number of LogFiles updated. 0 indicates LogFile was not found in the tracker or its ownerURI has been changed. More than 1 indicates faulty tracker implementation.
     */
    int uploadError(LogFile logFile);

    /**
     * Return all LogFile records owned by current instance in the specified state.
     *
     * Iterator must be closed or database connection will be leaked.
     */
    ResultIterator<LogFile> findMine(LogFileState state);

    /**
     * Return all LogFile records owned by current instance in one of the specified states.
     *
     * Iterator must be closed or database connection will be leaked.
     */
    ResultIterator<LogFile> findMine(Set<LogFileState> states);

    /**
     * Query for a time range of {@link LogFile}s by state.
     *
     * @param state the state to query for
     * @param start the start of the interval (inclusive)
     * @param end the end of the interval (exclusive)
     * @return the matching {@link LogFile}s.
     */
    ResultIterator<LogFile> findMine(LogFileState state, DateTime start, DateTime end);

    /**
     * Query for a time range of {@link LogFile}s by state.
     *
     * @param states the states to query for
     * @param start the start of the interval (inclusive)
     * @param end the end of the interval (exclusive)
     * @return the matching {@link LogFile}s.
     */
    ResultIterator<LogFile> findMine(Set<LogFileState> states, DateTime start, DateTime end);

    /**
     * Return count of LogFile records in the specified state.
     */
    int getCount(LogFileState state);

    /**
     * Return count of LogFile records in the specified states.
     */
    int getCount(Set<LogFileState> states);

    boolean addListener(LogFileStateListener listener);

    boolean removeListener(LogFileStateListener listener);

    public void start();

    public void stop();
}
