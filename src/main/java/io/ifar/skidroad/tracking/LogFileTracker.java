package io.ifar.skidroad.tracking;

import io.ifar.skidroad.LogFile;
import org.joda.time.DateTime;

import java.util.Iterator;

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
     * @return number of LogFiles updated. 0 indicates LogFile was not found in the tracker or its ownerURI has been changed. More than 1 indicates faulty tracker implementation.
     */
    int written(LogFile logFile);

    /**
     * Mark the specified LogFile as WRITE_ERROR
     *
     * @return number of LogFiles updated. 0 indicates LogFile was not found in the tracker or its ownerURI has been changed. More than 1 indicates faulty tracker implementation.
     */
    int writeError(LogFile logFile);

    /**
     * Record the prepPath for the specified LogFile
     *
     * @return number of LogFiles updated. 0 indicates LogFile was not found in the tracker or its ownerURI has been changed. More than 1 indicates faulty tracker implementation.
     */
    int updatePrepPath(LogFile logFile);

    /**
     * Record the archiveKey for the specified LogFile
     *
     * @return number of LogFiles updated. 0 indicates LogFile was not found in the tracker or its ownerURI has been changed. More than 1 indicates faulty tracker implementation.
     */
    int updateArchiveKey(LogFile logFile);

    /**
     * Record the archiveGroup and archiveURI for the specified LogFile
     *
     * @return number of LogFiles updated. 0 indicates LogFile was not found in the tracker or its ownerURI has been changed. More than 1 indicates faulty tracker implementation.
     */
    int updateArchiveLocation(LogFile logFile);

    /**
     * Mark the specified LogFile as PREPARING
     *
     * @return number of LogFiles updated. 0 indicates LogFile was not found in the tracker or its ownerURI has been changed. More than 1 indicates faulty tracker implementation.
     */
    int preparing(LogFile logFile);

    /**
     * Mark the specified LogFile as PREPARED
     *
     * @return number of LogFiles updated. 0 indicates LogFile was not found in the tracker or its ownerURI has been changed. More than 1 indicates faulty tracker implementation.
     */
    int prepared(LogFile logFile);

    /**
     * Mark the specified LogFile as PREP_ERROR
     *
     * @return number of LogFiles updated. 0 indicates LogFile was not found in the tracker or its ownerURI has been changed. More than 1 indicates faulty tracker implementation.
     */
    int prepError(LogFile logFile);

    /**
     * Mark the specified LogFile as UPLOADING
     *
     * @return number of LogFiles updated. 0 indicates LogFile was not found in the tracker or its ownerURI has been changed. More than 1 indicates faulty tracker implementation.
     */
    int uploading(LogFile logFile);

    /**
     * Mark the specified LogFile as UPLOADED
     *
     * @return number of LogFiles updated. 0 indicates LogFile was not found in the tracker or its ownerURI has been changed. More than 1 indicates faulty tracker implementation.
     */
    int uploaded(LogFile logFile);

    /**
     * Mark the specified LogFile as UPLOAD_ERROR
     *
     * @return number of LogFiles updated. 0 indicates LogFile was not found in the tracker or its ownerURI has been changed. More than 1 indicates faulty tracker implementation.
     */
    int uploadError(LogFile logFile);

    /**
     * Return all LogFile records owned by current instance in the specified state.
     * @return
     */
    Iterator<LogFile> findMine(LogFileState state);

    /**
     * Return LogFile record, if any, with teh specified rolling cohort and serial number.
     * @return
     */
    LogFile findByRollingCohortAndSerial(String rollingCohort, int serial);

    boolean addListener(LogFileStateListener listener);

    boolean removeListener(LogFileStateListener listener);

    public void start();

    public void stop();
}