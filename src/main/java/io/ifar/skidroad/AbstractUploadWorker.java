package io.ifar.skidroad;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * Base class for UploadWorkers that handles tracker interaction.
 */
public abstract class AbstractUploadWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractUploadWorker.class);
    protected final LogFile logFile;
    protected final LogFileTracker tracker;

    public AbstractUploadWorker(LogFile logFile, LogFileTracker tracker) {
        this.logFile = logFile;
        this.tracker = tracker;
    }

    @Override
    public void run() {
        try {
            if (tracker.uploading(logFile) != 1)
                throw new IllegalStateException("Cannot set UPLOADING state for " + logFile);
            //determine archive group and URI
            logFile.setArchiveGroup(determineArchiveGroup(logFile));
            logFile.setArchiveURI(determineArchiveURI(logFile));
            if (tracker.updateArchiveLocation(logFile) != 1)
                throw new IllegalStateException("Cannot set archive location for " + logFile);
            LOG.debug("Uploading {} to {}", logFile, logFile.getArchiveURI());
            push(logFile);
            LOG.debug("Uploaded {} to {}", logFile, logFile.getArchiveURI());
            tracker.uploaded(logFile); //ignore update failures; worker exiting anyway
        } catch (Exception e) {
            //org.jets3t.service.ServiceException needs to be toString'd to see the useful parts.
            //Otherwise all we get is the getMessage() of "S3 Error Message".
            LOG.warn("Upload for {} failed {}: ", logFile, e, e);
            tracker.uploadError(logFile); //ignore update failures; worker exiting anyway
        }
    }

    abstract String determineArchiveGroup(LogFile logFile) throws Exception;

    abstract URI determineArchiveURI(LogFile logFile) throws Exception;

    abstract void push(LogFile logFile) throws Exception;
}
