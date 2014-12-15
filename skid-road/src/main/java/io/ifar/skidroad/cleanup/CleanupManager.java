package io.ifar.skidroad.cleanup;

import com.codahale.metrics.Counter;
import com.google.common.util.concurrent.AbstractScheduledService;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileState;
import io.ifar.skidroad.tracking.LogFileTracker;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.ResultIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class CleanupManager {

    private static final Logger LOG = LoggerFactory.getLogger(CleanupManager.class);

    private final LogFileTracker tracker;
    private final int minAgeHours;
    private final int maxAgeHours;

    protected Counter deletedFilesCounter = new Counter();
    private CleanupJob cleanupJob;

    public CleanupManager(LogFileTracker tracker,  int minAgeHours, int maxAgeHours) {
        this.tracker = tracker;
        this.minAgeHours = minAgeHours;
        this.maxAgeHours = maxAgeHours;
    }

    public void start() {
        cleanupJob = new CleanupJob();
        cleanupJob.startAsync();
        cleanupJob.awaitRunning();
        LOG.info("{} started.",CleanupManager.class.getSimpleName());
    }

    public void stop() {
        LOG.info("Stopping {}.", CleanupManager.class.getSimpleName());
        if (cleanupJob != null) {
            cleanupJob.stopAsync();
            cleanupJob.awaitTerminated();
        }
        LOG.info("Stopped {}.", CleanupManager.class.getSimpleName());
    }

    public void sweep() {
        DateTime now = new DateTime();
        DateTime startInterval = now.minusHours(maxAgeHours);
        DateTime endInterval = now.minusHours(minAgeHours);
        ResultIterator<LogFile> oldFiles = tracker.findMine(LogFileState.UPLOADED, startInterval, endInterval);
        int removed = 0;
        LOG.info("Starting an uploaded file cleanup sweep.");
        while (oldFiles.hasNext()) {
            LogFile lf = oldFiles.next();
            for (Path toDelete: new Path[] { lf.getOriginPath(), lf.getPrepPath()}) {
                if (Files.exists(toDelete)) {
                    try {
                        Files.delete(toDelete);
                        deletedFilesCounter.inc();
                        ++removed;
                    } catch (IOException ioe) {
                        LOG.error("Unable to delete file {} for log file {}: ({}) {}",
                                toDelete,lf.getID(),ioe.getClass().getSimpleName(), ioe.getMessage());
                    }
                }
            }
        }
        LOG.info("Done sweeping for old uploaded files; removed {} this pass.",removed);
    }

    public class CleanupJob extends AbstractScheduledService
    {

        @Override
        protected void runOneIteration() throws Exception {
            try {
                sweep();
            } catch (Exception e) {
                LOG.error("Unable to complete cleanup invocation due to unexpected exception: ({}) {}",
                        e.getClass(), e.getMessage(), e);
            }
        }

        @Override
        protected Scheduler scheduler() {
            // TODO: Pull this out as a configuration property.
            return Scheduler.newFixedDelaySchedule(0L, 15, TimeUnit.MINUTES);
        }
    }

}
