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

    public CleanupManager(LogFileTracker tracker, int minAgeHours, int maxAgeHours) {
        this.tracker = tracker;
        this.minAgeHours = minAgeHours;
        this.maxAgeHours = maxAgeHours;
    }

    public void start() {
        Map<String,Object> config = new HashMap<>(1);
        LOG.info("Starting {}.",CleanupManager.class.getSimpleName());
        cleanupJob = new CleanupJob();
        cleanupJob.startAsync();
        LOG.info("Started {}.",CleanupManager.class.getSimpleName());
    }

    public void stop() {
        LOG.info("Stopping {}.", CleanupManager.class.getSimpleName());
        if (cleanupJob != null) {
            cleanupJob.stopAsync();

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


    private class CleanupJob extends AbstractScheduledService {
        @Override
        protected void runOneIteration() throws Exception {
            sweep();
        }

        @Override
        protected Scheduler scheduler() {
            return Scheduler.newFixedDelaySchedule(1L, 15L, TimeUnit.MINUTES);
        }
    }

}
