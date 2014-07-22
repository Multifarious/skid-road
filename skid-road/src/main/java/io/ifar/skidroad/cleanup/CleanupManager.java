package io.ifar.skidroad.cleanup;

import com.codahale.metrics.Counter;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.scheduling.SimpleQuartzScheduler;
import io.ifar.skidroad.tracking.LogFileState;
import io.ifar.skidroad.tracking.LogFileTracker;
import org.joda.time.DateTime;
import org.quartz.*;
import org.skife.jdbi.v2.ResultIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class CleanupManager {

    private static final Logger LOG = LoggerFactory.getLogger(CleanupManager.class);

    private final LogFileTracker tracker;
    private final int minAgeHours;
    private final int maxAgeHours;
    private final SimpleQuartzScheduler scheduler;

    protected Counter deletedFilesCounter = new Counter();
    private Trigger trigger;

    public CleanupManager(LogFileTracker tracker, SimpleQuartzScheduler scheduler, int minAgeHours, int maxAgeHours) {
        this.tracker = tracker;
        this.scheduler = scheduler;
        this.minAgeHours = minAgeHours;
        this.maxAgeHours = maxAgeHours;
    }

    public void start() {
        Map<String,Object> config = new HashMap<>(1);
        config.put(CleanupJob.CLEANUP_MANAGER, this);
        trigger = scheduler.schedule(CleanupJob.CLEANUP_MANAGER, CleanupJob.class,
                SimpleScheduleBuilder.repeatMinutelyForever(15), config);
        LOG.info("{} started.",CleanupManager.class.getSimpleName());
    }

    public void stop() {
        LOG.info("Stopping {}.", CleanupManager.class.getSimpleName());
        if (trigger != null) {
            scheduler.unschedule(trigger);
        }
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

    @DisallowConcurrentExecution
    public static class CleanupJob implements Job
    {
        public static final String CLEANUP_MANAGER = "cleanup_manager";

        public void execute(JobExecutionContext context) throws JobExecutionException {
            JobDataMap m = context.getMergedJobDataMap();
            CleanupManager mgr = (CleanupManager) m.get(CLEANUP_MANAGER);
            try {
                mgr.sweep();
            } catch (Exception e) {
                throw new JobExecutionException("Failure running cleanup job.", e);
            }
        }
    }

}
