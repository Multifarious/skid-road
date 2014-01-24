package io.ifar.skidroad.dropwizard;

import com.yammer.dropwizard.lifecycle.Managed;
import io.ifar.skidroad.cleanup.CleanupManager;
import io.ifar.skidroad.scheduling.SimpleQuartzScheduler;
import io.ifar.skidroad.tracking.LogFileTracker;

/**
 *
 */
public class ManagedCleanupManager extends CleanupManager implements Managed  {

    public ManagedCleanupManager(LogFileTracker tracker, SimpleQuartzScheduler scheduler, int minAgeHours, int maxAgeHours) {
        super(tracker, scheduler, minAgeHours, maxAgeHours);
    }
}
