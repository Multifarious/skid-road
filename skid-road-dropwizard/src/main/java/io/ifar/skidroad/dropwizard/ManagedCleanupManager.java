package io.ifar.skidroad.dropwizard;

import com.codahale.metrics.MetricRegistry;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Environment;
import io.ifar.skidroad.cleanup.CleanupManager;
import io.ifar.skidroad.tracking.LogFileTracker;

/**
 *
 */
public class ManagedCleanupManager extends CleanupManager implements Managed {

    public ManagedCleanupManager(LogFileTracker tracker, Environment environment, int minAgeHours, int maxAgeHours) {
        super(tracker, minAgeHours, maxAgeHours);
        environment.metrics().register(MetricRegistry.name(CleanupManager.class, "deletedUploadedFiles"), this.deletedFilesCounter);
    }
}
