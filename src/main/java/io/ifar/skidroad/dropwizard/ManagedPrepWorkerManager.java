package io.ifar.skidroad.dropwizard;

import com.yammer.dropwizard.lifecycle.Managed;
import io.ifar.skidroad.prepping.PrepWorkerFactory;
import io.ifar.skidroad.prepping.PrepWorkerManager;
import io.ifar.skidroad.scheduling.SimpleQuartzScheduler;
import io.ifar.skidroad.tracking.LogFileTracker;

public class ManagedPrepWorkerManager extends PrepWorkerManager implements Managed {
    public ManagedPrepWorkerManager(LogFileTracker tracker, PrepWorkerFactory workerFactory, SimpleQuartzScheduler scheduler, int pruneIntervalSeconds, int unhealthyQueueDepthThreshold) {
        super(tracker, workerFactory, scheduler, pruneIntervalSeconds, unhealthyQueueDepthThreshold);
    }
}
