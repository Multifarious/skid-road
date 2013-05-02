package io.ifar.skidroad.dropwizard;

import com.yammer.dropwizard.lifecycle.Managed;
import io.ifar.skidroad.scheduling.SimpleQuartzScheduler;
import io.ifar.skidroad.tracking.LogFileTracker;
import io.ifar.skidroad.upload.UploadWorkerFactory;
import io.ifar.skidroad.upload.UploadWorkerManager;

public class ManagedUploadWorkerManager extends UploadWorkerManager implements Managed {
    public ManagedUploadWorkerManager(UploadWorkerFactory workerFactory, LogFileTracker tracker, SimpleQuartzScheduler scheduler, int pruneIntervalSeconds, int maxConcurrentUploads, int unhealthyQueueDepthThreshold) {
        super(workerFactory, tracker, scheduler, pruneIntervalSeconds, maxConcurrentUploads, unhealthyQueueDepthThreshold);
    }
}
