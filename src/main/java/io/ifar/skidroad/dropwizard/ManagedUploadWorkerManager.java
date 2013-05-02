package io.ifar.skidroad.dropwizard;

import com.yammer.dropwizard.lifecycle.Managed;
import io.ifar.skidroad.upload.UploadWorkerFactory;
import io.ifar.skidroad.upload.UploadWorkerManager;
import io.ifar.skidroad.tracking.LogFileTracker;

public class ManagedUploadWorkerManager extends UploadWorkerManager implements Managed {
    public ManagedUploadWorkerManager(UploadWorkerFactory workerFactory, LogFileTracker tracker, int maxConcurrentUploads, int unhealthyQueueDepthThreshold) {
        super(workerFactory, tracker, maxConcurrentUploads, unhealthyQueueDepthThreshold);
    }
}
