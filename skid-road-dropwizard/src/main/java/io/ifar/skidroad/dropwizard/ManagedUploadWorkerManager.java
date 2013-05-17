package io.ifar.skidroad.dropwizard;

import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.lifecycle.Managed;
import io.ifar.skidroad.dropwizard.config.RequestLogUploadConfiguration;
import io.ifar.skidroad.dropwizard.config.SkidRoadConfiguration;
import io.ifar.skidroad.scheduling.SimpleQuartzScheduler;
import io.ifar.skidroad.tracking.LogFileTracker;
import io.ifar.skidroad.upload.UploadWorkerFactory;
import io.ifar.skidroad.upload.UploadWorkerManager;

import java.net.URISyntaxException;

public class ManagedUploadWorkerManager extends UploadWorkerManager implements Managed {
    public ManagedUploadWorkerManager(UploadWorkerFactory workerFactory, LogFileTracker tracker, SimpleQuartzScheduler scheduler, int retryIntervalSeconds, int maxConcurrentUploads, int unhealthyQueueDepthThreshold) {
        super(workerFactory, tracker, scheduler, retryIntervalSeconds, maxConcurrentUploads, unhealthyQueueDepthThreshold);
    }

    public static ManagedUploadWorkerManager build(RequestLogUploadConfiguration uploadConfiguration, Environment environment,
                                        LogFileTracker tracker, UploadWorkerFactory workerFactory, SimpleQuartzScheduler scheduler) throws URISyntaxException
    {
        ManagedUploadWorkerManager uploadManager = new ManagedUploadWorkerManager(
                workerFactory,
                tracker,
                scheduler,
                uploadConfiguration.getRetryIntervalSeconds(),
                uploadConfiguration.getMaxConcurrentUploads(),
                uploadConfiguration.getReportUnhealthyAtQueueDepth()
        );
        environment.manage(uploadManager);
        environment.addHealthCheck(uploadManager.healthcheck);
        return uploadManager;
    }

    public static ManagedUploadWorkerManager build(SkidRoadConfiguration skidRoadConfiguration, Environment environment,
                                                   LogFileTracker tracker, UploadWorkerFactory workerFactory, SimpleQuartzScheduler scheduler) throws URISyntaxException {
        return build(skidRoadConfiguration.getRequestLogUploadConfiguration(), environment, tracker, workerFactory, scheduler);
    }
}
