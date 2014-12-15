package io.ifar.skidroad.dropwizard;

import com.codahale.metrics.MetricRegistry;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Environment;
import io.ifar.skidroad.dropwizard.config.RequestLogUploadConfiguration;
import io.ifar.skidroad.dropwizard.config.SkidRoadConfiguration;
import io.ifar.skidroad.tracking.LogFileTracker;
import io.ifar.skidroad.upload.UploadWorkerFactory;
import io.ifar.skidroad.upload.UploadWorkerManager;

import java.net.URISyntaxException;

public class ManagedUploadWorkerManager extends UploadWorkerManager implements Managed {
    public ManagedUploadWorkerManager(UploadWorkerFactory workerFactory, LogFileTracker tracker,
                                      Environment environment, int retryIntervalSeconds, int maxConcurrentUploads,
                                      int unhealthyQueueDepthThreshold)
    {
        super(workerFactory, tracker, retryIntervalSeconds, maxConcurrentUploads, unhealthyQueueDepthThreshold);

        environment.metrics().register(MetricRegistry.name(UploadWorkerManager.class, "upload_errors", "errors"),this.errorMeter);
        environment.metrics().register(MetricRegistry.name(UploadWorkerManager.class, "upload_successes", "successes"),this.successMeter);
        environment.metrics().register(MetricRegistry.name(UploadWorkerManager.class, "queue_depth"),this.queueDepthGauge);
        environment.metrics().register(MetricRegistry.name(UploadWorkerManager.class, "files_uploading"),this.uploadingGauge);
        environment.metrics().register(MetricRegistry.name(UploadWorkerManager.class, "files_in_error"),this.errorGauge);
    }

    public static ManagedUploadWorkerManager build(RequestLogUploadConfiguration uploadConfiguration, Environment environment,
                                        LogFileTracker tracker, UploadWorkerFactory workerFactory) throws URISyntaxException
    {
        ManagedUploadWorkerManager uploadManager = new ManagedUploadWorkerManager(
                workerFactory,
                tracker,
                environment,
                uploadConfiguration.getRetryIntervalSeconds(),
                uploadConfiguration.getMaxConcurrentUploads(),
                uploadConfiguration.getReportUnhealthyAtQueueDepth()
        );
        environment.lifecycle().manage(uploadManager);
        environment.healthChecks().register("upload_worker_manager",uploadManager.healthcheck);
        return uploadManager;
    }

    public static ManagedUploadWorkerManager build(SkidRoadConfiguration skidRoadConfiguration, Environment environment,
                                                   LogFileTracker tracker,
                                                   UploadWorkerFactory workerFactory) throws URISyntaxException
    {
        return build(skidRoadConfiguration.getRequestLogUploadConfiguration(), environment, tracker, workerFactory);
    }
}
