package io.ifar.skidroad.dropwizard;

import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.lifecycle.Managed;
import io.ifar.skidroad.dropwizard.config.RequestLogPrepConfiguration;
import io.ifar.skidroad.dropwizard.config.SkidRoadConfiguration;
import io.ifar.skidroad.prepping.EncryptAndCompressPrepWorkerFactory;
import io.ifar.skidroad.prepping.PrepWorkerFactory;
import io.ifar.skidroad.prepping.PrepWorkerManager;
import io.ifar.skidroad.scheduling.SimpleQuartzScheduler;
import io.ifar.skidroad.tracking.LogFileTracker;

public class ManagedPrepWorkerManager extends PrepWorkerManager implements Managed {
    public ManagedPrepWorkerManager(LogFileTracker tracker, PrepWorkerFactory workerFactory, SimpleQuartzScheduler scheduler, int retryIntervalSeconds, int unhealthyQueueDepthThreshold) {
        super(tracker, workerFactory, scheduler, retryIntervalSeconds, unhealthyQueueDepthThreshold);
    }

    public static ManagedPrepWorkerManager build(PrepWorkerFactory workerFactory, RequestLogPrepConfiguration prepConfiguration, Environment environment, LogFileTracker tracker, SimpleQuartzScheduler scheduler) {
        ManagedPrepWorkerManager prepManager = new ManagedPrepWorkerManager(tracker,
                workerFactory,
                scheduler,
                prepConfiguration.getRetryIntervalSeconds(),
                prepConfiguration.getReportUnhealthyAtQueueDepth()
        );
        environment.manage(prepManager);
        environment.addHealthCheck(prepManager.healthcheck);
        tracker.addListener(prepManager);
        return prepManager;
    }

    public static ManagedPrepWorkerManager buildWithEncryptAndCompress(RequestLogPrepConfiguration prepConfiguration, Environment environment, LogFileTracker tracker, SimpleQuartzScheduler scheduler) {

        PrepWorkerFactory workerFactory = new EncryptAndCompressPrepWorkerFactory(
                prepConfiguration.getMasterKey(),
                prepConfiguration.getMasterIV()
        );

        return build(workerFactory, prepConfiguration, environment, tracker, scheduler);
    }

    public static ManagedPrepWorkerManager buildWithEncryptAndCompress(SkidRoadConfiguration skidRoadConfiguration, Environment environment, LogFileTracker tracker, SimpleQuartzScheduler scheduler) {
        return buildWithEncryptAndCompress(skidRoadConfiguration.getRequestLogPrepConfiguration(), environment, tracker, scheduler);
    }
}
