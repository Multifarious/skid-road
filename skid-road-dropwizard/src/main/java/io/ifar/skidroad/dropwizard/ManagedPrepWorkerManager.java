package io.ifar.skidroad.dropwizard;

import com.codahale.metrics.MetricRegistry;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Environment;
import io.ifar.skidroad.dropwizard.config.RequestLogPrepConfiguration;
import io.ifar.skidroad.dropwizard.config.SkidRoadConfiguration;
import io.ifar.skidroad.prepping.CompressPrepWorkerFactory;
import io.ifar.skidroad.prepping.EncryptAndCompressPrepWorkerFactory;
import io.ifar.skidroad.prepping.PrepWorkerFactory;
import io.ifar.skidroad.prepping.PrepWorkerManager;
import io.ifar.skidroad.tracking.LogFileTracker;

public class ManagedPrepWorkerManager extends PrepWorkerManager implements Managed {
    public ManagedPrepWorkerManager(LogFileTracker tracker, PrepWorkerFactory workerFactory,  Environment environment,
                                    int retryIntervalSeconds, int maxConcurrentWork, int unhealthyQueueDepthThreshold)
    {
        super(tracker, workerFactory, retryIntervalSeconds, maxConcurrentWork, unhealthyQueueDepthThreshold);

        environment.metrics().register(MetricRegistry.name(PrepWorkerManager.class, "prep_errors", "errors"), this.errorMeter);
        environment.metrics().register(MetricRegistry.name(PrepWorkerManager.class, "prep_successes", "successes"), this.successMeter);
        environment.metrics().register(MetricRegistry.name(PrepWorkerManager.class, "queue_depth"), this.queueDepthGauge);
        environment.metrics().register(MetricRegistry.name(PrepWorkerManager.class, "files_preparing"), this.preparingGauge);
        environment.metrics().register(MetricRegistry.name(PrepWorkerManager.class, "files_in_error"), this.errorGauge);
    }

    public static ManagedPrepWorkerManager build(PrepWorkerFactory workerFactory,
                                                 RequestLogPrepConfiguration prepConfiguration, Environment environment,
                                                 LogFileTracker tracker)
    {
        ManagedPrepWorkerManager prepManager = new ManagedPrepWorkerManager(tracker,
                workerFactory,
                environment,
                prepConfiguration.getRetryIntervalSeconds(),
                prepConfiguration.getMaxConcurrency(),
                prepConfiguration.getReportUnhealthyAtQueueDepth()
        );
        environment.lifecycle().manage(prepManager);
        environment.healthChecks().register("prep_worker_manager",prepManager.healthcheck);
        tracker.addListener(prepManager);
        return prepManager;
    }

    public static ManagedPrepWorkerManager buildWithEncryptAndCompress(RequestLogPrepConfiguration prepConfiguration,
                                                                       Environment environment, LogFileTracker tracker)
    {

        PrepWorkerFactory workerFactory = new EncryptAndCompressPrepWorkerFactory(
                prepConfiguration.getMasterKey()
        );

        return build(workerFactory, prepConfiguration, environment, tracker);
    }

    public static ManagedPrepWorkerManager buildWithCompress(RequestLogPrepConfiguration prepConfiguration,
                                                             Environment environment, LogFileTracker tracker)
    {
        PrepWorkerFactory workerFactory = new CompressPrepWorkerFactory();
        return build(workerFactory, prepConfiguration, environment, tracker);
    }

    public static ManagedPrepWorkerManager buildWithEncryptAndCompress(SkidRoadConfiguration skidRoadConfiguration,
                                                                       Environment environment, LogFileTracker tracker)
    {
        return buildWithEncryptAndCompress(skidRoadConfiguration.getRequestLogPrepConfiguration(), environment, tracker);
    }

    public static ManagedPrepWorkerManager buildWithCompress(SkidRoadConfiguration skidRoadConfiguration,
                                                             Environment environment, LogFileTracker tracker)
    {
        return buildWithCompress(skidRoadConfiguration.getRequestLogPrepConfiguration(), environment, tracker);
    }
}
