package io.ifar.skidroad.dropwizard;

import com.codahale.metrics.MetricRegistry;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Environment;
import io.ifar.goodies.Tuple;
import io.ifar.skidroad.dropwizard.config.RequestLogWriterConfiguration;
import io.ifar.skidroad.dropwizard.config.SkidRoadConfiguration;
import io.ifar.skidroad.rolling.*;
import io.ifar.skidroad.scheduling.SimpleQuartzScheduler;
import io.ifar.skidroad.tracking.LogFileTracker;
import io.ifar.skidroad.writing.WritingWorkerFactory;
import io.ifar.skidroad.writing.WritingWorkerManager;
import io.ifar.skidroad.writing.csv.CSVWritingWorkerFactory;
import io.ifar.skidroad.writing.file.FileWritingWorkerFactory;
import io.ifar.skidroad.writing.file.Serializer;

public class ManagedWritingWorkerManager<T> extends WritingWorkerManager<T> implements Managed {

    public ManagedWritingWorkerManager(FileRollingScheme rollingScheme, LogFileTracker tracker,
                                       WritingWorkerFactory<T> factory, SimpleQuartzScheduler scheduler, Environment environment, int pruneIntervalSeconds,
                                       int spawnThreshold, int unhealthyThreshold) {
        super(rollingScheme,tracker,factory,scheduler,pruneIntervalSeconds,spawnThreshold,unhealthyThreshold);

        environment.metrics().register(MetricRegistry.name(WritingWorkerManager.class, "queue_count"), this.queueCountGauge);
        environment.metrics().register(MetricRegistry.name(WritingWorkerManager.class, "queue_depth"), this.queueDepthGauge);
        environment.metrics().register(MetricRegistry.name(WritingWorkerManager.class, "worker_count"), this.workerCountGauge);

    }

    public static <T> ManagedWritingWorkerManager<T> build(LogFileTracker tracker, Serializer<T> serializer, SimpleQuartzScheduler scheduler, RequestLogWriterConfiguration logConf, Environment environment) {
        FileRollingScheme rollingScheme = getFileRollingScheme(logConf);
        int pruneIntervalSeconds = 5;
        WritingWorkerFactory<T> workerFactory = new FileWritingWorkerFactory<>(serializer, logConf.getFileFlushIntervalSeconds());
        ManagedWritingWorkerManager<T> writerManager = new ManagedWritingWorkerManager<>(
                rollingScheme,
                tracker,
                workerFactory,
                scheduler,
                environment,
                pruneIntervalSeconds,
                logConf.getSpawnNewWorkerAtQueueDepth(),
                logConf.getReportUnhealthyAtQueueDepth()
        );
        environment.lifecycle().manage(writerManager);
        environment.healthChecks().register("writing_worker_manager", writerManager.healthcheck);
        return writerManager;
    }

    public static <T> ManagedWritingWorkerManager<T> build(LogFileTracker tracker, Serializer<T> serializer, SimpleQuartzScheduler scheduler, SkidRoadConfiguration skidRoadConfiguration, Environment environment) {
        return build(tracker, serializer, scheduler, skidRoadConfiguration.getRequestLogWriterConfiguration(), environment);

    }

    public static <T extends Tuple> ManagedWritingWorkerManager<T> buildCSV(LogFileTracker tracker, String nullRepresentation, SimpleQuartzScheduler scheduler, RequestLogWriterConfiguration logConf, Environment environment) {
        FileRollingScheme rollingScheme = getFileRollingScheme(logConf);
        int pruneIntervalSeconds = 5;
        WritingWorkerFactory<T> workerFactory = new CSVWritingWorkerFactory<T>(nullRepresentation, logConf.getFileFlushIntervalSeconds());
        ManagedWritingWorkerManager<T> writerManager = new ManagedWritingWorkerManager<>(
                rollingScheme,
                tracker,
                workerFactory,
                scheduler,
                environment,
                pruneIntervalSeconds,
                logConf.getSpawnNewWorkerAtQueueDepth(),
                logConf.getReportUnhealthyAtQueueDepth()
        );
        environment.lifecycle().manage(writerManager);
        environment.healthChecks().register("writing_worker_manager",writerManager.healthcheck);
        return writerManager;
    }

    public static <T extends Tuple> ManagedWritingWorkerManager<T> buildCSV(LogFileTracker tracker, String nullRepresentation, SimpleQuartzScheduler scheduler, SkidRoadConfiguration skidRoadConfiguration, Environment environment) {
        return buildCSV(tracker, nullRepresentation, scheduler, skidRoadConfiguration.getRequestLogWriterConfiguration(), environment);

    }

    public static FileRollingScheme getFileRollingScheme(RequestLogWriterConfiguration logConf) {
        return new BasicFileRollingScheme(logConf.getBasePath(), logConf.getNamePrefix(), logConf.getNameSuffix(),
                logConf.getAfterRollCloseFileDelaySeconds(),logConf.getRollingFrequency().duration());
    }
}
