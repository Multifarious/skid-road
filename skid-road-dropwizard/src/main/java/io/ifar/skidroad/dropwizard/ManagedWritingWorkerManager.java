package io.ifar.skidroad.dropwizard;

import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.lifecycle.Managed;
import io.ifar.goodies.Tuple;
import io.ifar.skidroad.dropwizard.config.RequestLogWriterConfiguration;
import io.ifar.skidroad.dropwizard.config.SkidRoadConfiguration;
import io.ifar.skidroad.rolling.DailyFileRollingScheme;
import io.ifar.skidroad.rolling.FileRollingScheme;
import io.ifar.skidroad.rolling.HourlyFileRollingScheme;
import io.ifar.skidroad.rolling.MinutelyFileRollingScheme;
import io.ifar.skidroad.scheduling.SimpleQuartzScheduler;
import io.ifar.skidroad.tracking.LogFileTracker;
import io.ifar.skidroad.writing.WritingWorkerFactory;
import io.ifar.skidroad.writing.WritingWorkerManager;
import io.ifar.skidroad.writing.csv.CSVWritingWorkerFactory;
import io.ifar.skidroad.writing.file.FileWritingWorkerFactory;
import io.ifar.skidroad.writing.file.Serializer;

public class ManagedWritingWorkerManager<T> extends WritingWorkerManager<T> implements Managed {

    public ManagedWritingWorkerManager(FileRollingScheme rollingScheme, LogFileTracker tracker,
                                       WritingWorkerFactory<T> factory, SimpleQuartzScheduler scheduler, int pruneIntervalSeconds,
                                       int spawnThreshold, int unhealthyThreshold) {
        super(rollingScheme,tracker,factory,scheduler,pruneIntervalSeconds,spawnThreshold,unhealthyThreshold);
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
                pruneIntervalSeconds,
                logConf.getSpawnNewWorkerAtQueueDepth(),
                logConf.getReportUnhealthyAtQueueDepth()
        );
        environment.manage(writerManager);
        environment.addHealthCheck(writerManager.healthcheck);
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
                pruneIntervalSeconds,
                logConf.getSpawnNewWorkerAtQueueDepth(),
                logConf.getReportUnhealthyAtQueueDepth()
        );
        environment.manage(writerManager);
        environment.addHealthCheck(writerManager.healthcheck);
        return writerManager;
    }

    public static <T extends Tuple> ManagedWritingWorkerManager<T> buildCSV(LogFileTracker tracker, String nullRepresentation, SimpleQuartzScheduler scheduler, SkidRoadConfiguration skidRoadConfiguration, Environment environment) {
        return buildCSV(tracker, nullRepresentation, scheduler, skidRoadConfiguration.getRequestLogWriterConfiguration(), environment);

    }

    public static FileRollingScheme getFileRollingScheme(RequestLogWriterConfiguration logConf) {
        FileRollingScheme rollingScheme;
        switch (logConf.getRollingFrequency()) {
            case daily:
                rollingScheme = new DailyFileRollingScheme(
                        logConf.getBasePath(),
                        logConf.getNamePrefix(),
                        logConf.getNameSuffix(),
                        logConf.getAfterRollCloseFileDelaySeconds());
                break;
            case hourly:
                rollingScheme = new HourlyFileRollingScheme(
                        logConf.getBasePath(),
                        logConf.getNamePrefix(),
                        logConf.getNameSuffix(),
                        logConf.getAfterRollCloseFileDelaySeconds());
                break;
            case minutely:
                rollingScheme = new MinutelyFileRollingScheme(
                        logConf.getBasePath(),
                        logConf.getNamePrefix(),
                        logConf.getNameSuffix(),
                        logConf.getAfterRollCloseFileDelaySeconds());
                break;
            default:
                throw new IllegalArgumentException(String.format("Unrecognized rolling frequency '%s",logConf.getRollingFrequency()));
        }
        return rollingScheme;
    }
}
