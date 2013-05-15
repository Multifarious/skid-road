package io.ifar.skidroad.prepping;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.HealthCheck;
import com.yammer.metrics.core.Meter;
import io.ifar.goodies.AutoCloseableIterator;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.scheduling.SimpleQuartzScheduler;
import io.ifar.skidroad.tracking.LogFileStateListener;
import io.ifar.skidroad.tracking.LogFileTracker;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.ifar.skidroad.tracking.LogFileState.*;

/**
 * Manages PrepWorkers.
 *
 * Note there is no PrepWorker interface. Uses a PrepWorkerFactory
 * to create worker runnables.
 */
public class PrepWorkerManager implements LogFileStateListener {
    private final static Logger LOG = LoggerFactory.getLogger(PrepWorkerManager.class);
    private final LogFileTracker tracker;
    private final PrepWorkerFactory workerFactory;
    private final SimpleQuartzScheduler scheduler;
    private final int retryIntervalSeconds;
    private final int maxConcurrentPrepWork;
    private ExecutorService executor;
    private final Set<String> activeFiles;

    public final HealthCheck healthcheck;
    private final AtomicInteger queueDepth = new AtomicInteger(0);

    private final Gauge<Integer> queueDepthGauge = Metrics.newGauge(this.getClass(),
            "queue_depth",
            new Gauge<Integer>() {
                @Override
                public Integer value() {
                    return queueDepth.get();
                }
            });

    private final Gauge<Integer> preparingGauge = Metrics.newGauge(this.getClass(),
            "files_preparing",
            new Gauge<Integer>() {
                @Override
                public Integer value() {
                    return tracker.getCount(PREPARING);
                }
            });

    private final Gauge<Integer> errorGauge = Metrics.newGauge(this.getClass(),
            "files_in_error",
            new Gauge<Integer>() {
                @Override
                public Integer value() {
                    return tracker.getCount(PREP_ERROR);
                }
            });
       private final Meter errorMeter = Metrics.newMeter(this.getClass(), "prep_errors", "errors", TimeUnit.SECONDS);
    private final Meter successMeter = Metrics.newMeter(this.getClass(), "prep_successes", "successes", TimeUnit.SECONDS);

    /**
     * @param workerFactory Provides workers to perform the LogFile uploads.
     * @param tracker Provides access to LogFile metadata.
     * @param scheduler Quartz
     * @param retryIntervalSeconds How often to look for files that can be retried.
     * @param maxConcurrentWork Size of thread pool executing the workers.
     * @param unhealthyQueueDepthThreshold HealthCheck returns unhealthy when work queue reaches this size.
     */
    public PrepWorkerManager(LogFileTracker tracker, PrepWorkerFactory workerFactory, SimpleQuartzScheduler scheduler, int retryIntervalSeconds, int maxConcurrentWork, final int unhealthyQueueDepthThreshold) {
        this.tracker = tracker;
        this.workerFactory = workerFactory;
        this.scheduler = scheduler;
        this.retryIntervalSeconds = retryIntervalSeconds;
        this.maxConcurrentPrepWork = maxConcurrentWork;
        this.activeFiles = new HashSet<String>();

        this.healthcheck = new HealthCheck("prep_worker_manager") {
            protected Result check() throws Exception {
                //Would be better to measure latency than depth, but that's more expensive.
                if (queueDepth.get() < unhealthyQueueDepthThreshold)
                    return Result.healthy(String.format("%d files queued or in-flight.", queueDepth.get()));
                else
                    return Result.unhealthy(String.format("%d files queued or in-flight exceeds threshold (%d).", queueDepth.get(), unhealthyQueueDepthThreshold));
            }
        };
    }

    @Override
    public void stateChanged(final LogFile logFile) {
        switch (logFile.getState()) {
            case WRITTEN:
                processAsync(logFile);
                break;
            case PREP_ERROR:
                errorMeter.mark();
                break;
            case PREPARED:
                successMeter.mark();
            default:
                //ignore
        }
    }
    private void processAsync(final LogFile logFile) {
        if (claim(logFile)) {
            try {
                LOG.debug("Preparing {} from {}.", logFile, logFile.getOriginPath());
                final Runnable worker = workerFactory.buildWorker(logFile, tracker);
                queueDepth.incrementAndGet();
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            worker.run();
                        } finally {
                            queueDepth.decrementAndGet();
                        }
                    }
                });
            } finally {
                release(logFile);
            }
        } else {
            LOG.trace("{} is already being prepped on another thread. No-op on this thread.", logFile);
        }
    }

    public void start() {
        LOG.info("Starting {}.", PrepWorkerManager.class.getSimpleName());
        this.executor = new ThreadPoolExecutor(0, maxConcurrentPrepWork,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>());
        tracker.addListener(this);
        Map<String,Object> retryConfiguration = new HashMap<>(1);
        retryConfiguration.put(RetryJob.PREP_WORKER_MANAGER, this);
        scheduler.schedule(this.getClass().getSimpleName() + "_retry", RetryJob.class, retryIntervalSeconds, retryConfiguration);
    }

    public void stop() {
        LOG.info("Stopping {}.",PrepWorkerManager.class.getSimpleName());
        tracker.removeListener(this);
        this.executor.shutdown();
    }

    /**
     * Manages concurrency between listener-invoked and scheduler-invoked processing
     * @return true if this LogFile may be processed
     */
    private boolean claim(LogFile f) {
        synchronized (activeFiles) {
            return activeFiles.add(f.getID());
        }
    }

    /**
     * Releases processing lock on specified LogFile.
     * Caller responsible for ensuring that release is called if and only if claim returned true
     */
    private void release(LogFile f) {
        synchronized (activeFiles) {
            activeFiles.remove(f.getID());
        }
    }

    /**
     * @return true if this LogFile may be processed
     */
    private boolean isClaimed(LogFile f) {
        synchronized (activeFiles) {
            return activeFiles.contains(f.getID());
        }
    }

    @DisallowConcurrentExecution
    public static class RetryJob implements Job
    {
        public static final String PREP_WORKER_MANAGER = "prep_worker_manager";

        public void execute(JobExecutionContext context) throws JobExecutionException {
            JobDataMap m = context.getMergedJobDataMap();
            PrepWorkerManager mgr = (PrepWorkerManager) m.get(PREP_WORKER_MANAGER);


            try {
                try (AutoCloseableIterator<LogFile> iterator = mgr.tracker.findMine(PREPARING)) {
                    for (LogFile logFile : iterator) {
                        //claim check not required for thread safety, but avoid spurious WARNs
                        if (!mgr.isClaimed(logFile)) {
                            LOG.warn("Found stale PREPARING record for {}. Perhaps server was previously terminated while preparing it. Queueing preparation.", logFile.getOriginPath());
                            mgr.processAsync(logFile);
                        }
                    }
                }

                try (AutoCloseableIterator<LogFile> iterator = mgr.tracker.findMine(WRITTEN)) {
                    for (LogFile logFile : iterator) {
                        if (!mgr.isClaimed(logFile)) {
                            LOG.warn("Found stale WRITTEN record for {}. Perhaps server was previously terminated before preparing it. Queueing preparation.", logFile.getOriginPath());
                            mgr.processAsync(logFile);
                        }
                    }
                }

                try (AutoCloseableIterator<LogFile> iterator = mgr.tracker.findMine(PREP_ERROR)) {
                    for (LogFile logFile : iterator) {
                        //No need for claim check because UPLOAD_ERROR implies listener-based processing has terminated
                        LOG.warn("Found PREP_ERROR record for {}. Perhaps error was transient. Retrying.", logFile.getOriginPath());
                        mgr.processAsync(logFile);
                    }
                }
            } catch (Exception e) {
                throw new JobExecutionException("Failure pruning prep jobs.", e);
            }
        }
    }
}
