package io.ifar.skidroad.upload;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.HealthCheck;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.scheduling.SimpleQuartzScheduler;
import io.ifar.skidroad.tracking.LogFileStateListener;
import io.ifar.skidroad.tracking.LogFileTracker;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.ifar.skidroad.tracking.LogFileState.*;

/**
 * Manages UploadWorkers.
 *
 * Note there is no UploadWorker interface. Uses a UploadWorkerFactory
 * to create worker runnables.
 *
 * TODO (future): Nearly identical code to PrepWorkerManager. Consolidate?
 */
public class UploadWorkerManager implements LogFileStateListener {
    private final static Logger LOG = LoggerFactory.getLogger(UploadWorkerManager.class);

    private final UploadWorkerFactory workerFactory;
    private final LogFileTracker tracker;
    private final SimpleQuartzScheduler scheduler;
    public final int pruneIntervalSeconds;
    private final int maxConcurrentUploads;
    private ExecutorService executor;
    private final Set<String> activeFiles;

    public final HealthCheck healthcheck;
    private final AtomicInteger queueDepth = new AtomicInteger(0);
    private final Counter enqueueCount = Metrics.newCounter(this.getClass(), "enqueue_count");
    private final Counter errorCount = Metrics.newCounter(this.getClass(), "error_count");

    private final Gauge<Integer> queueDepthGauge = Metrics.newGauge(this.getClass(),
            "queue_depth",
            new Gauge<Integer>() {
                @Override
                public Integer value() {
                    return queueDepth.get();
                }
            });


    public UploadWorkerManager(UploadWorkerFactory workerFactory, LogFileTracker tracker, SimpleQuartzScheduler scheduler, int pruneIntervalSeconds, int maxConcurrentUploads,  final int unhealthyQueueDepthThreshold) {
        this.workerFactory = workerFactory;
        this.tracker = tracker;
        this.scheduler = scheduler;
        this.pruneIntervalSeconds = pruneIntervalSeconds;
        this.maxConcurrentUploads = maxConcurrentUploads;
        this.activeFiles = new HashSet<String>();

        this.healthcheck = new HealthCheck("upload_worker_manager") {
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
            case PREPARED:
                processAsync(logFile);
                break;
            case UPLOAD_ERROR:
                errorCount.inc();
                break;
            default:
                //ignore
        }
    }

    private void processAsync(final LogFile logFile) {
        if (claim(logFile)) {
            try {
                LOG.debug("Uploading {} from {}.", logFile, logFile.getPrepPath());
                final Runnable worker = workerFactory.buildWorker(logFile, tracker);
                enqueueCount.inc();
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
            LOG.trace("{} is already being uploaded on another thread. No-op on this thread.", logFile);
        }
    }

    public void start() {
        LOG.info("Starting {}.", UploadWorkerManager.class.getSimpleName());
        this.executor = new ThreadPoolExecutor(0, maxConcurrentUploads,
                                      60L, TimeUnit.SECONDS,
                                      new SynchronousQueue<Runnable>());
        tracker.addListener(this);

        Map<String,Object> pruneConfiguration = new HashMap<>(1);
        pruneConfiguration.put(PruneJob.UPLOAD_WORKER_MANAGER, this);
        scheduler.schedule(this.getClass().getSimpleName() + "_prune", PruneJob.class, pruneIntervalSeconds, pruneConfiguration);
    }

    public void stop() {
        LOG.info("Stopping {}.",UploadWorkerManager.class.getSimpleName());
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
    public static class PruneJob implements Job
    {
        public static final String UPLOAD_WORKER_MANAGER = "upload_worker_manager";

        public void execute(JobExecutionContext context) throws JobExecutionException {
            JobDataMap m = context.getMergedJobDataMap();
            UploadWorkerManager mgr = (UploadWorkerManager) m.get(UPLOAD_WORKER_MANAGER);


            Iterator<LogFile> logFileIterator = mgr.tracker.findMine(UPLOADING);

            try {
                while (logFileIterator.hasNext()) {
                    LogFile logFile = logFileIterator.next();
                    //claim check not required for thread safety, but avoid spurious WARNs
                    if (!mgr.isClaimed(logFile)) {
                        LOG.warn("Found stale UPLOADING record for {}. Perhaps server was previously terminated while uploading it. Queueing upload.", logFile.getOriginPath());
                        mgr.processAsync(logFile);
                    }
                }

                logFileIterator = mgr.tracker.findMine(PREPARED);
                while (logFileIterator.hasNext()) {
                    LogFile logFile = logFileIterator.next();
                    if (!mgr.isClaimed(logFile)) {
                        LOG.warn("Found stale PREPARED record for {}. Perhaps server was previously terminated before uploading it. Queueing upload.", logFile.getOriginPath());
                        mgr.processAsync(logFile);
                    }
                }

                logFileIterator = mgr.tracker.findMine(UPLOAD_ERROR);
                while (logFileIterator.hasNext()) {
                    LogFile logFile = logFileIterator.next();
                    //No need for claim check because UPLOAD_ERROR implies listener-based processing has terminated
                    LOG.warn("Found UPLOAD_ERROR record for {}. Perhaps error was transient. Retrying.", logFile.getOriginPath());
                    mgr.processAsync(logFile);
                }
            } catch (Exception e) {
                throw new JobExecutionException("Failure pruning upload jobs.", e);
            }
        }
    }
}