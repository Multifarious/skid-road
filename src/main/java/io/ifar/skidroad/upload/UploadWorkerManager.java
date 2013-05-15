package io.ifar.skidroad.upload;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.HealthCheck;
import com.yammer.metrics.core.Meter;
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
    public final int retryIntervalSeconds;
    private final int maxConcurrentUploads;
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

    private final Gauge<Integer> uploadingGauge = Metrics.newGauge(this.getClass(),
            "files_uploading",
            new Gauge<Integer>() {
                @Override
                public Integer value() {
                    return tracker.getCount(UPLOADING);
                }
            });

    private final Gauge<Integer> errorGauge = Metrics.newGauge(this.getClass(),
            "files_in_error",
            new Gauge<Integer>() {
                @Override
                public Integer value() {
                    return tracker.getCount(UPLOAD_ERROR);
                }
            });

    private final Meter errorMeter = Metrics.newMeter(this.getClass(), "upload_errors", "errors", TimeUnit.SECONDS);
    private final Meter successMeter = Metrics.newMeter(this.getClass(), "upload_successes", "successes", TimeUnit.SECONDS);

    public UploadWorkerManager(UploadWorkerFactory workerFactory, LogFileTracker tracker, SimpleQuartzScheduler scheduler, int retryIntervalSeconds, int maxConcurrentUploads,  final int unhealthyQueueDepthThreshold) {
        this.workerFactory = workerFactory;
        this.tracker = tracker;
        this.scheduler = scheduler;
        this.retryIntervalSeconds = retryIntervalSeconds;
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
                errorMeter.mark();
                break;
            case UPLOADED:
                successMeter.mark();
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

        Map<String,Object> retryConfiguration = new HashMap<>(1);
        retryConfiguration.put(RetryJob.UPLOAD_WORKER_MANAGER, this);
        scheduler.schedule(this.getClass().getSimpleName() + "_retry", RetryJob.class, retryIntervalSeconds, retryConfiguration);
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
    public static class RetryJob implements Job
    {
        public static final String UPLOAD_WORKER_MANAGER = "upload_worker_manager";

        public void execute(JobExecutionContext context) throws JobExecutionException {
            JobDataMap m = context.getMergedJobDataMap();
            UploadWorkerManager mgr = (UploadWorkerManager) m.get(UPLOAD_WORKER_MANAGER);



            try {
                Iterator<LogFile> uploadingIterator = mgr.tracker.findMine(UPLOADING);
                while (uploadingIterator.hasNext()) {
                    LogFile logFile = uploadingIterator.next();
                    //claim check not required for thread safety, but avoid spurious WARNs
                    if (!mgr.isClaimed(logFile)) {
                        LOG.warn("Found stale UPLOADING record for {}. Perhaps server was previously terminated while uploading it. Queueing upload.", logFile.getOriginPath());
                        mgr.processAsync(logFile);
                    }
                }

                Iterator<LogFile> preparedIterator = mgr.tracker.findMine(PREPARED);
                while (preparedIterator.hasNext()) {
                    LogFile logFile = preparedIterator.next();
                    if (!mgr.isClaimed(logFile)) {
                        LOG.warn("Found stale PREPARED record for {}. Perhaps server was previously terminated before uploading it. Queueing upload.", logFile.getOriginPath());
                        mgr.processAsync(logFile);
                    }
                }

                Iterator<LogFile> erroredIterator = mgr.tracker.findMine(UPLOAD_ERROR);
                while (erroredIterator.hasNext()) {
                    LogFile logFile = erroredIterator.next();
                    //No need for claim check because UPLOAD_ERROR implies listener-based processing has terminated
                    LOG.warn("Found UPLOAD_ERROR record for {}. Perhaps error was transient. Retrying.", logFile.getOriginPath());
                    mgr.processAsync(logFile);
                }
            } catch (Exception e) {
                //Observed causes:
                // findMine throws org.skife.jdbi.v2.exceptions.UnableToCreateStatementException: org.postgresql.util.PSQLException: This connection has been closed.
                // findMine throws org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException: org.postgresql.util.PSQLException: An I/O error occured while sending to the backend.
                throw new JobExecutionException("Failure pruning upload jobs.", e);
            }
        }
    }
}
