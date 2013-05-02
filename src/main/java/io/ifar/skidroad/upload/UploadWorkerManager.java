package io.ifar.skidroad.upload;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.HealthCheck;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileStateListener;
import io.ifar.skidroad.tracking.LogFileTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
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
    private ExecutorService executor;
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

    private final int maxConcurrentUploads;

    public UploadWorkerManager(UploadWorkerFactory workerFactory, LogFileTracker tracker, int maxConcurrentUploads,  final int unhealthyQueueDepthThreshold) {
        this.maxConcurrentUploads = maxConcurrentUploads;
        this.workerFactory = workerFactory;
        this.tracker = tracker;

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
    }

    public void start() {
        LOG.info("Starting {}.", UploadWorkerManager.class.getSimpleName());
        tracker.addListener(this);


        Iterator<LogFile> logFileIterator = tracker.findMine(UPLOADING);

        this.executor = new ThreadPoolExecutor(0, maxConcurrentUploads,
                                      60L, TimeUnit.SECONDS,
                                      new SynchronousQueue<Runnable>());
        try {
            while (logFileIterator.hasNext()) {
                LogFile logFile = logFileIterator.next();
                LOG.warn("Found stale UPLOADING record for {}. Perhaps server was previously terminated while uploading it. Queueing upload.", logFile.getOriginPath());
                processAsync(logFile);
            }

            logFileIterator = tracker.findMine(PREPARED);
            while (logFileIterator.hasNext()) {
                LogFile logFile = logFileIterator.next();
                LOG.warn("Found PREPARED record for {}. Perhaps server was previously terminated before uploading it. Queueing upload.", logFile.getOriginPath());
                processAsync(logFile);
            }
        } catch (Exception e) {
            stop();
        }
    }

    public void stop() {
        LOG.info("Stopping {}.",UploadWorkerManager.class.getSimpleName());
        tracker.removeListener(this);
        this.executor.shutdown();
    }
}
