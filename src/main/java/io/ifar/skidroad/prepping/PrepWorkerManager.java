package io.ifar.skidroad.prepping;

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
import java.util.concurrent.Executors;
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
    private final ExecutorService executor;
    public final HealthCheck healthcheck;
    private final PrepWorkerFactory workerFactory;

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

    public PrepWorkerManager(LogFileTracker tracker, PrepWorkerFactory workerFactory, final int unhealthyQueueDepthThreshold) {
        this.tracker = tracker;
        this.workerFactory = workerFactory;
        this.executor = Executors.newCachedThreadPool();
        this.healthcheck = new HealthCheck("prep_worker_manager") {
            protected Result check() throws Exception {
                //Would be better to measure latency than depth, but that's more expensive.
                if (queueDepth.get() < unhealthyQueueDepthThreshold)
                    return Result.healthy(String.format("%d files queued or in-flight.", queueDepth.get()));
                else
                    return Result.healthy(String.format("%d files queued or in-flight exceeds threshold (%d).", queueDepth.get(), unhealthyQueueDepthThreshold));
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
                errorCount.inc();
                break;
            default:
                //ignore
        }
    }
    private void processAsync(final LogFile logFile) {
        LOG.debug("Preparing {} from {}.", logFile, logFile.getOriginPath());
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
        LOG.info("Starting {}.", PrepWorkerManager.class.getSimpleName());
        tracker.addListener(this);

        Iterator<LogFile> logFileIterator = tracker.findMine(PREPARING);
        while (logFileIterator.hasNext()) {
            LogFile logFile = logFileIterator.next();
            LOG.warn("Found stale PREPARING record for {}. Perhaps server was previously terminated while preparing it. Queueing preparation.", logFile.getOriginPath());
            processAsync(logFile);
        }

        logFileIterator = tracker.findMine(WRITTEN);
        while (logFileIterator.hasNext()) {
            LogFile logFile = logFileIterator.next();
            LOG.warn("Found WRITTEN record for {}. Perhaps server was previously terminated before preparing it. Queueing preparation.", logFile.getOriginPath());
            processAsync(logFile);
        }
    }

    public void stop() {
        LOG.info("Stopping {}.",PrepWorkerManager.class.getSimpleName());
        tracker.removeListener(this);
        this.executor.shutdown();
    }
}
