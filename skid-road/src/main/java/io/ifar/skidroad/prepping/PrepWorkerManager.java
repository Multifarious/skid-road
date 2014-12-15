package io.ifar.skidroad.prepping;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.health.HealthCheck;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractScheduledService;
import io.ifar.goodies.IterableIterator;
import io.ifar.goodies.Iterators;
import io.ifar.goodies.Pair;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileStateListener;
import io.ifar.skidroad.tracking.LogFileTracker;
import org.skife.jdbi.v2.ResultIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
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
    private final static int PEEK_DEPTH = 50; //when randomly selecting an item to retry, how many of the available items to rifle through
    private final LogFileTracker tracker;
    private final PrepWorkerFactory workerFactory;
    private final int retryIntervalSeconds;
    private final int maxConcurrentPrepWork;
    private ExecutorService executor;
    private final Set<String> activeFiles; //either being worked or queued for work in the executor

    public final HealthCheck healthcheck;
    private final AtomicInteger queueDepth = new AtomicInteger(0);

    protected final Gauge<Integer> queueDepthGauge = new Gauge<Integer>() {
        @Override
        public Integer getValue() {
            return queueDepth.get();
        }
    };

    protected final Gauge<Integer> preparingGauge = new Gauge<Integer>() {
        @Override
        public Integer getValue() {
            return tracker.getCount(PREPARING);
        }
    };

    protected final Gauge<Integer> errorGauge = new Gauge<Integer>() {
        @Override
        public Integer getValue() {
            return tracker.getCount(PREP_ERROR);
        }
    };

    protected final Meter errorMeter = new Meter();
    protected final Meter successMeter = new Meter();
    private RetryJob retryJob;

    /**
     * @param workerFactory Provides workers to perform the LogFile uploads.
     * @param tracker Provides access to LogFile metadata.
     * @param retryIntervalSeconds How often to look for files that can be retried.
     * @param maxConcurrentWork Size of thread pool executing the workers.
     * @param unhealthyQueueDepthThreshold HealthCheck returns unhealthy when work queue reaches this size.
     */
    public PrepWorkerManager(LogFileTracker tracker, PrepWorkerFactory workerFactory, int retryIntervalSeconds, int maxConcurrentWork, final int unhealthyQueueDepthThreshold) {
        this.tracker = tracker;
        this.workerFactory = workerFactory;
        this.retryIntervalSeconds = retryIntervalSeconds;
        this.maxConcurrentPrepWork = maxConcurrentWork;
        this.activeFiles = new HashSet<String>();

        this.healthcheck = new HealthCheck() {
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
        LOG.trace("State change received: {} for {}", logFile.getState(), logFile);
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

    /**
     * Wraps increment/decrement of queueDepth and release of claim around provided worker
     * @param worker
     * @param logFile
     * @return
     */
    private Callable<Boolean> wrapWorker(final Callable<Boolean> worker, final LogFile logFile) {
        return new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
               LOG.debug("Preparing {} from {}.", logFile, logFile.getOriginPath());
               try {
                   return worker.call();
               } finally {
                   queueDepth.decrementAndGet();
                   release(logFile);
               }
            }
        };
    }

    /**
     * Synchronously processes the provided LogFile. Used by by retry handler for testing of whether
     * uploads are working again.
     * @param logFile
     * @return True if successful, False if LogFile could not be claimed. Exception thrown if failed.
     */
    private Boolean processSync(final LogFile logFile) throws Exception {
        if (claim(logFile)) {
            queueDepth.incrementAndGet();
            return wrapWorker(workerFactory.buildWorker(logFile, tracker),logFile).call();
        } else {
            LOG.trace("{} is already being prepped on another thread. No-op on this thread.", logFile);
            return Boolean.FALSE;
        }
    }

    private void processAsync(final LogFile logFile) {
        if (claim(logFile)) {
            final Callable<Boolean> worker = wrapWorker(workerFactory.buildWorker(logFile, tracker), logFile);
            queueDepth.incrementAndGet();
            executor.submit(worker);
        } else {
            LOG.trace("{} is already being prepped on another thread. No-op on this thread.", logFile);
        }
    }

    public void start() {
        LOG.info("Starting {}.", PrepWorkerManager.class.getSimpleName());
        this.executor = new ThreadPoolExecutor(0, maxConcurrentPrepWork,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
        tracker.addListener(this);

        retryJob = new RetryJob();
        retryJob.startAsync();
        retryJob.awaitRunning();
        LOG.info("Started {}.", PrepWorkerManager.class.getSimpleName());

    }

    public void stop() {
        LOG.info("Stopping {}.",PrepWorkerManager.class.getSimpleName());
        if (retryJob != null) {
            retryJob.stopAsync();
            retryJob.awaitTerminated();
        }
        tracker.removeListener(this);
        this.executor.shutdown();
        // TODO: Ensure that threads exit.
        LOG.info("Stopping {}.",PrepWorkerManager.class.getSimpleName());
    }

    /**
     * Manages concurrency between listener-invoked and scheduler-invoked processing
     * @return true if this LogFile may be processed
     */
    private boolean claim(LogFile f) {
        synchronized (activeFiles) {
            boolean claimed = activeFiles.add(f.getID());
            LOG.trace("{}ble to claim {} on thread {}.",claimed? "A" : "Una", f.getID(),Thread.currentThread().getId());
            return claimed;
        }
    }

    /**
     * Releases processing lock on specified LogFile.
     * Caller responsible for ensuring that release is called if and only if claim returned true
     */
    private void release(LogFile f) {
        synchronized (activeFiles) {
            activeFiles.remove(f.getID());
            LOG.trace("Released {} on thread {}.",f.getID(),Thread.currentThread().getId());
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

    /**
     * Guards against situation where there are many files to retry and retries are failing. Attempt one and,
     * if it succeeds, retry the others. Otherwise wait. Selection of one to try is random within the first
     * PEEK_DEPTH records returned by teh database. This avoids iterating the whole result set.
     */
    public void retryOneThenRetryAll() throws Exception {
        try (ResultIterator<LogFile> iterator = tracker.findMine(ImmutableSet.of(PREPARING,WRITTEN,PREP_ERROR))) {
            Pair<LogFile,IterableIterator<LogFile>> oneSelected = Iterators.takeOneFromTopN(iterator, PEEK_DEPTH);
            if (oneSelected.left != null) {
                //claim check not required for thread safety, but avoid spurious WARNs about retrying items while they are in-flight for the first time
                LogFile trialLogFile = oneSelected.left;
                if (!isClaimed(trialLogFile)) {
                    try {
                        logRetryMessageForState(trialLogFile);
                        if (processSync(trialLogFile)) {
                            if (oneSelected.right.hasNext()) {
                                LOG.info("First retry succeeded. Will queue others.");
                                for (LogFile logFile : oneSelected.right) {
                                    //claim check not required for thread safety, but avoid spurious WARNs about retrying items while they are in-flight for the first time
                                    if (!isClaimed(logFile)) {
                                        logRetryMessageForState(logFile);
                                        processAsync(logFile);
                                    }
                                }
                            } else {
                                LOG.info("First retry succeeded. There are no others left.");
                            }
                        } else {
                            LOG.warn("Another thread claimed {} since last checked. Are two retry jobs running at once?", trialLogFile);
                        }
                    } catch (Exception e) {
                        if (oneSelected.right.hasNext()) {
                            LOG.warn("Retry of trial record {} failed. Will not try any others this time.", trialLogFile);
                        } else {
                            LOG.warn("Retry of trial record {} failed. There are no others left.", trialLogFile);
                        }
                    }
                }
            }
        }
    }


    private void logRetryMessageForState(LogFile logFile) {
        switch (logFile.getState()) {
            case WRITTEN:
                LOG.info("Found stale {} record for {}. Perhaps server was previously terminated before preparing it.",
                        logFile.getState(),
                        logFile);
                break;
            case PREPARING:
                LOG.info("Found stale {} record for {}. Perhaps server was previously terminated while preparing it.",
                        logFile.getState(),
                        logFile);
                break;
            case PREP_ERROR:
                LOG.info("Found {} record for {}. Perhaps a transient error occurred while preparing it.",
                        logFile.getState(),
                        logFile);
                break;
            default:
                throw new IllegalStateException(String.format("Did not expect to be processing %s record for %s. Bug!", logFile.getState(), logFile));
        }
    }


    public class RetryJob extends AbstractScheduledService
    {

        @Override
        protected void runOneIteration() throws Exception {
            try {
                retryOneThenRetryAll();
            } catch (Exception e) {
                LOG.error("Unable to complete retry invocation due to unexpected exception: ({}) {}",
                        e.getClass(), e.getMessage(), e);
            }
        }

        @Override
        protected Scheduler scheduler() {
            return Scheduler.newFixedDelaySchedule(0L, retryIntervalSeconds, TimeUnit.SECONDS);
        }
    }
}
