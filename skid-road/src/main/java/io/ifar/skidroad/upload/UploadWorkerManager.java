package io.ifar.skidroad.upload;

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
 * Manages UploadWorkers.
 *
 * Note there is no UploadWorker interface. Uses a UploadWorkerFactory
 * to create worker runnables.
 *
 * TODO (future): Nearly identical code to PrepWorkerManager. Consolidate?
 */
public class UploadWorkerManager implements LogFileStateListener {
    private final static Logger LOG = LoggerFactory.getLogger(UploadWorkerManager.class);
    private final static int PEEK_DEPTH = 50; //when randomly selecting an item to retry, how many of the available items to rifle through

    private final UploadWorkerFactory workerFactory;
    private final LogFileTracker tracker;
    public final int retryIntervalSeconds;
    private final int maxConcurrentUploads;
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

    protected final Gauge<Integer> uploadingGauge = new Gauge<Integer>() {
        @Override
        public Integer getValue() {
            return tracker.getCount(UPLOADING);
        }
    };

    protected final Gauge<Integer> errorGauge = new Gauge<Integer>() {
        @Override
        public Integer getValue() {
            return tracker.getCount(UPLOAD_ERROR);
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
    public UploadWorkerManager(UploadWorkerFactory workerFactory, LogFileTracker tracker, int retryIntervalSeconds, int maxConcurrentWork,  final int unhealthyQueueDepthThreshold) {
        this.workerFactory = workerFactory;
        this.tracker = tracker;
        this.retryIntervalSeconds = retryIntervalSeconds;
        this.maxConcurrentUploads = maxConcurrentWork;
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
               LOG.debug("Uploading {} from {}.", logFile, logFile.getPrepPath());
               queueDepth.incrementAndGet();
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
                return wrapWorker(workerFactory.buildWorker(logFile, tracker),logFile).call();
        } else {
            LOG.trace("{} is already being uploaded on another thread. No-op on this thread.", logFile);
            return Boolean.FALSE;
        }
    }

    /**
     * Submits provided LogFile for async upload processing
     */
    private void processAsync(final LogFile logFile) {
        if (claim(logFile)) {
            final Callable<Boolean> worker = wrapWorker(workerFactory.buildWorker(logFile, tracker), logFile);
            executor.submit(worker);
        } else {
            LOG.trace("{} is already being uploaded on another thread. No-op on this thread.", logFile);
        }
    }

    public void start() {
        LOG.info("Starting {}.", UploadWorkerManager.class.getSimpleName());
        this.executor = new ThreadPoolExecutor(0, maxConcurrentUploads,
                                      60L, TimeUnit.SECONDS,
                                      new LinkedBlockingQueue<Runnable>());
        tracker.addListener(this);

        retryJob = new RetryJob();
        retryJob.startAsync();
        retryJob.awaitRunning();
        LOG.info("Started {}.", UploadWorkerManager.class.getSimpleName());
    }

    public void stop() {
        LOG.info("Stopping {}.",UploadWorkerManager.class.getSimpleName());
        tracker.removeListener(this);
        if (retryJob != null) {
            retryJob.stopAsync();
            retryJob.awaitTerminated();
        }
        this.executor.shutdown();
        LOG.info("Stopped {}.", UploadWorkerManager.class.getSimpleName());
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

    /**
     * Guards against situation where there are many files to retry and retries are failing. Attempt one and,
     * if it succeeds, retry the others. Otherwise wait. Selection of one to try is random within the first
     * PEEK_DEPTH records returned by the database. This avoids iterating the whole result set.
     */
    public void retryOneThenRetryAll() throws Exception {
        try (ResultIterator<LogFile> iterator = tracker.findMine(ImmutableSet.of(UPLOADING, PREPARED, UPLOAD_ERROR))){
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
            case PREPARED:
                LOG.info("Found stale {} record for {}. Perhaps server was previously terminated before uploading it.",
                        logFile.getState(),
                        logFile);
                break;
            case UPLOADING:
                LOG.info("Found stale {} record for {}. Perhaps server was previously terminated while uploading it.",
                        logFile.getState(),
                        logFile);
                break;
            case UPLOAD_ERROR:
                LOG.info("Found {} record for {}. Perhaps a transient error occurred while uploading it.",
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
                        e.getClass().getSimpleName(), e.getMessage(), e);
            }
        }

        @Override
        protected Scheduler scheduler() {
            return Scheduler.newFixedDelaySchedule(0L, retryIntervalSeconds, TimeUnit.SECONDS);
        }

    }
}
