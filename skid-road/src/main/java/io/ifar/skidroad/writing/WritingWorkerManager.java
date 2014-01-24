package io.ifar.skidroad.writing;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.HealthCheck;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.rolling.FileRollingScheme;
import io.ifar.skidroad.scheduling.SimpleQuartzScheduler;
import io.ifar.skidroad.tracking.LogFileTracker;
import org.joda.time.DateTime;
import org.quartz.*;
import org.skife.jdbi.v2.ResultIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static io.ifar.skidroad.tracking.LogFileState.WRITING;

/**
 * Manages writing workers and the input queues to feed data into them.
 *
 * This is the main main API integration point for submitting data. Skid-road clients construct a
 * WritingWorkerManager and then submit data via the {@link #record(org.joda.time.DateTime, Object)} method.
 *
 * Creates workers sparingly (since each one results in a new file).
 *
 * Handles worker lifecycle including applying the behavior dictated by
 * provided FileRollingScheme.
 *
 * Note there is no WritingWorker interface. Uses a WritingWorkerFactory
 * to create worker threads.
 */
public class WritingWorkerManager<T> {
    private static final Logger LOG = LoggerFactory.getLogger(WritingWorkerManager.class);
    private static final int NUM_PROCESSORS = Runtime.getRuntime().availableProcessors();
    private static final AtomicInteger instanceCounter = new AtomicInteger(0);

    public final HealthCheck healthcheck;

    private final FileRollingScheme rollingScheme;
    private final LogFileTracker tracker;
    private final WritingWorkerFactory<T> factory;
    private final int spawnNewWorkerAtQueueDepth;
    private final int unhealthyQueueDepthThreshold;
    private final SimpleQuartzScheduler scheduler;
    private final int pruneIntervalSeconds;
    /*
    queues is synchronized using itself as a monitor. Only getQueueFor puts new data; accessed concurrently.
    Only prune removes removes data; runs single-threaded.
     */
    private final Map<DateTime,BlockingQueue<T>> queues;
    /*
    works is synchronized using itself as a monitor. Only launchNewWorker puts new data; accessed concurrently.
    Only prune alters the List values or removes keys; runs single-threaded.
     */
    private final Map<DateTime, List<Thread>> workers;
    private final ExecutorService asyncWorkerCreator;

    public WritingWorkerManager(FileRollingScheme rollingScheme, LogFileTracker tracker,
                                WritingWorkerFactory<T> factory, SimpleQuartzScheduler scheduler, int pruneIntervalSeconds,
                                int spawnThreshold, int unhealthyThreshold) {

        File logDir = rollingScheme.getBaseDirectory();
        logDir.mkdirs();
        if (!logDir.exists() || !logDir.canWrite())
            throw new IllegalStateException("Cannot create or cannot write to output path " + logDir.getPath());

        this.rollingScheme = rollingScheme;
        this.tracker = tracker;
        this.factory = factory;
        this.spawnNewWorkerAtQueueDepth = spawnThreshold;
        this.unhealthyQueueDepthThreshold = unhealthyThreshold;
        this.scheduler = scheduler;
        this.pruneIntervalSeconds = pruneIntervalSeconds;
        this.queues = new HashMap<>();
        this.workers = new HashMap<>();
        this.asyncWorkerCreator = Executors.newSingleThreadExecutor();

        this.healthcheck = new HealthCheck("writing_worker_manager") {
            protected Result check() throws Exception {
                //Would be better to measure latency than depth, but that's more expensive.
                int queueDepth = queueDepthGauge.value();
                int queueCount = queueCountGauge.value();
                int workerCount = workerCountGauge.value();
                if (queueDepth < unhealthyQueueDepthThreshold)
                    return Result.healthy(String.format("%d items across %d queues with %d workers.", queueDepth, queueCount, workerCount));
                else
                    return Result.unhealthy(String.format("%d items across %d queues exceeds depth threshold (%d); %d workers", queueDepth, queueCount, unhealthyQueueDepthThreshold, workerCount));
            }
        };
    }

    private final Gauge<Integer> queueCountGauge = Metrics.newGauge(this.getClass(),
            "queue_count",
            new Gauge<Integer>() {
                @Override
                public Integer value() {
                    //skip synchronization, allow stale data.
                    return queues.size();
                }
            });

    private final Gauge<Integer> queueDepthGauge = Metrics.newGauge(this.getClass(),
            "queue_depth",
            new Gauge<Integer>() {
                @Override
                public Integer value() {
                    int sum = 0;
                    synchronized (queues) {
                        for(BlockingQueue<T> q : queues.values())
                            sum += q.size();
                    }
                    return sum;
                }
            });

    private final Gauge<Integer> workerCountGauge = Metrics.newGauge(this.getClass(),
            "worker_count",
                new Gauge<Integer>() {
                @Override
                public Integer value() {
                    //skip synchronization, allow stale data
                    return workers.size();
                }
            });

    /**
     * Submit an item to be recorded in a log file.
     * @param timeStamp Time to attribute item to
     * @param item
     */
    public void record(DateTime timeStamp, T item) {
        record(timeStamp.getMillis(), item);
    }

    /**
     * Submit an item to be recorded in a log file.
     * @param timeStamp Time to attribute item to
     * @param item
     */
    public void record(long timeStamp, T item) {
        getQueueFor(timeStamp).add(item);
    }

    /**
     * Submit an item to be recorded in a log file.
     * @param item
     */
    public void record(T item) {
        getQueueFor(System.currentTimeMillis()).add(item);
    }

    protected BlockingQueue<T> getQueueFor(DateTime timeStamp) {
        return getQueueFor(timeStamp.getMillis());
    }

    protected BlockingQueue<T> getQueueFor(long timeStamp) {
        final DateTime startTime = rollingScheme.getStartTime(timeStamp);
        final BlockingQueue<T> queue;
        boolean shouldLaunchNewWorker = false;

        synchronized (queues) {
            BlockingQueue<T> existing = queues.get(startTime);
            if (existing == null) {
                queue = new LinkedBlockingQueue<>();
                queues.put(startTime, queue);
                shouldLaunchNewWorker = true;
            } else {
                queue = existing;
            }
        }

        if (shouldLaunchNewWorker) {
            asyncWorkerCreator.submit(new Runnable() {
                public void run() {
                    try {
                        launchNewWorker(queue, startTime, 1);
                    } catch (Exception e) {
                        LOG.error("Could not launch WritingWorker", e);
                    }
                }
            });
        }

        return queue;
    }

    /**
     * Closes and creates workers as needed depending on rolling scheme and queue depth.
     */
    protected void prune() {
        LOG.trace("Pruning...");
        //Clear out any entries for workers that are no longer alive
        Set<Map.Entry<DateTime, List<Thread>>> workerEntries;
        //This is the only method that deletes from workers, and it is not run concurrently.
        //So sufficient to guard iteration against insertion of new data by getQueueFor.
        //Re-acquire lock before making changes.
        synchronized (workers) {
            workerEntries = new HashSet<>(workers.entrySet());
        }
        for (Map.Entry<DateTime, List<Thread>> entry : workerEntries) {
            List<Thread> zombies = new LinkedList<>();
            for(Thread worker : entry.getValue())
                if (!worker.isAlive())
                    zombies.add(worker);
            if (zombies.isEmpty()) {
                //do nothing
            } else if (zombies.size() == entry.getValue().size()) {
                LOG.debug("The {} worker(s) for {} have all exited.", zombies.size(), rollingScheme.getRepresentation(entry.getKey()));
                synchronized (workers) {
                    workers.remove(entry.getKey());
                }
            } else {
                LOG.debug("{} of the {} worker(s) for {} have exited.", zombies.size(), entry.getValue().size(), rollingScheme.getRepresentation(entry.getKey()));
                synchronized (workers) {
                    entry.getValue().removeAll(zombies);
                }
            }
        }

        Set<Map.Entry<DateTime,BlockingQueue<T>>> queueEntries;
        //This is the only method that deletes from queues, and it is not run concurrently.
        //So sufficient to guard iteration against insertion of new data by getQueueFor.
        //Re-acquire lock before making changes.
        synchronized(queues) {
            queueEntries = new HashSet<>(queues.entrySet());
        }
        for (Map.Entry<DateTime,BlockingQueue<T>> entry : queueEntries) {
            DateTime startTime = entry.getKey();
            BlockingQueue<T> queue = entry.getValue();

            if (rollingScheme.isTimeToClose(startTime) && queue.isEmpty()) {
                //There is no work left. Shut down the workers.
                List<Thread> workersForQueue = getWorkersForStartTime(startTime);
                if (workersForQueue.isEmpty()) {
                    LOG.debug("Done with stale queue {}", rollingScheme.getRepresentation(startTime));
                    synchronized (queues) {
                        queues.remove(startTime);
                    }
                } else {
                    LOG.debug("Closing {} remaining workers for stale queue {}", workersForQueue.size(), rollingScheme.getRepresentation(startTime));
                    for (Thread worker : workersForQueue)
                        worker.interrupt();
                }
            } else if (!queue.isEmpty()) {
                //There is work left. Ensure sufficient worker count.
                List<Thread> workersForQueue = getWorkersForStartTime(startTime);
                boolean needAnotherWorker = workersForQueue.isEmpty() || (queue.size() >= spawnNewWorkerAtQueueDepth && workersForQueue.size() < NUM_PROCESSORS);
                if (needAnotherWorker)
                    launchNewWorker(queue, startTime, workersForQueue.size() + 1);
            }
        }
    }

    protected List<Thread> getWorkersForStartTime(DateTime startTime) {
        LinkedList<Thread> result = new LinkedList<>();
        synchronized (workers) {
            List<Thread> l = workers.get(startTime);
            if (l!=null)
                result.addAll(l);
        }
        return result;
    }

    /**
     * CReates a new worker to consume from the provided queue.
     * @param queue queue from which worker will fetch events
     * @param startTime used to generate filename for worker's output
     * @param maxCount abort if queue already has this many workers
     */
    protected void launchNewWorker(BlockingQueue<T> queue, DateTime startTime, int maxCount) {
        synchronized (workers) {
            List<Thread> workersForQueue = workers.get(startTime);
            int currentCount = workersForQueue == null ? 0 : workersForQueue.size();
            if (currentCount < maxCount) {
                LOG.debug("Launching new worker for {}", rollingScheme.getRepresentation(startTime));
                String logFilePathPattern = rollingScheme.makeOutputPathPattern(startTime);
                LogFile logFileRecord = tracker.open(rollingScheme.getRepresentation(startTime), logFilePathPattern, startTime);
                Thread worker = factory.buildWorker(queue, logFileRecord, tracker);
                if (workersForQueue == null) {
                    workersForQueue = new LinkedList<>();
                    workers.put(startTime,workersForQueue);
                }
                workersForQueue.add(worker);
                worker.start();
            } else {
                LOG.debug("Skip launch of new worker for {}; already have {}.", rollingScheme.getRepresentation(startTime), maxCount);
            }
        }
    }

    public void start() throws Exception {
        LOG.info("Starting {}.",WritingWorkerManager.class.getSimpleName());

        //On startup, look for database records that were left hanging and tidy up.
        cleanStaleEntries();

        Map<String,Object> pruneConfiguration = new HashMap<>(1);
        pruneConfiguration.put(PruneJob.FILE_WRITING_WORKER_MANAGER, this);
        scheduler.schedule(this.getClass().getSimpleName()+"_prune_"+instanceCounter.incrementAndGet(), PruneJob.class, pruneIntervalSeconds * 1000, pruneConfiguration);
    }

    /**
     * Find any WRITING entries, presume they are stale, and move them to WRITTEN or WRITE_ERROR state. This method should
     * only be called when the server is not running (or just starting up), lest it stomp on a WRITING entry which is
     * in-progress.
     * @throws Exception
     */
    private void cleanStaleEntries() throws Exception {
        try (ResultIterator<LogFile> staleEntries = tracker.findMine(WRITING)){
            while (staleEntries.hasNext()) {
                LogFile staleEntry = staleEntries.next();
                if (Files.exists(staleEntry.getOriginPath())) {
                    LOG.warn("Found stale WRITING record for {}. Data exists on disk, marking WRITTEN.", staleEntry.getOriginPath());
                    tracker.written(staleEntry); //ignore update failures
                } else {
                    LOG.warn("Found stale WRITING record for {}. No data exists on disk, marking WRITE_ERROR.", staleEntry.getOriginPath());
                    tracker.writeError(staleEntry); //ignore update failures
                }
            }
        }
    }

    public void stop() throws InterruptedException {
        LOG.info("Stopping {}.",WritingWorkerManager.class.getSimpleName());
        //I believe the DropWizard lifecycle is:
        //stop taking requests, then shutdown the Managed resources in reverse-startup
        //order. In this case, the way to do a graceful shutdown is to wait for the queues
        //to drain before shutting down the workers.
        int remainingItems = Integer.MAX_VALUE;
        while (remainingItems > 0) {
            remainingItems = 0;
            for (Queue<T> q : queues.values())
                remainingItems += q.size();
            if (remainingItems > 0) {
                LOG.info("Waiting on queues to drain. {} items remaining.", remainingItems);
                Thread.sleep(1000L);
            }
        }
        LOG.info("Queues drained. Stopping workers.");
        int count=0;
        synchronized (workers) {
        for (List<Thread> workerList : workers.values())
            for (Thread worker : workerList) {
                worker.interrupt();
                count++;
            }
        }
        for (Thread worker : getWorkerThreadSnapshot()) {
            worker.join();
            LOG.debug("Stopped one. {} remain.", --count);
        }
        LOG.info("All workers stopped.");
    }

    /**
     * For testing. :-/
     */
    protected Set<Thread> getWorkerThreadSnapshot() {
        Set<Thread> result = new HashSet<>();
        synchronized (workers) {
            for (List<Thread> workersForStartTime : workers.values())
                result.addAll(workersForStartTime);
        }
        return result;
    }

    @DisallowConcurrentExecution
    public static class PruneJob implements Job
    {
        public static final String FILE_WRITING_WORKER_MANAGER = "file_writing_worker_manager";

        public void execute(JobExecutionContext context) throws JobExecutionException {
            JobDataMap m = context.getMergedJobDataMap();
            WritingWorkerManager mgr = (WritingWorkerManager) m.get(FILE_WRITING_WORKER_MANAGER);
            if (mgr != null)
                mgr.prune();
        }
    }
}
