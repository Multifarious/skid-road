package io.ifar.skidroad.writing;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A WritingWorkerFactory which creates dummy workers and provides explict controls over when the workers run, consume
 * queue items, and exit.
 */
public class DummyWritingWorkerFactory<T> implements WritingWorkerFactory<T> {

    public static class DummyWorker<T> implements Runnable {
        private final BlockingQueue<T> queue;
        private final CountDownLatch runLatch;
        private final AtomicBoolean drainFlag;
        private final CountDownLatch exitLatch;

        public DummyWorker(BlockingQueue<T> queue, CountDownLatch runLatch, AtomicBoolean drainFlag, CountDownLatch exitLatch) {
            this.queue = queue;
            this.runLatch = runLatch;
            this.drainFlag = drainFlag;
            this.exitLatch = exitLatch;
        }

        private final static Logger LOG = LoggerFactory.getLogger(DummyWorker.class);
        @Override
        public void run() {
            try {
                LOG.debug("Worker {} is alive.", Thread.currentThread().getName());
                if (!runLatch.await(1, TimeUnit.SECONDS))
                    LOG.error("Unreported test failure; took too long for run latch to be released.");
                if (drainFlag.get()) {
                    queue.drainTo(new ArrayList<T>());
                    LOG.debug("Worker {} drained queue, now awaiting exit.", Thread.currentThread().getName());
                } else {
                    LOG.debug("Worker {} leaving {} items in queue and awaiting exit.", Thread.currentThread().getName(), queue.size());
                }
                if (!exitLatch.await(1, TimeUnit.SECONDS))
                    LOG.error("Unreported test failure; took too long for exit latch to be released.");
            } catch (InterruptedException e) {
                LOG.debug("Worker {} has been asked to exit.", Thread.currentThread().getName());
            }
            LOG.debug("Worker {} exiting.", Thread.currentThread().getName());
        }
    }


    private List<CountDownLatch> creationLatches = new LinkedList<>();
    private List<Thread> threadsCreated = new ArrayList<>();
    private Map<BlockingQueue<T>, CountDownLatch> runLatches = new LinkedHashMap<>();
    private Map<BlockingQueue<T>, AtomicBoolean> drainFlags = new LinkedHashMap<>();
    private Map<BlockingQueue<T>, CountDownLatch> exitLatches = new LinkedHashMap<>();

    @Override
    public Thread buildWorker(final BlockingQueue<T> queue, Serializer<T> serializer, LogFile logFileRecord, LogFileTracker tracker) {
        if (!runLatches.containsKey(queue)) {
            runLatches.put(queue, new CountDownLatch(1));
            drainFlags.put(queue, new AtomicBoolean(false));
            exitLatches.put(queue, new CountDownLatch(1));
        }
        CountDownLatch runLatch = runLatches.get(queue);
        AtomicBoolean drainFlag = drainFlags.get(queue);
        CountDownLatch exitLatch = exitLatches.get(queue);

        Thread result = new Thread(new DummyWorker<T>(queue,runLatch,drainFlag,exitLatch), getClass().getSimpleName() + "_" + threadsCreated.size());

        for (CountDownLatch creationLatch : creationLatches)
            creationLatch.countDown();
        threadsCreated.add(result);
        return result;

    }

    /**
     * Create a latch that will be released when the specified number of new Threads have been created.
     */
    public CountDownLatch getCreationLatch(int creationsToExpect) {
        CountDownLatch latch = new CountDownLatch(creationsToExpect);
        creationLatches.add(latch);
        return latch;
    }

    /**
     * Allows workers to drain the oldest not-yet-drained queue.
     * Note: queue is drained once and then the workers just await exit. Add more workers to drain subsequent items.
     */
    public void drainNextQueue() {
        Map.Entry<BlockingQueue<T>,AtomicBoolean> drain = drainFlags.entrySet().iterator().next();
        drain.getValue().set(true);
        Map.Entry<BlockingQueue<T>,CountDownLatch> run = runLatches.entrySet().iterator().next();
        run.getValue().countDown();
    }

    /**
     * Allows workers for the oldest still-running queue to exit.
     */
    public void exitNextQueue() {
        Map.Entry<BlockingQueue<T>,CountDownLatch> run = runLatches.entrySet().iterator().next();
        run.getValue().countDown();
        Map.Entry<BlockingQueue<T>,CountDownLatch> exit = exitLatches.entrySet().iterator().next();
        exit.getValue().countDown();
    }

    public List<Thread> getThreadsCreated() {
        return threadsCreated;
    }

    /**
     * Drain all queues and let all worker threads exit.
     */
    public void stop() {
        for (Map.Entry<BlockingQueue<T>,CountDownLatch> entry : runLatches.entrySet()) {
            entry.getKey().drainTo(new ArrayList<T>());
            entry.getValue().countDown();
        }
        for (CountDownLatch latch : exitLatches.values())
            latch.countDown();
    }
}
