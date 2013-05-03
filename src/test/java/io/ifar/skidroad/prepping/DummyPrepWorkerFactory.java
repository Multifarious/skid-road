package io.ifar.skidroad.prepping;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A WritingWorkerFactory which creates dummy workers and provides explict controls over when the workers run, consume
 * queue items, and exit.
 */
public class DummyPrepWorkerFactory implements PrepWorkerFactory {
    private final static Logger LOG = LoggerFactory.getLogger(DummyPrepWorkerFactory.class);

    public static class DummyWorker<T> implements Runnable {
        private final CountDownLatch runLatch;
        private final CountDownLatch exitLatch;

        public DummyWorker(CountDownLatch runLatch, CountDownLatch exitLatch) {
            this.runLatch = runLatch;
            this.exitLatch = exitLatch;
        }

        private final static Logger LOG = LoggerFactory.getLogger(DummyWorker.class);
        @Override
        public void run() {
            try {
                LOG.debug("Worker {} is alive.", Thread.currentThread().getName());
                if (!runLatch.await(1, TimeUnit.SECONDS))
                    LOG.error("Unreported test failure; took too long for run latch to be released.");
                if (!exitLatch.await(1, TimeUnit.SECONDS))
                    LOG.error("Unreported test failure; took too long for exit latch to be released.");
            } catch (InterruptedException e) {
                LOG.debug("Worker {} has been asked to exit.", Thread.currentThread().getName());
            }
            LOG.debug("Worker {} exiting.", Thread.currentThread().getName());
        }
    }


    private List<CountDownLatch> runCountLatches = new LinkedList<>();
    private final List<CountDownLatch> exitLatches = new ArrayList<>();

    @Override
    public Runnable buildWorker(LogFile logFileRecord, LogFileTracker tracker) {
        final CountDownLatch latch = new CountDownLatch(1);
        synchronized (exitLatches) {
            exitLatches.add(latch);
        }

        Runnable worker = new Runnable() {
            @Override
            public void run() {
                try {
                    LOG.debug("Worker {} is alive.", Thread.currentThread().getName());
                    for (CountDownLatch creationLatch : runCountLatches)
                        creationLatch.countDown();
                    if (!latch.await(1, TimeUnit.SECONDS))
                        LOG.error("Unreported test failure; took too long for exit latch to be released.");
                } catch (InterruptedException e) {
                    LOG.debug("Worker {} has been asked to exit.", Thread.currentThread().getName());
                }
            }
        };
        return worker;
    }

    /**
     * Create a latch that will be released when the specified number of new workers have been run.
     */
    public CountDownLatch getRunCountLatch(int creationsToExpect) {
        CountDownLatch latch = new CountDownLatch(creationsToExpect);
        runCountLatches.add(latch);
        return latch;
    }

    /**
     * Allows next worker to exit
     */
    public void exitNextWorker() {
        synchronized (exitLatches) {
            CountDownLatch latch = exitLatches.remove(0);
            latch.countDown();
        }
    }

    /**
     * Lets all workers exit
     */
    public void stop() {
        synchronized (exitLatches) {
            for (CountDownLatch latch : exitLatches)
                latch.countDown();
        }
    }
}
