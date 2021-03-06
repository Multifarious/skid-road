package io.ifar.skidroad.prepping;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static io.ifar.skidroad.prepping.PrepWorkerManagerTest.MAX_TEST_DURATION;

/**
 * A PrepWorkerFactory which creates dummy workers and provides explict controls over when the workers run, consume
 * queue items, and exit.
 */
public class DummyPrepWorkerFactory implements PrepWorkerFactory {
    private final static Logger LOG = LoggerFactory.getLogger(DummyPrepWorkerFactory.class);
    private long secondsToWait = MAX_TEST_DURATION;

    public void setSecondsToWait(long secondsToWait) {
        this.secondsToWait = secondsToWait;
    }

    /**
     * Created for test methods that want to know when a certain number of runs have occurred.
     */
    private List<CountDownLatch> runCountLatches = new LinkedList<>();
    private final CountDownLatch exitLatch = new CountDownLatch(1);

    @Override
    public Callable<Boolean> buildWorker(final LogFile logFileRecord, LogFileTracker tracker) {
        LOG.debug("Creating worker for " + logFileRecord);
        Callable<Boolean> worker = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                try {
                    LOG.debug("Worker {} is alive.", Thread.currentThread().getName());
                    for (CountDownLatch creationLatch : runCountLatches) {
                        creationLatch.countDown();
                    }
                    if (!exitLatch.await(secondsToWait, TimeUnit.SECONDS)) {
                        if (secondsToWait == MAX_TEST_DURATION) {
                            LOG.error("Unreported test failure; took too long for exit latch to be released.");
                        } else {
                            LOG.info("Completing work after waiting for {} seconds.",secondsToWait);
                        }
                    }
                    LOG.debug("Returning from worker on {}.",logFileRecord.getID());
                    return Boolean.TRUE;
                } catch (InterruptedException e) {
                    LOG.debug("Worker {} has been asked to exit.", Thread.currentThread().getName());
                    throw e;
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
     * Lets all workers exit
     */
    public void stop() {
        exitLatch.countDown();
    }
}
