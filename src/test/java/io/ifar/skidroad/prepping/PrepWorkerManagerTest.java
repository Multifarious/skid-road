package io.ifar.skidroad.prepping;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracker.TransientLogFileTracker;
import io.ifar.skidroad.tracking.LogFileTracker;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class PrepWorkerManagerTest {

    LogFileTracker tracker;
    PrepWorkerManager manager;
    DummyPrepWorkerFactory factory;

    @Before
    public void setup() throws Exception {
        tracker = new TransientLogFileTracker();
        factory = new DummyPrepWorkerFactory();
        manager = new PrepWorkerManager(
                tracker,
                factory,
                10
        );
    }

    @After
    public void teardown() throws Exception {
        factory.stop();
        manager.stop();
    }

    @Test
    public void testStateChangeFiresWorker() throws Exception {
        CountDownLatch oneWorkerRan = factory.getRunCountLatch(1);
        LogFile logFile = tracker.open("foo", "/biz/baz/%s.log", DateTime.now());
        manager.start(); //start manager first so it can register to receive notifications
        tracker.written(logFile);
        awaitLatch(oneWorkerRan);
    }

    @Test
    public void testStartupProcessesStalePreparingRecords() throws Exception {
        CountDownLatch oneWorkerRan = factory.getRunCountLatch(1);
        LogFile logFile = tracker.open("foo", "/biz/baz/%s.log", DateTime.now());
        tracker.preparing(logFile); //set file first, then start manager so it can discover it during startup
        manager.start();
        awaitLatch(oneWorkerRan);
    }

    @Test
    public void testStartupProcessesStaleWrittenRecords() throws Exception {
        CountDownLatch oneWorkerRan = factory.getRunCountLatch(1);
        LogFile logFile = tracker.open("foo", "/biz/baz/%s.log", DateTime.now());
        tracker.written(logFile); //set file first, then start manager so it can discover it during startup
        manager.start();
        awaitLatch(oneWorkerRan);
    }

    private void awaitLatch(CountDownLatch latch) throws InterruptedException {
        assertTrue("timeout waiting for latch.", latch.await(1, TimeUnit.SECONDS));
    }
}
