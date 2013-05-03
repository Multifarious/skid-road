package io.ifar.skidroad.prepping;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.scheduling.SimpleQuartzScheduler;
import io.ifar.skidroad.tracker.TransientLogFileTracker;
import io.ifar.skidroad.tracking.LogFileState;
import io.ifar.skidroad.tracking.LogFileTracker;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class PrepWorkerManagerTest {

    LogFileTracker tracker;
    PrepWorkerManager manager;
    DummyPrepWorkerFactory factory;
    SimpleQuartzScheduler scheduler;

    @Rule
    public TestName name = new TestName();

    @Before
    public void setup() throws Exception {
        tracker = new TransientLogFileTracker();
        factory = new DummyPrepWorkerFactory();
        scheduler = new SimpleQuartzScheduler(getClass().getSimpleName() + "#" + name.getMethodName(), 1);
        scheduler.start();
        manager = new PrepWorkerManager(
                tracker,
                factory,
                scheduler,
                5,
                10
        );
    }

    @After
    public void teardown() throws Exception {
        factory.stop();
        manager.stop();
        scheduler.stop();
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

    @Test
    public void testRetry() throws Exception {
        CountDownLatch oneWorkerRan = factory.getRunCountLatch(1);
        CountDownLatch twoWorkersRan = factory.getRunCountLatch(2);
        LogFile logFile = tracker.open("foo", "/biz/baz/%s.log", DateTime.now());
        manager.start(); //start manager first so it can register to receive notifications
        tracker.written(logFile);
        awaitLatch(oneWorkerRan);
        logFile.setState(LogFileState.PREP_ERROR); //simulate failure of first prep attempt
        awaitLatch(twoWorkersRan); //wait for retry

    }

    private void awaitLatch(CountDownLatch latch) throws InterruptedException {
        assertTrue("timeout waiting for latch.", latch.await(10, TimeUnit.SECONDS));
    }
}
