package io.ifar.skidroad.prepping;

import io.ifar.skidroad.LogFile;
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
    private static final int RETRY_INTERVAL_SECONDS = 3;
    public static final int MAX_TEST_DURATION = 12; //must be > 2 * RETRY_INTERVAL

    @Rule
    public TestName name = new TestName();

    @Before
    public void setup() throws Exception {
        tracker = new TransientLogFileTracker();
        factory = new DummyPrepWorkerFactory();
        manager = new PrepWorkerManager(
                tracker,
                factory,
                RETRY_INTERVAL_SECONDS,
                5,
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

    @Test
    public void testRetry() throws Exception {
        // Wait a little less this time out.
        factory.setSecondsToWait(5L);
        CountDownLatch oneWorkerCreated = factory.getRunCountLatch(1);
        CountDownLatch secondWorkerCreated = factory.getRunCountLatch(2);
        LogFile logFile = tracker.open("foo", "/biz/baz/%s.log", DateTime.now());
        manager.start(); //start manager first so it can register to receive notifications
        tracker.written(logFile);
        awaitLatch(oneWorkerCreated);
        logFile.setState(LogFileState.PREP_ERROR); //simulate failure of first prep attempt
        awaitLatch(secondWorkerCreated); //wait for retry
    }

    @Test
    public void testDoNotRetryInFlightWork() throws Exception {
        //The retryOneThenRetryAll scans the database for rows that are ready for prep but haven't completed prep and
        //are not currently being prepared. Async processing is then scheduled.
        //But the retry should not be scheduled if work is already in-flight (either actively or pending)
        //See https://github.com/Multifarious/skid-road/issues/20
        CountDownLatch oneWorkerCreated = factory.getRunCountLatch(1);
        CountDownLatch twoWorkersCreated = factory.getRunCountLatch(2);
        LogFile logFile = tracker.open("foo", "/biz/baz/%s.log", DateTime.now());
        manager.start(); //start manager first so it can register to receive notifications
        tracker.written(logFile);
        awaitLatch(oneWorkerCreated); //wait initial worker
        //sigh. This is a slow test because we have to wait for the retry job to get not get scheduled.
        if (twoWorkersCreated.await(2 * RETRY_INTERVAL_SECONDS, TimeUnit.SECONDS)) {
            fail("Retry was scheduled for in-flight task!");
        }
    }

    private void awaitLatch(CountDownLatch latch) throws InterruptedException {
        assertTrue("timeout waiting for latch.", latch.await(MAX_TEST_DURATION, TimeUnit.SECONDS));
    }
}
