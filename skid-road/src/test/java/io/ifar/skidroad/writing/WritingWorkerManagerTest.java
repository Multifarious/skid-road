package io.ifar.skidroad.writing;

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
import org.skife.jdbi.v2.ResultIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class WritingWorkerManagerTest {
    private final static Logger LOG = LoggerFactory.getLogger(WritingWorkerManagerTest.class);

    private final static int PRUNE_INTERVAL_SECONDS = 1;
    private final static int LAUNCH_MORE_WORKERS_THRESHOLD = 3;
    private final static int UNHEALTHY_QUEUE_SIZE = 10;

    @Rule
    public TestName name = new TestName();

    LogFileTracker tracker;
    private ManualRollingScheme rollingScheme;
    DummyWritingWorkerFactory<String> factory;
    WritingWorkerManager<String> manager;

    @Before
    public void setup() throws Exception {
        tracker = new TransientLogFileTracker();
        rollingScheme = new ManualRollingScheme();

        factory = new DummyWritingWorkerFactory<>();
        manager = new WritingWorkerManager<>(
                rollingScheme,
                tracker,
                factory,
                PRUNE_INTERVAL_SECONDS,
                LAUNCH_MORE_WORKERS_THRESHOLD,
                UNHEALTHY_QUEUE_SIZE);
        manager.start();
    }

    @After
    public void teardown() throws Exception {
        factory.stop(); //stop factory first, that consumes all remaining work and lets manager exit.
        manager.stop();
    }

    @Test
    public void testCreateNewQueueWhenRolled() throws Exception {
        DateTime t = DateTime.now();
        rollingScheme.setNextRoll(t.plusSeconds(10));
        BlockingQueue<String> queue1 = manager.getQueueFor(t);
        assertSame(queue1, manager.getQueueFor(t.plusSeconds(8)));
        assertNotSame(queue1, manager.getQueueFor(t.plusSeconds(12)));
    }

    @Test
    public void testPruneDeadThreads() throws Exception {
        CountDownLatch creationLatch = factory.getCreationLatch(1);
        manager.record(System.currentTimeMillis(), "foo");
        awaitLatch(creationLatch);
        assertEquals(1,factory.getThreadsCreated().size());
        Thread originalWorker = factory.getThreadsCreated().get(0);
        factory.exitNextQueue();
        awaitNextPrune();
        assertFalse("Dead worker should have been pruned", manager.getWorkerThreadSnapshot().contains(originalWorker));
    }

    @Test
    public void testPruneStaleThreads() throws Exception {
        CountDownLatch creationLatch = factory.getCreationLatch(1);
        DateTime t = DateTime.now();
        manager.record(t, "foo");
        rollingScheme.setNextRoll(t.plusSeconds(1));
        awaitLatch(creationLatch);
        factory.drainNextQueue();
        awaitNextPrune(); //See worker is not needed and interrupt it.
        awaitNextPrune(); //See worker is not alive and prune it.
        assertTrue("Worker for stale & empty queue should have been pruned", manager.getWorkerThreadSnapshot().isEmpty());
    }

    @Test
    public void testPreserveNonEmptyQueues() throws Exception {
        CountDownLatch creationLatch = factory.getCreationLatch(1);
        DateTime t = DateTime.now();
        manager.record(t, "foo");
        rollingScheme.setNextRoll(t.plusSeconds(1));
        awaitLatch(creationLatch);
        awaitNextPrune();
        assertFalse("Worker for stale queue should not be pruned if queue is not empty.", manager.getWorkerThreadSnapshot().isEmpty());
    }

    @Test
    public void testPreserveCurrentQueues() throws Exception {
        CountDownLatch creationLatch = factory.getCreationLatch(1);
        manager.record(DateTime.now(), "foo");
        awaitLatch(creationLatch);
        factory.drainNextQueue();
        awaitNextPrune();
        assertFalse("Worker for current queue should not be pruned (even if queue empty).",
                manager.getWorkerThreadSnapshot().isEmpty());
    }

    @Test
    public void testLaunchExtraWorkersWhenQueueLarge() throws Exception {
        CountDownLatch creationLatch = factory.getCreationLatch(2);
        long t = System.currentTimeMillis();
        for (int i = 0; i < LAUNCH_MORE_WORKERS_THRESHOLD * 2; i++)
            manager.record(t, "foo");

        awaitNextPrune();
        awaitLatch(creationLatch);
    }

    @Test
    public void testLaunchReplacementWorkerWhenAllWorkersDead() throws Exception {
        CountDownLatch creationLatchFirst = factory.getCreationLatch(1);
        CountDownLatch creationLatchAnother = factory.getCreationLatch(2);
        manager.record(System.currentTimeMillis(), "foo");
        awaitLatch(creationLatchFirst);
        factory.exitNextQueue();
        awaitNextPrune();
        awaitLatch(creationLatchAnother);
    }

    @Test
    public void testDoNotLaunchReplacementIfQueueEmpty() throws Exception {
        CountDownLatch creationLatchFirst = factory.getCreationLatch(1);
        CountDownLatch creationLatchAnother = factory.getCreationLatch(2);
        manager.record(System.currentTimeMillis(), "foo");
        awaitLatch(creationLatchFirst);
        factory.drainNextQueue();
        factory.exitNextQueue();
        awaitNextPrune();
        assertTrue("No additional Thread should have been created.", creationLatchAnother.getCount() > 0);
    }

    @Test
    public void testResetStaleRecordsOnStartup() throws Exception {
        LogFileTracker tracker = mock(LogFileTracker.class);
        List<LogFile> dummyFiles = Arrays.asList(new LogFile(), new LogFile());
        Path path = Files.createTempFile(name.getMethodName(), ".1");
        dummyFiles.get(0).setOriginPath(path);
        dummyFiles.get(1).setOriginPath(Paths.get("bogus/bogus/bogus"));
        final Iterator<LogFile> dummyFilesIter = dummyFiles.iterator();
        when(tracker.findMine(LogFileState.WRITING)).thenReturn(new ResultIterator<LogFile>() {
            @Override
            public void close() {
                // no op
            }

            @Override
            public boolean hasNext() {
                return dummyFilesIter.hasNext();
            }

            @Override
            public LogFile next() {
                return dummyFilesIter.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        });

        WritingWorkerManager<String> manager = new WritingWorkerManager<>(
                rollingScheme,
                tracker,
                factory,
                PRUNE_INTERVAL_SECONDS,
                LAUNCH_MORE_WORKERS_THRESHOLD,
                UNHEALTHY_QUEUE_SIZE);
        try {
            manager.start();

            //LogFile records with real files on disk should get placed in WRITTEN state
            verify(tracker).written(dummyFiles.get(0));
            //LogFile records with no data on dist should get errored out
            verify(tracker).writeError(dummyFiles.get(1));
        } finally {
            manager.stop();
        }

    }

    private void awaitNextPrune() throws InterruptedException {
        LOG.debug("Awaiting prune");
        Thread.sleep(PRUNE_INTERVAL_SECONDS * 1000 + 500);
        LOG.debug("Hopefully pruned.");
    }

    private void awaitLatch(CountDownLatch latch) throws InterruptedException {
        assertTrue("timeout waiting for latch.", latch.await(10, TimeUnit.SECONDS));
    }


}
