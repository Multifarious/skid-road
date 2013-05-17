package io.ifar.skidroad.writing;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileTracker;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

public class FileWritingWorkerTest {


    BlockingQueue<String> queue;
    FileWritingWorker<String> worker;
    UpcaseErroringSerializer serializer;
    LogFile record;
    LogFileTracker tracker;
    Thread thread;

    @Rule
    public TestName name = new TestName();

    @Before
    public void setup() {
        queue = new LinkedBlockingQueue<>();
        serializer = new UpcaseErroringSerializer();
        record = new LogFile();
        tracker = mock(LogFileTracker.class);
        worker = new FileWritingWorker<>(
                queue,
                serializer,
                record,
                1,
                tracker
        );
        thread = new Thread(worker,name.getMethodName());
    }

    @Test
    public void testCannotOpenFile() throws IOException, InterruptedException {
        Path path = Files.createTempFile(name.getMethodName(),".1", PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("---------")));
        try {
            queue.addAll(Arrays.asList("foo", "bar", "baz"));
            record.setOriginPath(path);
            thread.start();
            assertExit(thread, "detected detected that it cannot write to output file");
            assertEquals("Worker should have detected that it cannot write to output file and not consumed any items.", 3, queue.size());
            verify(tracker,times(1)).writeError((LogFile) anyObject());
            verify(tracker,never()).written((LogFile) anyObject());
        } finally {
            Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-rw-rw-"));
            Files.delete(path);
        }
    }


    @Test
    public void testGracefulShutdown() throws IOException, InterruptedException {
        Path path = Files.createTempFile(name.getMethodName(),".1");
        try {
            record.setOriginPath(path);
            thread.start();
            thread.interrupt();
            assertExit(thread, "detected interrupt");
        } finally {
            Files.delete(path);
        }
    }

    @Test
    public void testWriteOutput() throws IOException, InterruptedException {
        Path path = Files.createTempFile(name.getMethodName(),".1");
        try {
            queue.addAll(Arrays.asList("foo", "bar", "baz"));
            record.setOriginPath(path);
            thread.start();
            assertDrain(queue);
            thread.interrupt();
            thread.join(1000);
            List<String> actual = Files.readAllLines(path, FileWritingWorker.UTF8);
            assertEquals(Arrays.asList("FOO","BAR","BAZ"), actual);
            verify(tracker,never()).writeError((LogFile) anyObject());
            verify(tracker,times(1)).written((LogFile) anyObject());
        } finally {
            Files.delete(path);
        }
    }

    @Test
    public void testFlush() throws IOException, InterruptedException {
        Path path = Files.createTempFile(name.getMethodName(),".1");
        try {
            queue.addAll(Arrays.asList("foo", "bar", "baz"));
            record.setOriginPath(path);
            thread.start();
            assertDrain(queue);
            //no flush yet, expect empty file
            assertEquals(Collections.<String>emptyList(), Files.readAllLines(path, FileWritingWorker.UTF8));

            thread.sleep(1500); //Wait for auto-flush interval to pass.
            assertEquals(Arrays.asList("FOO", "BAR", "BAZ"), Files.readAllLines(path, FileWritingWorker.UTF8));
            thread.interrupt();
        } finally {
            Files.delete(path);
        }
    }

    @Test
    public void testPutBackOnQueue() throws IOException, InterruptedException {
        Path path = Files.createTempFile(name.getMethodName(),".1");
        try {
            queue.add("foo");
            record.setOriginPath(path);
            thread.start();
            assertDrain(queue);
            //Hard to simulate an IOError from writing to the file, but can exercise the same error handling by making the serializer throw an error.
            serializer.throwErrors = true;
            queue.add("bar");
            assertExit(thread, "detected serialization error");
            assertArrayEquals("Worker should have put failed item back on the queue.", new String[]{"bar"}, queue.toArray());
            assertEquals(Arrays.asList("FOO"), Files.readAllLines(path, FileWritingWorker.UTF8));
            verify(tracker,never()).writeError((LogFile) anyObject());
            verify(tracker,times(1)).written((LogFile) anyObject());
        } finally {
            Files.delete(path);
        }
    }

    private <T> void assertDrain(Queue<T> queue) throws InterruptedException {
        long timeoutAt = System.currentTimeMillis() + 1000;
        while (System.currentTimeMillis() < timeoutAt) {
            if (queue.isEmpty())
                break;
            Thread.sleep(5);
        }
        assertTrue("Worker should have drained the queue.", queue.isEmpty());
    }

    private void assertExit(Thread thread, String reason) throws InterruptedException {
        thread.join(1000);
        assertFalse("Worker should have " + reason + " and exited.", thread.isAlive());
    }
}
