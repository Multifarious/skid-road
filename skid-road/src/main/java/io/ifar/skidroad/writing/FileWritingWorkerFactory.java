package io.ifar.skidroad.writing;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileTracker;

import java.util.concurrent.BlockingQueue;


public class FileWritingWorkerFactory<T> implements WritingWorkerFactory<T> {
    private final int flushIntervalSeconds;

    public FileWritingWorkerFactory(int flushIntervalSeconds) {
        this.flushIntervalSeconds = flushIntervalSeconds;
    }

    @Override
    public Thread buildWorker(BlockingQueue<T> queue, Serializer<T> serializer, LogFile logFileRecord, LogFileTracker tracker) {
        FileWritingWorker<T> runnable = new FileWritingWorker<T>(queue, serializer, logFileRecord, flushIntervalSeconds, tracker);
        String threadName = FileWritingWorker.class.getSimpleName() + "__" + logFileRecord.getOriginPath().getFileName();
        return new Thread(runnable, threadName);
    }
}
