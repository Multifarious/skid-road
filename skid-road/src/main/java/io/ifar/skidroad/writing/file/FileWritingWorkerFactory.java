package io.ifar.skidroad.writing.file;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileTracker;
import io.ifar.skidroad.writing.WritingWorkerFactory;

import java.util.concurrent.BlockingQueue;


public class FileWritingWorkerFactory<T> implements WritingWorkerFactory<T> {
    private final Serializer<T> serializer;
    private final int flushIntervalSeconds;

    public FileWritingWorkerFactory(Serializer<T> serializer, int flushIntervalSeconds) {
        this.serializer = serializer;
        this.flushIntervalSeconds = flushIntervalSeconds;
    }

    @Override
    public Thread buildWorker(BlockingQueue<T> queue, LogFile logFileRecord, LogFileTracker tracker) {
        FileWritingWorker<T> runnable = new FileWritingWorker<T>(queue, serializer, logFileRecord, flushIntervalSeconds, tracker);
        String threadName = FileWritingWorker.class.getSimpleName() + "__" + logFileRecord.getOriginPath().getFileName();
        return new Thread(runnable, threadName);
    }
}
