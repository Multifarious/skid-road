package io.ifar.skidroad.writing;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileTracker;

import java.util.concurrent.BlockingQueue;

/**
 * Constructs WritingWorkers for the WritingWorkerManager.
 */
public interface WritingWorkerFactory<T> {
    Thread buildWorker(BlockingQueue<T> queue, Serializer<T> serializer, LogFile logFileRecord, LogFileTracker tracker);
}
