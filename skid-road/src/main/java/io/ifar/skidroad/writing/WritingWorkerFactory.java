package io.ifar.skidroad.writing;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileTracker;
import io.ifar.skidroad.writing.file.Serializer;

import java.util.concurrent.BlockingQueue;

/**
 * Constructs WritingWorkers for the WritingWorkerManager.
 */
public interface WritingWorkerFactory<T> {
    Thread buildWorker(BlockingQueue<T> queue, LogFile logFileRecord, LogFileTracker tracker);
}
