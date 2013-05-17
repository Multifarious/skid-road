package io.ifar.skidroad.upload;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileTracker;

import java.util.concurrent.Callable;

/**
 * Constructs UploadWorkers for the UploadWorkerManager
 */
public interface UploadWorkerFactory {
    Callable<Boolean> buildWorker(LogFile logFile, LogFileTracker tracker);
}
