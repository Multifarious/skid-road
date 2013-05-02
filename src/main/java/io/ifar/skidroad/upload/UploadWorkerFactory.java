package io.ifar.skidroad.upload;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileTracker;

/**
 * Constructs UploadWorkers for the UploadWorkerManager
 */
public interface UploadWorkerFactory {
    Runnable buildWorker(LogFile logFile, LogFileTracker tracker);
}
