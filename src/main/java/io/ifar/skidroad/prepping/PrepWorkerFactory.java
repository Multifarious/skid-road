package io.ifar.skidroad.prepping;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileTracker;

/**
 * Constructs PrepWorkers for the PrepWorkManager
 */
public interface PrepWorkerFactory {
    Runnable buildWorker(LogFile logFile, LogFileTracker tracker);
}
