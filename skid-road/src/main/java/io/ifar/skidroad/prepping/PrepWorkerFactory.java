package io.ifar.skidroad.prepping;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileTracker;

import java.util.concurrent.Callable;

/**
 * Constructs PrepWorkers for the PrepWorkManager
 */
public interface PrepWorkerFactory {
    Callable<Boolean> buildWorker(LogFile logFile, LogFileTracker tracker);
}
