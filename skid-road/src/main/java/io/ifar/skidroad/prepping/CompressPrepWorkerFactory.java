package io.ifar.skidroad.prepping;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileTracker;

import java.util.concurrent.Callable;

/**
 * Constructs {@link io.ifar.skidroad.prepping.CompressPrepper} objects.
 */
public class CompressPrepWorkerFactory implements PrepWorkerFactory {
    @Override
    public Callable<Boolean> buildWorker(LogFile logFile, LogFileTracker tracker) {
        return new CompressPrepper(logFile, tracker);
    }
}
