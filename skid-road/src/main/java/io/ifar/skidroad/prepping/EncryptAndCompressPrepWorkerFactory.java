package io.ifar.skidroad.prepping;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileTracker;

import java.util.concurrent.Callable;

/**
 * Constructs {@link EncryptAndCompressPrepper} objects.
 */
public class EncryptAndCompressPrepWorkerFactory implements PrepWorkerFactory {
    private final String masterKeyBase64;

    public EncryptAndCompressPrepWorkerFactory(String masterKeyBase64) {
        this.masterKeyBase64 = masterKeyBase64;
    }

    @Override
    public Callable<Boolean> buildWorker(LogFile logFile, LogFileTracker tracker) {
        return new EncryptAndCompressPrepper(logFile, tracker, masterKeyBase64);
    }
}
