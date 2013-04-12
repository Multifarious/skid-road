package io.ifar.skidroad.prepping;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileTracker;

/**
 * Created with IntelliJ IDEA.
 * User: lhn
 * Date: 3/21/13
 * Time: 11:52 AM
 * To change this template use File | Settings | File Templates.
 */
public class EncryptAndCompressPrepWorkerFactory implements PrepWorkerFactory {
    private final String masterKeyBase64;
    private final String masterIVBase64;

    public EncryptAndCompressPrepWorkerFactory(String masterKeyBase64, String masterIVBase64) {
        this.masterKeyBase64 = masterKeyBase64;
        this.masterIVBase64 = masterIVBase64;
    }

    @Override
    public Runnable buildWorker(LogFile logFile, LogFileTracker tracker) {
        return new EncryptAndCompressPrepper(logFile, tracker, masterKeyBase64, masterIVBase64);
    }
}
