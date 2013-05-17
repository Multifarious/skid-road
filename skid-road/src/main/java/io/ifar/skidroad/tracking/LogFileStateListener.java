package io.ifar.skidroad.tracking;

import io.ifar.skidroad.LogFile;

/**
 * Receives notification when a LogFile's state has changed.
 */
public interface LogFileStateListener {
    public void stateChanged(LogFile logFile);
}
