package io.ifar.skidroad.prepping;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

/**
 * Provides standard LogFileTracker interaction.
 */
public abstract class AbstractPrepWorker implements Callable<Boolean> {

    private final static Logger LOG = LoggerFactory.getLogger(AbstractPrepWorker.class);
    protected final LogFile logFile;
    protected final LogFileTracker tracker;

    protected AbstractPrepWorker(LogFile logFile, LogFileTracker tracker) {
        this.logFile = logFile;
        this.tracker = tracker;
    }

    @Override
    public Boolean call() throws Exception {
        try {
            if (tracker.preparing(logFile) != 1)
                throw new IllegalStateException("Cannot place " + logFile + " into into PREPARING state.");

            final Path inputPath = logFile.getPrepPath() == null ? logFile.getOriginPath() : logFile.getPrepPath();
            logFile.setPrepPath(prepare(inputPath));
            if (tracker.updatePrepPath(logFile) != 1)
                throw new IllegalStateException("Cannot set prep path for " + logFile);


            LOG.debug("Prepared {} to {}", logFile, logFile.getPrepPath());
            tracker.prepared(logFile); //ignore update failures; worker exiting anyway
            return Boolean.TRUE;
        } catch (Exception e) {
            LOG.warn("Preparation for {} failed.", logFile, e);
            tracker.prepError(logFile); //ignore update failures; worker exiting anyway
            throw e;
        }
    }

    /**
     * Runs preparation step on provided LogFile. logFile's inputPath will be set to the return value of this function.
     *
     * @param inputPath Where data is located. Sourced from LogFile's originPath for first PrepWorker and LogFile's inputPath for subsequent PrepWorkers, if any.
     * @return Output location
     * @throws PreparationException if preparation failed
     */
    abstract protected Path prepare(Path inputPath) throws PreparationException;

    /**
     * Removes the last .extension of provided path, if any, and appends the provided new extension.
     * @param p
     * @param newExtension
     * @return
     */
    public Path withNewExtension(Path p, String newExtension) {
        String dottedExtension = newExtension.charAt(0) == '.' ? newExtension : '.' + newExtension;
        String asString = p.toString();
        int lastDot = asString.indexOf('.');
        String withoutExtension = lastDot < 0 ? asString : asString.substring(0, lastDot);
        return Paths.get(withoutExtension + dottedExtension);
    }
}
