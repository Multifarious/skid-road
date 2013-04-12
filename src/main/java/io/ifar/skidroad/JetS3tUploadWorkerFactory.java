package io.ifar.skidroad;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.jets3t.JetS3tStorage;
import io.ifar.skidroad.tracking.LogFileTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * Constructs JetS3tUploadWorkers that upload LogFiles via JetS3t (e.g. to Amazon S3)
 * @see JetS3tUploadWorker
 */
public class JetS3tUploadWorkerFactory implements UploadWorkerFactory {
    private final static Logger LOG = LoggerFactory.getLogger(JetS3tUploadWorkerFactory.class);

    private final URI uploadBaseURI;
    private final JetS3tStorage jetS3tStorage;

    public JetS3tUploadWorkerFactory(JetS3tStorage storage, URI uploadBaseURI) {
        this.jetS3tStorage = storage;
        this.uploadBaseURI = uploadBaseURI;

    }

    @Override
    public Runnable buildWorker(final LogFile logFile, final LogFileTracker tracker) {
        return new JetS3tUploadWorker(logFile, tracker, uploadBaseURI, jetS3tStorage);
    }
}
