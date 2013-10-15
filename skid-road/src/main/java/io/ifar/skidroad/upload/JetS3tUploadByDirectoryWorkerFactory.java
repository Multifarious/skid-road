package io.ifar.skidroad.upload;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.jets3t.JetS3tStorage;
import io.ifar.skidroad.tracking.LogFileTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.Callable;

/**
 * Constructs {@link JetS3tUploadByDirectoryWorker}s that upload LogFiles via JetS3t (e.g. to Amazon S3)
 * @see JetS3tUploadByDirectoryWorker
 */
public class JetS3tUploadByDirectoryWorkerFactory implements UploadWorkerFactory {
    private final static Logger LOG = LoggerFactory.getLogger(JetS3tUploadByDirectoryWorkerFactory.class);

    private final URI uploadBaseURI;
    private final JetS3tStorage jetS3tStorage;

    public JetS3tUploadByDirectoryWorkerFactory(JetS3tStorage storage, URI uploadBaseURI) {
        this.jetS3tStorage = storage;
        this.uploadBaseURI = uploadBaseURI;

    }

    @Override
    public Callable<Boolean> buildWorker(final LogFile logFile, final LogFileTracker tracker) {
        return new JetS3tUploadByDirectoryWorker(logFile, tracker, uploadBaseURI, jetS3tStorage);
    }
}
