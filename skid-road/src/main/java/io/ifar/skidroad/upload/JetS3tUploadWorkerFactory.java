package io.ifar.skidroad.upload;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.jets3t.S3Storage;
import io.ifar.skidroad.tracking.LogFileTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.Callable;

/**
 * Constructs {@link AwsS3ClientUploadWorker}s that upload LogFiles via JetS3t (e.g. to Amazon S3)
 * @see AwsS3ClientUploadWorker
 */
public class JetS3tUploadWorkerFactory implements UploadWorkerFactory {
    private final static Logger LOG = LoggerFactory.getLogger(JetS3tUploadWorkerFactory.class);

    private final URI uploadBaseURI;
    private final S3Storage s3Storage;

    public JetS3tUploadWorkerFactory(S3Storage storage, URI uploadBaseURI) {
        this.s3Storage = storage;
        this.uploadBaseURI = uploadBaseURI;

    }

    @Override
    public Callable<Boolean> buildWorker(final LogFile logFile, final LogFileTracker tracker) {
        return new AwsS3ClientUploadWorker(logFile, tracker, uploadBaseURI, s3Storage);
    }
}
