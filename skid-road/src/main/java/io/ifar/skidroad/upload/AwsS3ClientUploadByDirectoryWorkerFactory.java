package io.ifar.skidroad.upload;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.awssdk.S3Storage;
import io.ifar.skidroad.tracking.LogFileTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.Callable;

/**
 * Constructs {@link AwsS3ClientUploadByDirectoryWorker}s that upload LogFiles via JetS3t (e.g. to Amazon S3)
 * @see AwsS3ClientUploadByDirectoryWorker
 */
public class AwsS3ClientUploadByDirectoryWorkerFactory implements UploadWorkerFactory {
    private final static Logger LOG = LoggerFactory.getLogger(AwsS3ClientUploadByDirectoryWorkerFactory.class);

    private final URI uploadBaseURI;
    private final S3Storage s3Storage;

    public AwsS3ClientUploadByDirectoryWorkerFactory(S3Storage storage, URI uploadBaseURI) {
        this.s3Storage = storage;
        this.uploadBaseURI = uploadBaseURI;

    }

    @Override
    public Callable<Boolean> buildWorker(final LogFile logFile, final LogFileTracker tracker) {
        return new AwsS3ClientUploadByDirectoryWorker(logFile, tracker, uploadBaseURI, s3Storage);
    }
}
