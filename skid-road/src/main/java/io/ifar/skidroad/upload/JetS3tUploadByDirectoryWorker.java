package io.ifar.skidroad.upload;

import com.amazonaws.AmazonClientException;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.jets3t.S3Storage;
import io.ifar.skidroad.tracking.LogFileTracker;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * A variation of {@link AwsS3ClientUploadWorker} which uploads into a directory structure that reflects the local parent
 * directory of the {@link LogFile}.
 *
 * Log files are uploaded into a /<em>&lt;parent&gt;</em>/yyyy/MM/dd (implemented in {@link #determineArchiveURI(io.ifar.skidroad.LogFile)}). In
 * the local tracker database they are assigned a <em>&lt;parent&gt;</em> group (implemented in {@link #determineArchiveGroup(io.ifar.skidroad.LogFile)}).
 *
 * For use in environments where multiple {@link io.ifar.skidroad.writing.WritingWorkerManager} and multiple
 * {@link LogFileTracker}s are in use, sharing a single database and S3 configuration.
 */
public class JetS3tUploadByDirectoryWorker extends AbstractUploadWorker {
    private final static DateTimeFormatter URI_FORMATTER = new DateTimeFormatterBuilder()
            .appendYear(4,4)
            .appendLiteral('/')
            .appendMonthOfYear(2)
            .appendLiteral('/')
            .appendDayOfMonth(2)
            .toFormatter();

    private final URI uploadBasePath;
    private final S3Storage storage;

    public JetS3tUploadByDirectoryWorker(LogFile logFile, LogFileTracker tracker, URI uploadBaseURI, S3Storage storage) {
        super(logFile, tracker);
        this.uploadBasePath = uploadBaseURI;
        this.storage = storage;
    }

    /**
     * Returns name of immediate parent directory of the logFile (on the local file system)
     */
    private String getParent(LogFile logFile) {
        return logFile.getPrepPath().getParent().getFileName().toString();
    }

    @Override
    String determineArchiveGroup(LogFile logFile) {
        return getParent(logFile);
    }

    @Override
    URI determineArchiveURI(LogFile logFile) throws URISyntaxException {
        StringBuilder stringBuilder = new StringBuilder(uploadBasePath.toString());
        if (stringBuilder.charAt(stringBuilder.length()-1) != '/')
            stringBuilder.append('/');
        stringBuilder.append(getParent(logFile));
        stringBuilder.append('/');
        stringBuilder.append(URI_FORMATTER.print(logFile.getStartTime()));
        stringBuilder.append('/');
        stringBuilder.append(logFile.getPrepPath().getFileName().toString());
        return new URI(stringBuilder.toString());
    }

    @Override
    void push(LogFile logFile) throws AmazonClientException {
        storage.put(logFile.getArchiveURI().toString(), logFile.getPrepPath().toFile());
    }
}
