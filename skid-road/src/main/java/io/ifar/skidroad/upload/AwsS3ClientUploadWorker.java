package io.ifar.skidroad.upload;

import com.amazonaws.AmazonClientException;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.jets3t.S3Storage;
import io.ifar.skidroad.tracking.LogFileTracker;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.ISODateTimeFormat;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Constructs UploadWorkers that uploads LogFiles via JetS3t (e.g. to Amazon S3).
 *
 * Log files are uploaded into a yyyy/MM/dd (implemented in {@link #determineArchiveURI(io.ifar.skidroad.LogFile)}). In
 * the local tracker database they are assigned a yyyyMMdd group (implemented in {@link #determineArchiveGroup(io.ifar.skidroad.LogFile)}).
 */
public class AwsS3ClientUploadWorker extends AbstractUploadWorker {
    private final static DateTimeFormatter GROUP_FORMATTER = ISODateTimeFormat.basicDate();
    private final static DateTimeFormatter URI_FORMATTER = new DateTimeFormatterBuilder()
            .appendYear(4,4)
            .appendLiteral('/')
            .appendMonthOfYear(2)
            .appendLiteral('/')
            .appendDayOfMonth(2)
            .toFormatter();

    private final URI uploadBasePath;
    private final S3Storage storage;

    public AwsS3ClientUploadWorker(LogFile logFile, LogFileTracker tracker, URI uploadBaseURI, S3Storage storage) {
        super(logFile, tracker);
        this.uploadBasePath = uploadBaseURI;
        this.storage = storage;
    }

    @Override
    String determineArchiveGroup(LogFile logFile) {
        //TODO (future): pluggable mechanism like FileRollingScheme for archive group. For now always yyyymmdd. Or maybe do not need archive group at all?
        return GROUP_FORMATTER.print(logFile.getStartTime());
    }

    @Override
    URI determineArchiveURI(LogFile logFile) throws URISyntaxException {
        StringBuilder stringBuilder = new StringBuilder(uploadBasePath.toString());
        if (stringBuilder.charAt(stringBuilder.length()-1) != '/')
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
