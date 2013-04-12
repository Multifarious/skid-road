package io.ifar.skidroad;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.jets3t.JetS3tStorage;
import io.ifar.skidroad.tracking.LogFileTracker;
import org.jets3t.service.ServiceException;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.ISODateTimeFormat;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Constructs UploadWorkers that uploads LogFiles via JetS3t (e.g. to Amazon S3)
 */
public class JetS3tUploadWorker extends AbstractUploadWorker {
    private final static DateTimeFormatter GROUP_FORMATTER = ISODateTimeFormat.basicDate();
    private final static DateTimeFormatter URI_FORMATTER = new DateTimeFormatterBuilder()
            .appendYear(4,4)
            .appendLiteral('/')
            .appendMonthOfYear(2)
            .appendLiteral('/')
            .appendDayOfMonth(2)
            .toFormatter();

    private final URI uploadBasePath;
    private final JetS3tStorage storage;

    public JetS3tUploadWorker(LogFile logFile, LogFileTracker tracker, URI uploadBaseURI, JetS3tStorage storage) {
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
    void push(LogFile logFile) throws ServiceException {
        storage.put(logFile.getArchiveURI().toString(), logFile.getPrepPath().toFile());
    }
}
