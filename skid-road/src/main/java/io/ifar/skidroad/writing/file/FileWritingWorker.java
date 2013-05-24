package io.ifar.skidroad.writing.file;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileTracker;
import io.ifar.skidroad.writing.AbstractWritingWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;

import static java.nio.file.StandardOpenOption.*;

/**
 * File-based {@link io.ifar.skidroad.writing.AbstractWritingWorker} implementation. A {@link Serializer} is required to
 * write each item to the file.
 */
public class FileWritingWorker<T> extends AbstractWritingWorker<Writer, T> {
    private static final Logger LOG = LoggerFactory.getLogger(FileWritingWorker.class);
    public static final Charset UTF8 = Charset.forName("UTF-8");

    private final Serializer<T> serializer;

    public FileWritingWorker(final BlockingQueue<T> queue, final Serializer<T> serializer, final LogFile logFileRecord, final int maxFlushIntervalSeconds, final LogFileTracker tracker) {
        super(queue, logFileRecord, maxFlushIntervalSeconds, tracker);
        this.serializer = serializer;
    }

    @Override
    protected Writer openForWriting(Path path) throws IOException {
        return Files.newBufferedWriter(path, UTF8, CREATE, WRITE, APPEND);
    }

    @Override
    protected void writeItem(Writer writer, T item) throws IOException {
        writer.write(serializer.serialize(item));
        writer.write("\n");
    }
}
