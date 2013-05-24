package io.ifar.skidroad.writing.csv;

import com.google.common.collect.Lists;
import io.ifar.goodies.Tuple;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileTracker;
import io.ifar.skidroad.writing.AbstractWritingWorker;
import org.supercsv.cellprocessor.ConvertNullTo;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.file.StandardOpenOption.*;

/**
 * CSV-based {@link io.ifar.skidroad.writing.AbstractWritingWorker} implementation that consumes Tuples.
 */
public class CSVWritingWorker<T extends Tuple> extends AbstractWritingWorker<CsvListWriter, T> {
    public static final Charset UTF8 = Charset.forName("UTF-8");
    private final CellProcessor cellProcessor;
    private final ConcurrentHashMap<Integer,CellProcessor[]> cellProcessorCache = new ConcurrentHashMap<>();

    public CSVWritingWorker(final BlockingQueue<T> queue, final LogFile logFileRecord, final int maxFlushIntervalSeconds, final String nullRepresentation, final LogFileTracker tracker) {
        super(queue, logFileRecord, maxFlushIntervalSeconds, tracker);
        if (nullRepresentation == null || "".equals(nullRepresentation)) {
            cellProcessor = null;
        } else {
            cellProcessor = new ConvertNullTo(nullRepresentation);
        }
    }

    @Override
    protected CsvListWriter openForWriting(Path path) throws IOException {
        return new CsvListWriter(Files.newBufferedWriter(path, UTF8, CREATE, WRITE, APPEND), CsvPreference.STANDARD_PREFERENCE);
    }

    @Override
    protected void writeItem(CsvListWriter writer, Tuple item) throws IOException {
        if (cellProcessor == null) {
            writer.write(item.getValues());
        } else {
            CellProcessor[] cellProcessors = cellProcessorCache.get(item.getArity());
            if (cellProcessors == null) {
                cellProcessors = new CellProcessor[item.getArity()];
                Arrays.fill(cellProcessors, cellProcessor);
                cellProcessorCache.put(item.getArity(),cellProcessors); //every thread will do the same thing, so ignore race conditions
            }
            //Don't use ImmutableList.copyOf because ImmutableList does not allow null elements !
            writer.write(Lists.newArrayList(item.getValues()),cellProcessors);
        }
    }
}
