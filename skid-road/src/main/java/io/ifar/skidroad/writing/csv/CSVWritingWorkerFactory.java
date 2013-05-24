package io.ifar.skidroad.writing.csv;

import io.ifar.goodies.Tuple;
import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileTracker;
import io.ifar.skidroad.writing.WritingWorkerFactory;

import java.util.concurrent.BlockingQueue;


public class CSVWritingWorkerFactory<T extends Tuple> implements WritingWorkerFactory<T> {
    private final int flushIntervalSeconds;
    private final String nullRepresentation;

    public CSVWritingWorkerFactory(int flushIntervalSeconds) {
        this("",flushIntervalSeconds);
    }

    /**
     * @param flushIntervalSeconds
     * @param nullRepresentation How null values should be represented in the output
     */
    public CSVWritingWorkerFactory(String nullRepresentation, int flushIntervalSeconds) {
        this.flushIntervalSeconds = flushIntervalSeconds;
        this.nullRepresentation = nullRepresentation;
    }

    @Override
    public Thread buildWorker(BlockingQueue<T> queue, LogFile logFileRecord, LogFileTracker tracker) {
        CSVWritingWorker runnable = new CSVWritingWorker(queue, logFileRecord, flushIntervalSeconds, nullRepresentation, tracker);
        String threadName = CSVWritingWorker.class.getSimpleName() + "__" + logFileRecord.getOriginPath().getFileName();
        return new Thread(runnable, threadName);
    }
}
