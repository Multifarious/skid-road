package io.ifar.skidroad.writing;

import io.ifar.skidroad.LogFile;
import io.ifar.skidroad.tracking.LogFileTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardOpenOption.*;

/**
 * A Thread which owns a file and pulls RequestBeans off of a shared queue.
 *
 * Opt for our own Thread management rather than e.g. ForkJoin pool because we
 * ownership of a file across the handling of multiple requests. Could use a
 * model of single-request-Runnables checking out Writers from a common pool,
 * but that would make Writer lifecycle management (e.g. regular flushing) fiddly.
 * Also, since each item processes so quickly, we gain little from work-stealing.
 *
 * Future: Support max-file-size (or entry count) forced exit (thus rotating to new worker/file).
 */
public class FileWritingWorker<T> implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(FileWritingWorker.class);
    public static final Charset UTF8 = Charset.forName("UTF-8");

    private final BlockingQueue<T> queue;
    private final Serializer<T> serializer;
    private boolean shuttingDown;
    private final LogFile logFileRecord;
    private final int maxFlushIntervalSeconds;
    private String name;
    private final LogFileTracker tracker;

    public FileWritingWorker(final BlockingQueue<T> queue, final Serializer<T> serializer, final LogFile logFileRecord, final int maxFlushIntervalSeconds, final LogFileTracker tracker) {
        this.queue = queue;
        this.serializer = serializer;
        this.shuttingDown = false;
        this.logFileRecord = logFileRecord;
        this.maxFlushIntervalSeconds = maxFlushIntervalSeconds;
        this.tracker = tracker;
    }

    @Override
    public void run() {
        this.name = Thread.currentThread().getName(); //don't do this in the constructor, that runs before Runnable handed off to worker thread.
        try {
            LOG.info("Starting worker thread {} for {}", this.name, this.logFileRecord.getOriginPath());
            T item = null;
            boolean dirty = false;
            try (BufferedWriter writer = Files.newBufferedWriter(logFileRecord.getOriginPath(), UTF8, CREATE, WRITE, APPEND)) {
                try {
                    long nextFlush = 0L;

                    //wait for data, breaking periodically to flush to disk
                    while(!shuttingDown) {
                        try {
                            if (dirty) {
                                long interval = nextFlush - System.currentTimeMillis();
                                if (interval <= 0) {
                                    LOG.debug("Flushing {} because {} seconds have elapsed since last flush", this.name, maxFlushIntervalSeconds);
                                    flush(writer);
                                    dirty = false;
                                    item = queue.take();
                                } else {
                                    //if nothing arrives within interval, we'll loop around and do a flush, then a take()
                                    item = queue.poll(interval, TimeUnit.MILLISECONDS);
                                }
                            } else {
                                item = queue.take();
                            }

                            if (item != null) {
                                write(writer, item);
                                item = null;
                                if (!dirty) {
                                    dirty = true;
                                    nextFlush = System.currentTimeMillis() + maxFlushIntervalSeconds * 1000;
                                }
                            }
                        } catch(InterruptedException e) {
                            LOG.debug("{} caught InterruptedException. Stopping...", this.name);
                            shuttingDown = true;
                        }
                    }
                    flush(writer);
                    dirty = false;
                } catch (IOException e) {
                    LOG.warn("Abnormal worker termination ({})", this.name, e);
                    //Cleanup happens in finally block
                }
                finally {
                    if (item != null) {
                        LOG.info("{} putting unwritten item '{}' back onto queue because of abnormal worker termination.", this.name, item);
                        queue.add(item);
                    }
                    if (dirty) {
                        LOG.info("{} attempting to flush {} to disk because of abnormal worker termination.", this.name, logFileRecord.getOriginPath());
                        try {
                            flush(writer);
                        } catch (IOException e) {
                            //ignore. Probably was failed and logged before.
                        }
                    }
                    //Mark as written; we got the file open and there might be data in it.
                    logFileRecord.setByteSize(Files.size(logFileRecord.getOriginPath()));
                    tracker.written(logFileRecord); //ignore update failures; worker exiting anyway
                }
            } catch (IOException e) {
                LOG.error("{} failed to open output file {}. This instance will not consume any items.", this.name, logFileRecord.getOriginPath(), e);
                //Mark as write error; we never got the file open and there's no point in using it.
                tracker.writeError(logFileRecord); //ignore update failures; worker exiting anyway
            }
        } finally {
            LOG.info("Stopped worker {} for {}", this.name, logFileRecord.getOriginPath());
        }
    }

    private void flush(Writer writer) throws IOException {
        try {
            writer.flush();
        } catch (IOException e) {
            //Not much we can do at this point. Don't have the items anymore.
            LOG.error("{} cannot flush writer. Possible data loss.", this.name, e);
            throw e;
        }

    }

    private void write(Writer writer, T item) throws IOException {
        String s = serializer.serialize(item);
        try {
            writer.write(s);
            writer.write('\n');
        } catch (IOException e) {
            LOG.error("{} error writing request to disk.", this.name, s, e);
            throw e;
        }
    }

}
