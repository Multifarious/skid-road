package io.ifar.skidroad.recorder;

import com.google.common.base.Function;
import com.sun.istack.internal.Nullable;
import io.ifar.skidroad.writing.WritingWorkerManager;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standard {@link Recorder} implementation.
 *
 * The logical pieces involved are:
 * 1) a predicate to determine whether the filter should run
 * 2) an "extractor" function to obtain the data to be recorded from the provided input. E.g. to pull a piece of data
 * from a request POJO.
 * 3) a "transformer" function to transform the data into the output format. E.g. to wrap the data into a tuple with
 * request id and data type.
 * 4) a mechanism to determine the timestamp for the log entry. E.g. request arrival time as determined by
 * {@code RequestTimestampFilter} (from skid-road-jersey)
 * 5) a Skid Road writer manager to consume the output
 *
 */
public class BasicRecorder<S, T, U> implements Recorder<S> {
    private final static Logger LOG = LoggerFactory.getLogger(BasicRecorder.class);

    private final Function<? super S, Boolean> predicate;
    private final Function<? super S, ? extends T> extractor;
    private final Function<? super T, ? extends U> transformer;
    private final Function<? super S, DateTime> timestamp;
    private final WritingWorkerManager<? super U> writer;
    private final Boolean skipNulls;

    public static Function<Object,DateTime> constantTimestamp(final DateTime timestamp) {
        return new Function<Object, DateTime>() {
            @Override
            public DateTime apply(@Nullable Object input) {
                return timestamp;
            }
        };
    }

    public BasicRecorder(Function<? super S, Boolean> predicate, Function<? super S, ? extends T> extractor, Function<? super T, ? extends U> transformer, Function<? super S, DateTime> timestamp, WritingWorkerManager<? super U> writer, Boolean skipNulls) {
        this.predicate = predicate;
        this.extractor = extractor;
        this.transformer = transformer;
        this.timestamp = timestamp;
        this.writer = writer;
        this.skipNulls = skipNulls;
    }

    @Override
    public void record(S item) {
        try {
            if (predicate == null || predicate.apply(item)) {
                T extracted = extractor.apply(item);
                if ( !(skipNulls && extracted == null)) {
                    U transformed = transformer.apply(extracted);

                    if ( !(skipNulls && transformed == null)) {
                        writer.record(
                                timestamp.apply(item),
                                transformed
                        );
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn(String.format("Unexpected Exception recording '%s'", item), e);
        }
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
