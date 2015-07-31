package io.ifar.skidroad.jersey.single;

import com.google.common.base.Functions;
import io.ifar.goodies.Triple;
import io.ifar.skidroad.recorder.BasicRecorder;
import io.ifar.skidroad.recorder.Recorder;
import io.ifar.skidroad.writing.WritingWorkerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Constructs Recorder objects which write data to Skid Road.
 *

 *
 * Helper functions are provided to assemble default constellations of these pieces.
 *
 */
public class RecorderFactory {
    private static final Logger LOG = LoggerFactory.getLogger(RecorderFactory.class);

    /**
     * Constructs a Recorder that accepts any input and writes it out as an (id, tag, data) tuple.
     *
     * id is determined at recorder-creation-time from {@link UUIDGeneratorFilter}.
     * timestamp is determined at recorder-creation-time from {@link RequestTimestampFilter}
     * tag is specified at recorder-creation-time.
     *
     * data is supplied at run-time and toString'd.
     */
    public static <T> Recorder<T> build(
            final String typeTag,
            final WritingWorkerManager<Triple<String,String,String>> writingWorkerManager
    ) {
        return new BasicRecorder<>(
                null,
                Functions.<T>identity(),
                IDTagTripleTransformFactory.stringWithFixedID(typeTag),
                BasicRecorder.constantTimestamp(RequestTimestampFilter.getRequestDateTime()),
                writingWorkerManager,
                false
        );
    }
    /**
     * Construct a Recorder that uses the request time of the current request (as
     * obtained by {@link RequestTimestampFilter} as the timestamp for all entries.
     * @param predicate
     * @param extractor
     * @param transformer
     * @param writingWorkerManager
     * @param skipNulls
     * @param <S>
     * @param <T>
     * @param <U>
     * @return
     */
    /*
    public static <S,T,U> Recorder<S> buildForRequest(
            Function<? super S, Boolean> predicate,
            Function<? super S, ? extends T> extractor,
            Function<? super T, ? extends U> transformer,
            WritingWorkerManager<U> writingWorkerManager,
            boolean skipNulls
    ) {
        return new BasicRecorder<S,T,U>(
                predicate,
                extractor,
                transformer,
                constantTimestamp(
                        RequestTimestampFilter.getRequestDateTime()
                ),
                writingWorkerManager,
                skipNulls
        );

    }

    public static <F,T> Recorder<F> build(
            final Function<F,T> transform,
            final Function<NoType, DateTime> determineTimestamp,
            final WritingWorkerManager<T> writingWorkerManager,
            final boolean skipNulls)
    {
        return new Recorder<F>() {
            @Override
            public void record(F data) {
                try {
                    if ( !(skipNulls && data == null)) {
                        T transformed = transform.apply( data );

                        if ( !(skipNulls && transformed == null)) {
                            writingWorkerManager.record(
                                    determineTimestamp.apply(null),
                                    transformed);
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("Recorder exception.", e);

                }
            }
        };
    }

    public static <F,T> Recorder<F> build(
            final Function<F,T> transform,
            final DateTime dateTime,
            final WritingWorkerManager<T> writingWorkerManager,
            final boolean skipNulls)
    {
        return build(
                transform,
                new Function<NoType,DateTime>() {
                    @Nullable
                    @Override
                    public DateTime apply(@Nullable NoType input) {
                        return dateTime;
                    }
                },
                writingWorkerManager,
                skipNulls);
    }

    public static <T> Recorder<T> build(
            final DateTime dateTime,
            final WritingWorkerManager<T> writingWorkerManager,
            final boolean skipNulls)
    {
        return build(Functions.<T>identity(), dateTime, writingWorkerManager, skipNulls);
    }

    public static <F,T> Recorder build(
            final Function<NoType, DateTime> determineTimestamp,
            final WritingWorkerManager<T> writingWorkerManager,
            final boolean skipNulls)
    {
        return build(Functions.<T>identity(), determineTimestamp, writingWorkerManager, skipNulls);
    }

    public static <F,T> Recorder<F> build(
            final Function<F,T> transform,
            final Function<NoType, DateTime> determineTimestamp,
            final WritingWorkerManager<T> writingWorkerManager)
    {
        return build(transform,determineTimestamp,writingWorkerManager,false);
    }

    public static <F,T> Recorder<F> build(
            final Function<F,T> transform,
            final DateTime dateTime,
            final WritingWorkerManager<T> writingWorkerManager)
    {
        return build(transform,dateTime,writingWorkerManager,false);
    }

    public static <T> Recorder<T> build(
            final DateTime dateTime,
            final WritingWorkerManager<T> writingWorkerManager)
    {
        return build(dateTime, writingWorkerManager, false);
    }

    public static <F,T> Recorder build(
            final Function<NoType, DateTime> determineTimestamp,
            final WritingWorkerManager<T> writingWorkerManager)
    {
        return build(determineTimestamp, writingWorkerManager, false);
    }
    */
}
