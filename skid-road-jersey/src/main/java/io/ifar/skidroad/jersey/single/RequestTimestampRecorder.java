package io.ifar.skidroad.jersey.single;

import com.google.common.base.Function;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;
import io.ifar.skidroad.jersey.predicate.request.ContainerRequestPredicate;
import io.ifar.skidroad.writing.WritingWorkerManager;
import org.joda.time.DateTime;

/**
 * Records the entity (payload) of an incoming ContainerRequest. Byte stream is read into a String, respecting
 * content encoding (if provided). String is then passed to a transform Function to make it ready for recording by
 * Skid Road WritingWorkerManager.
 */
public class RequestTimestampRecorder<T> implements ContainerRequestFilter {
    public final static String TIMESTAMP_TYPE_KEY = "TIMESTAMP";

    private final ContainerRequestPredicate predicate;
    private final WritingWorkerManager<T> writingWorkerManager;
    private final Function<DateTime,T> transformFunction;

    /**
     * Construct a RequestEntityRecorder that records all requests.
     * @param transformFunction Transforms request entity bytes into a form that the provided WritingWorkerManager can serialize.
     */
    public RequestTimestampRecorder(Function<DateTime, T> transformFunction, WritingWorkerManager<T> writingWorkerManager) {
        this(null, transformFunction, writingWorkerManager);
    }

    /**
     * Construct a RequestEntityRecorder that records requests which match the provided predicate.
     * @param transformFunction Transforms request DateTime into a form that the provided WritingWorkerManager can serialize.
     */
    public RequestTimestampRecorder(ContainerRequestPredicate predicate, Function<DateTime, T> transformFunction, WritingWorkerManager<T> writingWorkerManager) {
        this.predicate = predicate;
        this.transformFunction = transformFunction;
        this.writingWorkerManager = writingWorkerManager;
    }

    @Override
    public ContainerRequest filter(ContainerRequest request) {
        if (predicate == null || predicate.isMatch(request)) {
            writingWorkerManager.record(
                    RequestTimestampFilter.getRequestDateTime(),
                    transformFunction.apply(RequestTimestampFilter.getRequestDateTime())
            );
        }
        return request;
    }
}
