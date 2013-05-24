package io.ifar.skidroad.jersey.single;

import com.google.common.base.Function;
import com.sun.jersey.api.container.ContainerException;
import com.sun.jersey.core.util.ReaderWriter;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;
import io.ifar.skidroad.jersey.predicate.request.ContainerRequestPredicate;
import io.ifar.skidroad.writing.WritingWorkerManager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Records the entity (payload) of an incoming ContainerRequest. Byte stream is read into a String, respecting
 * content encoding (if provided). String is then passed to a transform Function to make it ready for recording by
 * Skid Road WritingWorkerManager.
 */
public class RequestEntityRecorder<T> implements ContainerRequestFilter {
    public final static String REQUEST_TYPE_KEY = "REQ";

    private final ContainerRequestPredicate predicate;
    private final WritingWorkerManager<T> writingWorkerManager;
    private final Function<String,T> transformFunction;

    /**
     * Construct a RequestEntityRecorder that records all requests.
     * @param transformFunction Transforms request entity bytes into a form that the provided WritingWorkerManager can serialize.
     */
    public RequestEntityRecorder(Function<String, T> transformFunction, WritingWorkerManager<T> writingWorkerManager) {
        this(null, transformFunction, writingWorkerManager);
    }

    /**
     * Construct a RequestEntityRecorder that records requests which match the provided predicate.
     * @param transformFunction Transforms request entity bytes into a form that the provided WritingWorkerManager can serialize.
     */
    public RequestEntityRecorder(ContainerRequestPredicate predicate, Function<String, T> transformFunction, WritingWorkerManager<T> writingWorkerManager) {
        this.predicate = predicate;
        this.transformFunction = transformFunction;
        this.writingWorkerManager = writingWorkerManager;
    }

    @Override
    public ContainerRequest filter(ContainerRequest request) {
        if (predicate == null || predicate.isMatch(request)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            InputStream in = request.getEntityInputStream();
            try {
                if(in.available() > 0) {
                    ReaderWriter.writeTo(in, out);

                    byte[] requestEntity = out.toByteArray();

                    request.setEntityInputStream(new ByteArrayInputStream(requestEntity));
                    writingWorkerManager.record(
                            System.currentTimeMillis(),
                            transformFunction.apply(
                                    ReaderWriter.readFromAsString(new ByteArrayInputStream(requestEntity), request.getMediaType())
                            ));
                }
            } catch (IOException ex) {
                throw new ContainerException(ex);
            }
        }
        return request;
    }
}
