package io.ifar.skidroad.jersey;

import com.sun.jersey.api.container.ContainerException;
import com.sun.jersey.core.util.ReaderWriter;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Captures the entity (payload) byte stream of an incoming ContainerRequest and places it
 * in the request's properties map for later access.
 */
public class RequestEntityBytesCaptureFilter implements ContainerRequestFilter {
    public final static String REQUEST_ENTITY = "REQUEST_ENTITY_BYTES";

    /**
     * Return captured byte array for provided request or null if not captured.
     */
    public static byte[] getEntityBytes(ContainerRequest request) {
        Object result = request.getProperties().get(REQUEST_ENTITY);
        return (byte[]) result;
    }

    private final ContainerRequestPredicate predicate;

    /**
     * Construct a RequestEntityBytesCaptureFilter that captures all requests.
     */
    public RequestEntityBytesCaptureFilter() {
        predicate = null;
    }

    /**
     * Construct a RequestEntityBytesCaptureFilter that captures requests which match the provided predicate.
     */
    public RequestEntityBytesCaptureFilter(ContainerRequestPredicate predicate) {
        this.predicate = predicate;
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
                    request.getProperties().put(REQUEST_ENTITY, requestEntity);
                }
            } catch (IOException ex) {
                throw new ContainerException(ex);
            }
        }
        return request;
    }
}
