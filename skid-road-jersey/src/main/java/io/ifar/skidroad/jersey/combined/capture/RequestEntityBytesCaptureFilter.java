package io.ifar.skidroad.jersey.combined.capture;

import io.ifar.skidroad.jersey.predicate.request.ContainerRequestPredicate;
import org.glassfish.jersey.message.internal.ReaderWriter;
import org.glassfish.jersey.server.ContainerException;
import org.glassfish.jersey.server.ContainerRequest;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
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
    public static byte[] getEntityBytes(ContainerRequestContext request) {
        Object result = request.getProperty(REQUEST_ENTITY);
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
    public void filter(ContainerRequestContext request) {
        if (predicate == null || predicate.apply(request)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            InputStream in = request.getEntityStream();
            try {
                if(in.available() > 0) {
                    ReaderWriter.writeTo(in, out);

                    byte[] requestEntity = out.toByteArray();

                    request.setEntityStream(new ByteArrayInputStream(requestEntity));
                    request.setProperty(REQUEST_ENTITY, requestEntity);
                }
            } catch (IOException ex) {
                throw new ContainerException(ex);
            }
        }

    }
}
