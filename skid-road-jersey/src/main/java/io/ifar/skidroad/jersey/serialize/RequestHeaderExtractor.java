package io.ifar.skidroad.jersey.serialize;

import com.sun.jersey.spi.container.ContainerRequest;

import javax.ws.rs.core.MultivaluedMap;

/**
 * Processes ContainerRequest objects, returning headers to be serialized in Skid Road output.
 *
 * Standard use-case is to extract those headers which are of interest, but interface intentionally allows injection of
 * new data.
 *
 * Implementations should be thread-safe.
 */
public interface RequestHeaderExtractor {
    /**
     * @param request Request from which to extract HTTP headers
     * @return Headers to serialize
     */
    MultivaluedMap<String,String> extract(ContainerRequest request);
}
