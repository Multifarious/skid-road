package io.ifar.skidroad.jersey.single;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;

import java.util.UUID;

/**
 * Generates a unique UUID for this request and stashes it in a ThreadLocal for the duration of the request.
 */
public class UUIDGeneratorFilter implements ContainerRequestFilter {
    private static ThreadLocal<String> requestID = new ThreadLocal<>();

    @Override
    public ContainerRequest filter(ContainerRequest request) {
        requestID.set(UUID.randomUUID().toString()); //for perf, could look into sequentially generating UUIDs if predictability not a security concern.
        return request;
    }

    public static String getID() {
        return requestID.get();
    }
}
