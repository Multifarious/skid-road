package io.ifar.skidroad.jersey.single;

import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import java.util.UUID;

/**
 * Generates a unique UUID for this request and stashes it in a ThreadLocal for the duration of the request.
 */
@Priority(Priorities.AUTHENTICATION - 100)
public class UUIDGeneratorFilter implements ContainerRequestFilter {
    private static ThreadLocal<String> requestID = new ThreadLocal<>();

    @Override
    public void filter(ContainerRequestContext requestContext) {
        requestID.set(UUID.randomUUID().toString()); //for perf, could look into sequentially generating UUIDs if predictability not a security concern.
    }

    public static String getID() {
        return requestID.get();
    }
}
