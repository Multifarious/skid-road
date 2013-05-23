package io.ifar.skidroad.jersey.predicate.response;

import com.google.common.collect.ImmutableSet;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerResponse;

import javax.ws.rs.core.Response;
import java.util.HashSet;
import java.util.Set;

import static javax.ws.rs.core.Response.Status.*;

/**
 * ContainerResponsePredicate that bases decision on set of HTTP Status codes to include.
 * request path.
 */
public class StatusCodeContainerResponsePredicate implements ContainerResponsePredicate {
    public final static StatusCodeContainerResponsePredicate OK_PREDICATE =
            of(OK);

    public final static StatusCodeContainerResponsePredicate SUCCESS_PREDICATE =
            of(OK, ACCEPTED, CREATED, NO_CONTENT);

    public static StatusCodeContainerResponsePredicate of(Response.Status... statusCodes) {
        Set<Integer> integers = new HashSet<>(statusCodes.length);
        for (Response.Status s : statusCodes) {
            integers.add(s.getStatusCode());
        }
        return new StatusCodeContainerResponsePredicate(ImmutableSet.copyOf(integers.iterator()));
    }

    private final Set<Integer> statusCodes;

    public StatusCodeContainerResponsePredicate(Set <Integer> statusCodes) {
        this.statusCodes = statusCodes;
    }

    @Override
    public boolean isMatch(ContainerRequest request, ContainerResponse response) {
        return statusCodes.contains(response.getStatus());
    }
}
