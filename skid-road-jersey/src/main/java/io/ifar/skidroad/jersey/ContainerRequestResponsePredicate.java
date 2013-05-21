package io.ifar.skidroad.jersey;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerResponse;

/**
 * Predicate to apply to ContainerRequest Response pairs.
 *
 * Intended use: allow ContainerResponseFilters to be selectively applied to some requests and not others.
 *
 * ContainerRequestResponsePredicate implementations should be thread-safe.
 */
public interface ContainerRequestResponsePredicate {
    boolean isMatch(ContainerRequest request, ContainerResponse response);
}
