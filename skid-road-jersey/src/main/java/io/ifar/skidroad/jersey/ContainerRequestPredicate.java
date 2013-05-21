package io.ifar.skidroad.jersey;

import com.sun.jersey.spi.container.ContainerRequest;

/**
 * Predicate to apply to ContainerRequests.
 *
 * Intended use: allow ContainerRequestFilters to be selectively applied to some requests and not others.
 *
 * ContainerRequestPredicate implementations should be thread-safe.
 */
public interface ContainerRequestPredicate {
    boolean isMatch(ContainerRequest request);
}
