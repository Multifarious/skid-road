package io.ifar.skidroad.jersey.capture;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerResponse;

/**
 * Predicate to apply to ContainerRequest / ContainerResponse pairs.
 *
 * Intended use: allow ContainerResponseFilters to be selectively applied to some requests and not others.
 *
 * ContainerResponsePredicate implementations should be thread-safe.
 */
public interface ContainerResponsePredicate {
    boolean isMatch(ContainerRequest request, ContainerResponse response);
}
