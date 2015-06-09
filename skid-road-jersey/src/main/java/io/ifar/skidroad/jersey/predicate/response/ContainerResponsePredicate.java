package io.ifar.skidroad.jersey.predicate.response;

import com.google.common.base.Function;
import io.ifar.skidroad.jersey.ContainerRequestAndResponse;

/**
 * Predicate to apply to ContainerRequest / ContainerResponse pairs.
 *
 * Intended use: allow ContainerResponseFilters to be selectively applied to some requests and not others.
 *
 * ContainerResponsePredicate implementations should be thread-safe.
 */
public interface ContainerResponsePredicate extends Function<ContainerRequestAndResponse,Boolean> {
    Boolean apply(ContainerRequestAndResponse input);
}
