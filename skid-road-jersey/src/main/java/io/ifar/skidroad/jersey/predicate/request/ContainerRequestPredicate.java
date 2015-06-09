package io.ifar.skidroad.jersey.predicate.request;

import com.google.common.base.Function;

import javax.ws.rs.container.ContainerRequestContext;

/**
 * Predicate to apply to ContainerRequests.
 *
 * Intended use: allow ContainerRequestFilters to be selectively applied to some requests and not others.
 *
 * ContainerRequestPredicate implementations should be thread-safe.
 */
public interface ContainerRequestPredicate extends Function<ContainerRequestContext,Boolean> {
    @Override
    Boolean apply(ContainerRequestContext requestContext);
}
