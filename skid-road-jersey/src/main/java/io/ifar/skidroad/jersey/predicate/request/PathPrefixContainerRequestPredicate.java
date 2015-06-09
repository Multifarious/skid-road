package io.ifar.skidroad.jersey.predicate.request;

import javax.ws.rs.container.ContainerRequestContext;

/**
 * ContainerRequestPredicate that bases decision on simple String comparison against
 * request path.
 */
public class PathPrefixContainerRequestPredicate implements ContainerRequestPredicate {
    private final String prefix;

    public PathPrefixContainerRequestPredicate(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public Boolean apply(ContainerRequestContext requestContext) {
        return requestContext.getUriInfo().getPath().startsWith(prefix);
    }
}
