package io.ifar.skidroad.jersey.predicate.request;

import com.sun.jersey.spi.container.ContainerRequest;

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
    public boolean isMatch(ContainerRequest request) {
        return request.getPath().startsWith(prefix);
    }
}
