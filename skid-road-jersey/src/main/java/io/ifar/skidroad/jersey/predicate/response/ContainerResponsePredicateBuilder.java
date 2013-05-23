package io.ifar.skidroad.jersey.predicate.response;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerResponse;

/**
 * Allows boolean logic combinations of ContainerRequestResponsePredicates
 */
public class ContainerResponsePredicateBuilder {
    /**
     * Builds new ContainerResponsePredicate that returns true if and only if all provided predicates return true.
     */
    public static ContainerResponsePredicate and (final ContainerResponsePredicate... predicates) {
        return new ContainerResponsePredicate() {
            @Override
            public boolean isMatch(ContainerRequest request, ContainerResponse response) {
                for (ContainerResponsePredicate p : predicates) {
                    if (!p.isMatch(request, response)) {
                        return false;
                    }
                }
                return true;
            }
        };
    }

    /**
     * Builds new ContainerResponsePredicate that returns false if and only if all provided predicates return false.
     */
    public static ContainerResponsePredicate or (final ContainerResponsePredicate... predicates) {
        return new ContainerResponsePredicate() {
            @Override
            public boolean isMatch(ContainerRequest request, ContainerResponse response) {
                for (ContainerResponsePredicate p : predicates) {
                    if (p.isMatch(request, response)) {
                        return true;
                    }
                }
                return false;
            }
        };
    }
}
