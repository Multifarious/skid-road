package io.ifar.skidroad.jersey.predicate.request;

import com.sun.jersey.spi.container.ContainerRequest;

/**
 * Allows boolean logic combinations of ContainerRequestPredicates
 */
public class ContainerRequestPredicateBuilder {
    /**
     * Builds new ContainerRequestPredicate that returns true if and only if all provided predicates return true.
     */
    public static ContainerRequestPredicate and (final ContainerRequestPredicate... predicates) {
        return new ContainerRequestPredicate() {
            @Override
            public boolean isMatch(ContainerRequest request) {
                for (ContainerRequestPredicate p : predicates) {
                    if (!p.isMatch(request)) {
                        return false;
                    }
                }
                return true;
            }
        };
    }

    /**
     * Builds new ContainerRequestPredicate that returns false if and only if all provided predicates return false.
     */
    public static ContainerRequestPredicate or (final ContainerRequestPredicate... predicates) {
        return new ContainerRequestPredicate() {
            @Override
            public boolean isMatch(ContainerRequest request) {
                for (ContainerRequestPredicate p : predicates) {
                    if (p.isMatch(request)) {
                        return true;
                    }
                }
                return false;
            }
        };
    }
}
