package io.ifar.skidroad.jersey.predicate.request;

import com.sun.jersey.spi.container.ContainerRequest;

/**
 * Allows boolean logic combinations of ContainerRequestPredicates
 */
public class ContainerRequestPredicateBuilder {
    public final static ContainerRequestPredicate ALWAYS = new ContainerRequestPredicate() {
        @Override
        public Boolean apply(ContainerRequest request) {
            return true;
        }
    };

    public final static ContainerRequestPredicate NEVER = new ContainerRequestPredicate() {
        @Override
        public Boolean apply(ContainerRequest request) {
            return false;
        }
    };

    /**
     * Builds new ContainerRequestPredicate that returns true if and only if all provided predicates return true.
     */
    public static ContainerRequestPredicate and (final ContainerRequestPredicate... predicates) {
        return new ContainerRequestPredicate() {
            @Override
            public Boolean apply(ContainerRequest request) {
                for (ContainerRequestPredicate p : predicates) {
                    if (!p.apply(request)) {
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
            public Boolean apply(ContainerRequest request) {
                for (ContainerRequestPredicate p : predicates) {
                    if (p.apply(request)) {
                        return true;
                    }
                }
                return false;
            }
        };
    }
}
