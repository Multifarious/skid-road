package io.ifar.skidroad.jersey.predicate.response;

import io.ifar.skidroad.jersey.ContainerRequestAndResponse;

/**
 * Allows boolean logic combinations of ContainerRequestResponsePredicates
 */
public class ContainerResponsePredicateBuilder {
    public final static ContainerResponsePredicate ALWAYS = new ContainerResponsePredicate() {
        @Override
        public Boolean apply(ContainerRequestAndResponse input) {
            return true;
        }
    };

    public final static ContainerResponsePredicate NEVER = new ContainerResponsePredicate() {
        @Override
        public Boolean apply(ContainerRequestAndResponse input) {
            return false;
        }
    };

    /**
     * Builds new ContainerResponsePredicate that returns true if and only if all provided predicates return true.
     */
    public static ContainerResponsePredicate and (final ContainerResponsePredicate... predicates) {
        return new ContainerResponsePredicate() {
            @Override
            public Boolean apply(ContainerRequestAndResponse input) {
                for (ContainerResponsePredicate p : predicates) {
                    if (!p.apply(input)) {
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
            public Boolean apply(ContainerRequestAndResponse input) {
                for (ContainerResponsePredicate p : predicates) {
                    if (p.apply(input)) {
                        return true;
                    }
                }
                return false;
            }
        };
    }
}
