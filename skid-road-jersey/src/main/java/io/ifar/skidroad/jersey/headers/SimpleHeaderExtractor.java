package io.ifar.skidroad.jersey.headers;

import com.google.common.collect.ImmutableSet;
import org.glassfish.jersey.internal.util.collection.StringKeyIgnoreCaseMultivaluedMap;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.core.MultivaluedMap;
import java.util.*;

/**
 * Extracts headers by name from ContainerRequests or ContainerResponses.
 *
 * Name comparisons are done in a case-insensitive manner.
 *
 * Note that because SimpleHeaderExtractor implements both {@link RequestHeaderExtractor} and
 * {@link ResponseHeaderExtractor}, it must be cast to one of those interfaces in order to be used with
 * {@link io.ifar.skidroad.jersey.combined.serialize.JSONContainerRequestAndResponseSerializer}'s overloaded {@code with} methods.
 *
 */
public class SimpleHeaderExtractor implements RequestHeaderExtractor, ResponseHeaderExtractor {
    protected static enum Mode {ONLY_THESE, EVERYTHING_BUT}

    protected final ImmutableSet<String> filterList;
    protected final Mode mode;

    /**
     * Constructs a SimpleHeaderExtractor which only extracts the specified headers
     * @param headers
     */
    public static SimpleHeaderExtractor only(List<String> headers) {
        return new SimpleHeaderExtractor(headers, Mode.ONLY_THESE);
    }

    /**
     * Constructs a SimpleHeaderExtractor which only extracts the specified headers
     * @param headers
     */
    public static SimpleHeaderExtractor only(String... headers) {
        return new SimpleHeaderExtractor(ImmutableSet.copyOf(headers), Mode.ONLY_THESE);
    }

    /**
     * Constructs a SimpleHeaderExtractor which extracts all but the specified headers
     * @param headers
     */
    public static SimpleHeaderExtractor except(List<String> headers) {
        return new SimpleHeaderExtractor(headers, Mode.EVERYTHING_BUT);
    }

    /**
     * Constructs a SimpleHeaderExtractor which extracts all but the specified headers
     * @param headers
     */
    public static SimpleHeaderExtractor except(String... headers) {
        return new SimpleHeaderExtractor(ImmutableSet.copyOf(headers), Mode.EVERYTHING_BUT);
    }

    protected SimpleHeaderExtractor(Collection<String> headers, Mode mode) {
        Set<String> lowerCase = new HashSet<>();
        for (String header : headers) {
            lowerCase.add(header.toLowerCase());
        }
        this.filterList = ImmutableSet.copyOf(lowerCase.iterator());
        this.mode = mode;
    }

    public <T> void extract(MultivaluedMap<String,T> headers, MultivaluedMap<String,T> result) {
        for (Map.Entry<String, List<T>> entry : headers.entrySet()) {
            boolean putThisOne;
            if (this.mode == Mode.ONLY_THESE) {
                putThisOne = filterList.contains(entry.getKey().toLowerCase());
            } else {
                putThisOne = !filterList.contains(entry.getKey().toLowerCase());
            }
            if (putThisOne) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public MultivaluedMap<String, String> extract(ContainerRequestContext request) {
        MultivaluedMap<String,String> result = new StringKeyIgnoreCaseMultivaluedMap<>();
        extract(request.getHeaders(), result);
        return result;
    }

    /**
     * Note: headers are only extract from the response. Request is ignored.
     * @param response Request from which to extract HTTP headers
     * @return
     */
    @Override
    public MultivaluedMap<String, Object> extract(ContainerRequestContext request, ContainerResponseContext response) {
        MultivaluedMap<String,Object> result = new StringKeyIgnoreCaseMultivaluedMap<>();
        extract(response.getHeaders(), result);
        return result;
    }
}
