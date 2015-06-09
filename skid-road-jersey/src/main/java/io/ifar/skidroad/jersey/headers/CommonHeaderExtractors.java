package io.ifar.skidroad.jersey.headers;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.core.MultivaluedMap;

public class CommonHeaderExtractors {
    public final static RequestHeaderExtractor NO_REQUEST_HEADERS = new RequestHeaderExtractor() {
        @Override
        public MultivaluedMap<String, String> extract(ContainerRequestContext request) {
            return null;
        }
    };

    public final static RequestHeaderExtractor ALL_REQUEST_HEADERS = new RequestHeaderExtractor() {
        @Override
        public MultivaluedMap<String, String> extract(ContainerRequestContext request) {
            return request.getHeaders();
        }
    };

    public final static ResponseHeaderExtractor NO_RESPONSE_HEADERS = new ResponseHeaderExtractor() {
        @Override
        public MultivaluedMap<String, Object> extract(ContainerRequestContext request, ContainerResponseContext response) {
            return null;
        }
    };

    public final static ResponseHeaderExtractor ALL_RESPONSE_HEADERS = new ResponseHeaderExtractor() {
        @Override
        public MultivaluedMap<String, Object> extract(ContainerRequestContext request, ContainerResponseContext response) {
            return response.getHeaders();
        }
    };
}
