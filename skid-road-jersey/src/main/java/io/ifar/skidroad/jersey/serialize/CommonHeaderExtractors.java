package io.ifar.skidroad.jersey.serialize;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerResponse;

import javax.ws.rs.core.MultivaluedMap;

public class CommonHeaderExtractors {
    public final static RequestHeaderExtractor NO_REQUEST_HEADERS = new RequestHeaderExtractor() {
        @Override
        public MultivaluedMap<String, String> extract(ContainerRequest request) {
            return null;
        }
    };

    public final static RequestHeaderExtractor ALL_REQUEST_HEADERS = new RequestHeaderExtractor() {
        @Override
        public MultivaluedMap<String, String> extract(ContainerRequest request) {
            return request.getRequestHeaders();
        }
    };

    public final static ResponseHeaderExtractor NO_RESPONSE_HEADERS = new ResponseHeaderExtractor() {
        @Override
        public MultivaluedMap<String, Object> extract(ContainerRequest request, ContainerResponse response) {
            return null;
        }
    };

    public final static ResponseHeaderExtractor ALL_RESPONSE_HEADERS = new ResponseHeaderExtractor() {
        @Override
        public MultivaluedMap<String, Object> extract(ContainerRequest request, ContainerResponse response) {
            return response.getHttpHeaders();
        }
    };
}
