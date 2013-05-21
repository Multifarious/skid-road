package io.ifar.skidroad.jersey;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerResponse;

/**
 * Simple request/response tuple
 */
public class ContainerRequestAndResponse {
    private final ContainerRequest request;
    private final ContainerResponse response;

    public ContainerRequestAndResponse(ContainerRequest request, ContainerResponse response) {
        this.request = request;
        this.response = response;
    }

    public ContainerRequest getRequest() {
        return request;
    }

    public ContainerResponse getResponse() {
        return response;
    }
}
