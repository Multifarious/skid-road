package io.ifar.skidroad.jersey;


import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;

/**
 * Simple request/response tuple
 */
public class ContainerRequestAndResponse {
    private final ContainerRequestContext request;
    private final ContainerResponseContext response;

    public ContainerRequestAndResponse(ContainerRequestContext request, ContainerResponseContext response) {
        this.request = request;
        this.response = response;
    }

    public ContainerRequestContext getRequest() {
        return request;
    }

    public ContainerResponseContext getResponse() {
        return response;
    }
}
