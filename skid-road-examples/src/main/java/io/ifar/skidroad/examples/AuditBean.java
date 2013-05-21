package io.ifar.skidroad.examples;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.ifar.skidroad.examples.rest.RainbowRequest;
import io.ifar.skidroad.examples.rest.RainbowRequestResponse;

/**
 * Per-request audit information to be recorded via Skid-Road
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class AuditBean {
    private final RainbowRequest request;
    private final String favoriteColor;
    private final RainbowRequestResponse response;

    public AuditBean(RainbowRequest request, String favoriteColor, RainbowRequestResponse response) {
        this.request = request;
        this.favoriteColor = favoriteColor;
        this.response = response;
    }

    public RainbowRequest getRequest() {
        return request;
    }

    public String getFavoriteColor() {
        return favoriteColor;
    }

    public RainbowRequestResponse getResponse() {
        return response;
    }
}
