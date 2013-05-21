package io.ifar.skidroad.examples.rest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents the Requests to be recorded via SkidRoad
 */
public class RainbowRequest {

    private final double radius;
    private final boolean withGold;

    @JsonCreator
    public RainbowRequest(@JsonProperty("radius") double radius, @JsonProperty("with_gold") boolean withGold) {
        this.radius = radius;
        this.withGold = withGold;
    }

    public double getRadius() {
        return radius;
    }

    public boolean isWithGold() {
        return withGold;
    }
}
