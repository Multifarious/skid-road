package io.ifar.skidroad.dropwizard.config;

import org.joda.time.Duration;

/**
 *
 */
public enum RollingFrequencyUnit {
    minutely(Duration.standardMinutes(1)),
    five_minutely(Duration.standardMinutes(5)),
    ten_minutely(Duration.standardMinutes(10)),
    fifteen_minutely(Duration.standardMinutes(15)),
    twenty_minutely(Duration.standardMinutes(20)),
    thirty_minutely(Duration.standardMinutes(30)),
    hourly(Duration.standardHours(1)),
    daily(Duration.standardDays(1));

    private final Duration spacing;

    private RollingFrequencyUnit(Duration spacing) {
        this.spacing = spacing;
    }

    public Duration duration() {
        return spacing;
    }

}
