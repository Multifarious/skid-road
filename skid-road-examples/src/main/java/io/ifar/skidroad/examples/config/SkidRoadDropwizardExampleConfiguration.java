package io.ifar.skidroad.examples.config;

import io.dropwizard.Configuration;
import io.ifar.skidroad.dropwizard.config.SkidRoadConfiguration;

public class SkidRoadDropwizardExampleConfiguration extends Configuration {
    SkidRoadConfiguration skidRoad;

    public SkidRoadConfiguration getSkidRoad() {
        return skidRoad;
    }
}
