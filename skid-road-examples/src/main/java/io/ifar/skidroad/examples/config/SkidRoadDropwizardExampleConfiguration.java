package io.ifar.skidroad.examples.config;

import com.yammer.dropwizard.config.Configuration;
import io.ifar.skidroad.dropwizard.config.SkidRoadConfiguration;

public class SkidRoadDropwizardExampleConfiguration extends Configuration {
    SkidRoadConfiguration skidRoad;

    public SkidRoadConfiguration getSkidRoad() {
        return skidRoad;
    }
}
