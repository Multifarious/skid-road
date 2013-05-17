package io.ifar.skidroad.dropwizard.config;

import com.yammer.dropwizard.config.Configuration;

/**
 *
 */
public interface SkidRoadConfigurationStrategy<T extends Configuration> {

    SkidRoadConfiguration getSkidRoadConfiguration(T configuration);
}
