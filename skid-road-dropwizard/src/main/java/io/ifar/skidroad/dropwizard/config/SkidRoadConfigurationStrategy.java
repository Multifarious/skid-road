package io.ifar.skidroad.dropwizard.config;


import io.dropwizard.Configuration;

/**
 *
 */
public interface SkidRoadConfigurationStrategy<T extends Configuration> {

    SkidRoadConfiguration getSkidRoadConfiguration(T configuration);
}
