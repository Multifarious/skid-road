package io.ifar.skidroad.dropwizard.config;

import io.dropwizard.Configuration;

/**
 *
 */
public interface SkidRoadReadOnlyConfigurationStrategy<T extends Configuration> {

    SkidRoadReadOnlyConfiguration getSkidRoadReadOnlyConfiguration(T configuration);
}
