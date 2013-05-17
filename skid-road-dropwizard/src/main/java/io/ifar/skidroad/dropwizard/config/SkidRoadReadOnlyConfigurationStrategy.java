package io.ifar.skidroad.dropwizard.config;

import com.yammer.dropwizard.config.Configuration;

/**
 *
 */
public interface SkidRoadReadOnlyConfigurationStrategy<T extends Configuration> {

    SkidRoadReadOnlyConfiguration getSkidRoadReadOnlyConfiguration(T configuration);
}
