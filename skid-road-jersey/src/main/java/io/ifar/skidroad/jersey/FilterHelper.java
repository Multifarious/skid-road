package io.ifar.skidroad.jersey;

import com.google.common.collect.ImmutableList;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.spi.container.ContainerRequestFilter;
import com.sun.jersey.spi.container.ContainerResponseFilter;
import com.yammer.dropwizard.config.Environment;

import java.util.List;

/**
 */
public class FilterHelper {

    /**
     * Adds provided filter to the Dropwizard Jersey configuration.
     *
     * Note: do not use in combination with mechanisms that set the filter list via other formats (e.g. Strings, Class objects)
     */
    public static void addFilter(Environment env, ContainerRequestFilter filter) {
        List<ContainerRequestFilter> l = env.getJerseyProperty(ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS);
        ImmutableList.Builder<ContainerRequestFilter> builder = ImmutableList.builder();
        if (l != null) {
            builder.addAll(l.iterator());
        }
        builder.add(filter);
        env.setJerseyProperty(ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS, builder.build());
    }

    /**
     * Adds provided filter to the Dropwizard Jersey configuration.
     *
     * Note: do not use in combination with mechanisms that set the filter list via other formats (e.g. Strings, Class objects)
     */
    public static void addFilter(Environment env, ContainerResponseFilter filter) {
        List<ContainerResponseFilter> l = env.getJerseyProperty(ResourceConfig.PROPERTY_CONTAINER_RESPONSE_FILTERS);
        ImmutableList.Builder<ContainerResponseFilter> builder = ImmutableList.builder();
        if (l != null) {
            builder.addAll(l.iterator());
        }
        builder.add(filter);
        env.setJerseyProperty(ResourceConfig.PROPERTY_CONTAINER_RESPONSE_FILTERS, builder.build());
    }
}
