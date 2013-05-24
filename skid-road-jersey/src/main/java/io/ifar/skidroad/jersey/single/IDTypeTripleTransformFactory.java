package io.ifar.skidroad.jersey.single;

import com.google.common.base.Function;
import io.ifar.goodies.Triple;

import javax.annotation.Nullable;

/**
 * Support for use case of using Skid Road to record (request id, tag, data) triples.
 */
public class IDTypeTripleTransformFactory {

    /**
     * Generates a transformer that wraps data in a Triple of (request id, data type tag, data).
     */
    public static <T> Function<T,Triple<String,String,T>> buildTransform(final String typeTag) {
        return new Function<T, Triple<String, String, T>>() {
            @Nullable
            @Override
            public Triple<String, String, T> apply(@Nullable T input) {
                return new Triple<>(
                        UUIDGeneratorFilter.getID(),
                        typeTag,
                        input
                );
            }
        };
    }

    /**
     * Generates a transformer that wraps data in a Triple of (request id, data type tag, transformed data).
     */
    public static <F,T> Function<F,Triple<String,String,T>> buildTransform(final String typeTag, final Function<F,T> dataTransform) {
        return new Function<F, Triple<String, String, T>>() {
            @Nullable
            @Override
            public Triple<String, String, T> apply(@Nullable F input) {
                return new Triple<>(
                        UUIDGeneratorFilter.getID(),
                        typeTag,
                        dataTransform.apply(input)
                );
            }
        };
    }
}
