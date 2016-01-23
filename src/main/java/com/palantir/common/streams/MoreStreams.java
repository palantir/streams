package com.palantir.common.streams;

import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Utility methods for Java 8 {@link Stream}.
 */
public class MoreStreams {

    private static final boolean NOT_PARALLEL = false;

    /**
     * Returns a stream of the values returned by {@code iterable}.
     */
    public static <T> Stream<T> stream(Iterable<? extends T> iterable) {
        @SuppressWarnings("unchecked")
        Stream<T> stream = (Stream<T>) StreamSupport.stream(iterable.spliterator(), NOT_PARALLEL);
        return stream;
    }

    /**
     * Returns a stream containing the value held by {@code optionalValue}, or an empty stream
     * if {@code optionalValue} is empty.
     */
    public static <T> Stream<T> stream(Optional<T> optionalValue) {
        return optionalValue.map(Stream::of).orElse(Stream.of());
    }

    private MoreStreams() {}
}
