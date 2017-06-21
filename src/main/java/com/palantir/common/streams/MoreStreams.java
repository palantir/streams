/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.common.streams;

import static java.util.Spliterators.spliteratorUnknownSize;

import java.util.Iterator;
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
     *
     * @deprecated Use {@link com.google.common.collect.Streams#stream(Iterable)}, available in Guava 21+
     */
    @Deprecated
    public static <T> Stream<T> stream(Iterable<? extends T> iterable) {
        @SuppressWarnings("unchecked")
        Stream<T> stream = (Stream<T>) StreamSupport.stream(iterable.spliterator(), NOT_PARALLEL);
        return stream;
    }

    /**
     * Returns a stream containing the value held by {@code optionalValue}, or an empty stream
     * if {@code optionalValue} is empty.
     *
     * @deprecated Use {@link com.google.common.collect.Streams#stream(Optional)}, available in Guava 21+
     */
    @Deprecated
    public static <T> Stream<T> stream(Optional<T> optionalValue) {
        return optionalValue.map(Stream::of).orElse(Stream.of());
    }

    /**
     * Returns a stream from the provided iterator, preserving the iteration order.
     *
     * @param iterator Iterator for which a stream needs to be created
     * @param <T> Type parameter for the iterator
     * @return A stream for the iterator
     *
     * @deprecated Use {@link com.google.common.collect.Streams#stream(Iterator)}, available in Guava 21+
     */
    @Deprecated
    public static <T> Stream<T> stream(Iterator<T> iterator) {
        return StreamSupport.stream(spliteratorUnknownSize(iterator, 0), NOT_PARALLEL);
    }

    private MoreStreams() {}
}
