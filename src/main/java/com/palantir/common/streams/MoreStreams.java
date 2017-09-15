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

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import static java.util.Spliterators.spliteratorUnknownSize;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Utility methods for Java 8 {@link Stream}.
 */
public class MoreStreams {

    private static final boolean NOT_PARALLEL = false;

    /**
     * Given a {@code Stream<ListenableFuture<U>>}, this function will return a blocking stream of the completed
     * futures in completion order, looking at most {@code maxParallelism} futures ahead in the stream.
     */
    public static <U> Stream<ListenableFuture<U>> inCompletionOrder(
            Stream<ListenableFuture<U>> arguments, int maxParallelism) {
        return StreamSupport.stream(
                new InCompletionOrderSpliterator<>(arguments.spliterator(), maxParallelism), NOT_PARALLEL);
    }

    /**
     * A convenient variant of {@link #inCompletionOrder(Stream, int)} in which the user passes in a
     * function and an executor to run it on.
     */
    public static <U, V> Stream<V> inCompletionOrder(
            Stream<U> arguments, Function<U, V> mapper, Executor executor, int maxParallelism) {
        return inCompletionOrder(
                arguments.map(x -> Futures.transform(Futures.immediateFuture(x), mapper::apply, executor)),
                maxParallelism)
                .map(Futures::getUnchecked);
    }

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
