/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.common.streams.BufferingSpliterator.InCompletionOrder;
import com.palantir.common.streams.BufferingSpliterator.InSourceOrder;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterators;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Utility methods for Java 8 {@link Stream}.
 */
public final class MoreStreams {

    private static final boolean NOT_PARALLEL = false;

    /**
     * Given a stream of listenable futures, this function will return a blocking stream of the completed
     * futures in completion order, looking at most {@code maxParallelism} futures ahead in the stream.
     *
     * @deprecated This function provides no guarantees, maxParallelism is
     * ignored in many cases (e.g. flatmap has been called).
     */
    @Deprecated
    public static <T, F extends ListenableFuture<T>> Stream<F> inCompletionOrder(
            Stream<F> futures, int maxParallelism) {
        return StreamSupport.stream(
                new BufferingSpliterator<>(
                        InCompletionOrder.INSTANCE, futures.spliterator(), Function.identity(), maxParallelism),
                NOT_PARALLEL);
    }

    /**
     * A convenient variant of {@link #inCompletionOrder(Stream, int)} in which the user passes in a
     * function and an executor to run it on.
     */
    public static <U, V> Stream<V> inCompletionOrder(
            Stream<U> arguments, Function<U, V> mapper, Executor executor, int maxParallelism) {
        return StreamSupport.stream(
                        new BufferingSpliterator<>(
                                InCompletionOrder.INSTANCE,
                                arguments.spliterator(),
                                x -> Futures.transform(Futures.immediateFuture(x), mapper::apply, executor),
                                maxParallelism),
                        NOT_PARALLEL)
                .map(Futures::getUnchecked);
    }

    /**
     * This function will return a blocking stream that waits for each future to complete before returning it,
     * but which looks ahead {@code maxParallelism} futures to ensure a fixed parallelism rate.
     *
     * @deprecated This function provides no guarantees, maxParallelism is
     * ignored in many cases (e.g. flatmap has been called).
     */
    @Deprecated
    public static <T, F extends ListenableFuture<T>> Stream<F> blockingStreamWithParallelism(
            Stream<F> futures, int maxParallelism) {
        return StreamSupport.stream(
                        new BufferingSpliterator<>(
                                InSourceOrder.INSTANCE, futures.spliterator(), Function.identity(), maxParallelism),
                        NOT_PARALLEL)
                .map(MoreFutures::blockUntilCompletion);
    }

    /**
     * A convenient variant of {@link #blockingStreamWithParallelism(Stream, int)} in which the user passes in a
     * function and an executor to run it on.
     */
    public static <U, V> Stream<V> blockingStreamWithParallelism(
            Stream<U> arguments, Function<U, V> mapper, Executor executor, int maxParallelism) {
        return StreamSupport.stream(
                        new BufferingSpliterator<>(
                                InSourceOrder.INSTANCE,
                                arguments.spliterator(),
                                x -> Futures.transform(Futures.immediateFuture(x), mapper::apply, executor),
                                maxParallelism),
                        NOT_PARALLEL)
                .map(MoreFutures::blockUntilCompletion)
                .map(Futures::getUnchecked);
    }

    /**
     * A convenient variant of {@link #blockingStreamWithParallelism(Stream, Function, Executor, int)} in which the user
     * passes in a consumer (rather than and a function) and the method returns once all members of the stream have
     * completed consumption.
     */
    public static <U> void blockingConsumeStreamWithParallelism(
            Stream<U> arguments, Consumer<U> consumer, Executor executor, int maxParallelism) {
        StreamSupport.stream(
                        new BufferingSpliterator<>(
                                InSourceOrder.INSTANCE,
                                arguments.spliterator(),
                                x -> Futures.transform(
                                        Futures.immediateFuture(x),
                                        y -> {
                                            consumer.accept(y);
                                            return null;
                                        },
                                        executor),
                                maxParallelism),
                        NOT_PARALLEL)
                .map(MoreFutures::blockUntilCompletion)
                .map(Futures::getUnchecked)
                .forEach(_unused -> {});
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
        return optionalValue.map(Stream::of).orElseGet(Stream::empty);
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
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), NOT_PARALLEL);
    }

    private MoreStreams() {}
}
