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

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import java.time.Duration;
import java.util.Spliterator;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class MoreStreamsTests {
    private SettableFuture<String> firstInSource = SettableFuture.create();
    private SettableFuture<String> secondInSource = SettableFuture.create();

    @Mock
    private Spliterator<SettableFuture<String>> spliterator;

    private Stream<SettableFuture<String>> stream;

    private final AtomicBoolean streamClosed = new AtomicBoolean(false);

    @BeforeEach
    public void before() {
        when(spliterator.tryAdvance(any()))
                .thenAnswer(x -> {
                    Consumer<ListenableFuture<String>> consumer = x.getArgument(0);
                    consumer.accept(firstInSource);
                    return true;
                })
                .thenAnswer(x -> {
                    Consumer<ListenableFuture<String>> consumer = x.getArgument(0);
                    consumer.accept(secondInSource);
                    return true;
                })
                .thenAnswer(_x -> {
                    secondInSource.set("first to be completed");
                    firstInSource.set("second to be completed");
                    return false;
                })
                .thenReturn(false);

        stream = StreamSupport.stream(spliterator, false).onClose(() -> streamClosed.set(true));
    }

    @SuppressWarnings("DoNotCall")
    @Test
    public void testInCompletionOrder_future() {
        Stream<SettableFuture<String>> completedFutureStream = MoreStreams.inCompletionOrder(stream, 3);
        assertThat(completedFutureStream).containsExactly(secondInSource, firstInSource);
        assertThat(streamClosed).isTrue();
    }

    @SuppressWarnings("DoNotCall")
    @Test
    public void testBlockingStreamWithParallelism_future() {
        Stream<SettableFuture<String>> completedFutureStream = MoreStreams.blockingStreamWithParallelism(stream, 3);
        assertThat(completedFutureStream).containsExactly(firstInSource, secondInSource);
        assertThat(streamClosed).isTrue();
    }

    @Test
    public void testInCompletionOrder_transformWithExecutor() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        // 2 cannot start until a task has finished, 0 cannot start until 2 is running, so 1 must come first.
        try (Stream<Integer> integerStream = MoreStreams.inCompletionOrder(
                IntStream.range(0, 3).boxed().onClose(() -> streamClosed.set(true)), reorder(), executorService, 2)) {
            assertThat(integerStream.collect(toList())).startsWith(1).containsExactlyInAnyOrder(0, 1, 2);
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.SECONDS);
        }
        assertThat(streamClosed).isTrue();
    }

    @Test
    public void testInCompletionOrder_transformWithFutureSupplier() throws InterruptedException {
        ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(3));
        UnaryOperator<Integer> reorder = reorder();
        Function<Integer, ListenableFuture<Integer>> futureSupplier =
                i -> executorService.submit(() -> reorder.apply(i));

        // 2 cannot start until a task has finished, 0 cannot start until 2 is running, so 1 must come first.
        try (Stream<Integer> integerStream = MoreStreams.inCompletionOrder(
                IntStream.range(0, 3).boxed().onClose(() -> streamClosed.set(true)), futureSupplier, 2)) {
            assertThat(integerStream.collect(toList())).startsWith(1).containsExactlyInAnyOrder(0, 1, 2);
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.SECONDS);
        }
        assertThat(streamClosed).isTrue();
    }

    @Test
    public void testBlockingStreamWithParallelism_transformWithExecutor() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        // due to size of thread pool, 1 must finish before 0, but 0 will return first.
        try (Stream<Integer> integerStream = MoreStreams.blockingStreamWithParallelism(
                IntStream.range(0, 3).boxed().onClose(() -> streamClosed.set(true)), reorder(), executorService, 3)) {
            assertThat(integerStream.collect(toList())).containsExactly(0, 1, 2);
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.SECONDS);
        }
        assertThat(streamClosed).isTrue();
    }

    @Test
    public void testBlockingStreamWithParallelism_transformWithFutureSupplier() throws InterruptedException {
        ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2));
        UnaryOperator<Integer> reorder = reorder();
        Function<Integer, ListenableFuture<Integer>> futureSupplier =
                i -> executorService.submit(() -> reorder.apply(i));
        // due to size of thread pool, 1 must finish before 0, but 0 will return first.
        try (Stream<Integer> integerStream = MoreStreams.blockingStreamWithParallelism(
                IntStream.range(0, 3).boxed().onClose(() -> streamClosed.set(true)), futureSupplier, 3)) {
            assertThat(integerStream.collect(toList())).containsExactly(0, 1, 2);
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.SECONDS);
        }
        assertThat(streamClosed).isTrue();
    }

    @Test
    public void testConcurrencySimpleStream() throws InterruptedException {
        testConcurrency(IntStream.range(0, 3).boxed());
    }

    @Test
    public void testConcurrencyWithFlatmap() throws InterruptedException {
        testConcurrency(Stream.of(1).flatMap(_ignored -> IntStream.range(0, 3).boxed()));
    }

    private void testConcurrency(Stream<Integer> value) throws InterruptedException {
        AtomicInteger maximum = new AtomicInteger(-1);
        AtomicInteger current = new AtomicInteger();
        int maxParallelism = 1;
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        try (Stream<Integer> inCompletionOrder = MoreStreams.inCompletionOrder(
                value.onClose(() -> streamClosed.set(true)),
                input -> {
                    int running = current.incrementAndGet();
                    maximum.accumulateAndGet(running, Math::max);
                    try {
                        Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(200));
                    } finally {
                        current.decrementAndGet();
                    }
                    return input;
                },
                executorService,
                maxParallelism)) {
            assertThat(inCompletionOrder.collect(toList())).containsExactlyInAnyOrder(0, 1, 2);
            assertThat(maximum).hasValue(maxParallelism);
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.SECONDS);
        }
        assertThat(streamClosed).isTrue();
    }

    @Test
    public void testConcurrencySimpleStreamAndFutureSupplier() throws InterruptedException {
        testConcurrencyWithFutureSupplier(IntStream.range(0, 3).boxed());
    }

    @Test
    public void testConcurrencyWithFlatmapAndFutureSupplie() throws InterruptedException {
        testConcurrencyWithFutureSupplier(
                Stream.of(1).flatMap(_ignored -> IntStream.range(0, 3).boxed()));
    }

    private void testConcurrencyWithFutureSupplier(Stream<Integer> value) throws InterruptedException {
        AtomicInteger maximum = new AtomicInteger(-1);
        AtomicInteger current = new AtomicInteger();
        int maxParallelism = 1;
        ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(3));
        Function<Integer, ListenableFuture<Integer>> toFuture = input -> executorService.submit(() -> {
            int running = current.incrementAndGet();
            maximum.accumulateAndGet(running, Math::max);
            try {
                Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(200));
            } finally {
                current.decrementAndGet();
            }
            return input;
        });
        try (Stream<Integer> inCompletionOrder =
                MoreStreams.inCompletionOrder(value.onClose(() -> streamClosed.set(true)), toFuture, maxParallelism)) {
            assertThat(inCompletionOrder.collect(toList())).containsExactlyInAnyOrder(0, 1, 2);
            assertThat(maximum).hasValue(maxParallelism);
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.SECONDS);
        }
        assertThat(streamClosed).isTrue();
    }

    private UnaryOperator<Integer> reorder() {
        CyclicBarrier barrier = new CyclicBarrier(2);
        return input -> {
            if (input == 0 || input == 2) {
                await(barrier);
            }
            return input;
        };
    }

    private static void await(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (BrokenBarrierException e) {
            throw new RuntimeException(e);
        }
    }
}
