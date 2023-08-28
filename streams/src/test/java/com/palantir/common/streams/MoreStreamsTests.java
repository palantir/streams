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

import com.google.common.util.concurrent.Uninterruptibles;
import java.time.Duration;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

public class MoreStreamsTests {
    @Test
    public void testInCompletionOrder_transformWithExecutor() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        try {
            // 2 cannot start until a task has finished, 0 cannot start until 2 is running, so 1 must come first.
            assertThat(MoreStreams.inCompletionOrder(IntStream.range(0, 3).boxed(), reorder(), executorService, 2)
                            .collect(toList()))
                    .startsWith(1)
                    .containsExactlyInAnyOrder(0, 1, 2);
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testBlockingStreamWithParallelism_transformWithExecutor() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        try {
            // due to size of thread pool, 1 must finish before 0, but 0 will return first.
            assertThat(MoreStreams.blockingStreamWithParallelism(
                                    IntStream.range(0, 3).boxed(), reorder(), executorService, 3)
                            .collect(toList()))
                    .containsExactly(0, 1, 2);
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.SECONDS);
        }
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
        try {
            Stream<Integer> inCompletionOrder = MoreStreams.inCompletionOrder(
                    value,
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
                    maxParallelism);
            assertThat(inCompletionOrder.collect(toList())).containsExactlyInAnyOrder(0, 1, 2);
            assertThat(maximum).hasValue(maxParallelism);
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.SECONDS);
        }
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
