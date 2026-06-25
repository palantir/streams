/*
 * (c) Copyright 2026 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EagerBufferingSpliteratorTests {

    private ExecutorService executorService;
    private final AtomicBoolean streamClosed = new AtomicBoolean(false);

    @BeforeEach
    public void before() {
        executorService = Executors.newFixedThreadPool(10);
        streamClosed.set(false);
    }

    @AfterEach
    public void after() throws InterruptedException {
        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    public void testResultsReturnedInSourceOrder() {
        List<Integer> result;
        try (Stream<Integer> stream = MoreStreams.eagerBlockingStreamWithParallelism(
                IntStream.range(0, 10).boxed().onClose(() -> streamClosed.set(true)), x -> x, executorService, 3)) {
            result = stream.collect(toList());
        }

        assertThat(result).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertThat(streamClosed).isTrue();
    }

    @Test
    public void testEmptyStream() {
        List<Integer> result;
        try (Stream<Integer> stream = MoreStreams.eagerBlockingStreamWithParallelism(
                Stream.<Integer>empty().onClose(() -> streamClosed.set(true)), x -> x, executorService, 3)) {
            result = stream.collect(toList());
        }

        assertThat(result).isEmpty();
        assertThat(streamClosed).isTrue();
    }

    @Test
    public void testSingleElement() {
        List<Integer> result;
        try (Stream<Integer> stream = MoreStreams.eagerBlockingStreamWithParallelism(
                Stream.of(42).onClose(() -> streamClosed.set(true)), x -> x * 2, executorService, 3)) {
            result = stream.collect(toList());
        }

        assertThat(result).containsExactly(84);
        assertThat(streamClosed).isTrue();
    }

    @Test
    public void testMaxParallelismOne() {
        List<Integer> result;
        try (Stream<Integer> stream = MoreStreams.eagerBlockingStreamWithParallelism(
                IntStream.range(0, 5).boxed().onClose(() -> streamClosed.set(true)), x -> x, executorService, 1)) {
            result = stream.collect(toList());
        }

        assertThat(result).containsExactly(0, 1, 2, 3, 4);
        assertThat(streamClosed).isTrue();
    }

    @Test
    public void testImmediateFutures() {
        List<Integer> result;
        try (Stream<Integer> stream = MoreStreams.eagerBlockingStreamWithParallelism(
                IntStream.range(0, 10_000).boxed().onClose(() -> streamClosed.set(true)),
                x -> x,
                MoreExecutors.directExecutor(),
                3)) {
            result = stream.collect(toList());
        }

        assertThat(result)
                .containsExactlyElementsOf(IntStream.range(0, 10_000).boxed().collect(toList()));
        assertThat(streamClosed).isTrue();
    }

    @Test
    public void testMaintainsParallelismWhenFirstElementIsSlow() throws InterruptedException {
        AtomicInteger maxConcurrent = new AtomicInteger(0);
        AtomicInteger currentConcurrent = new AtomicInteger(0);
        CountDownLatch firstElementStarted = new CountDownLatch(1);

        List<Integer> result;
        try (Stream<Integer> stream = MoreStreams.eagerBlockingStreamWithParallelism(
                IntStream.range(0, 6).boxed().onClose(() -> streamClosed.set(true)),
                x -> {
                    int concurrent = currentConcurrent.incrementAndGet();
                    maxConcurrent.accumulateAndGet(concurrent, Math::max);

                    try {
                        if (x == 0) {
                            firstElementStarted.countDown();
                            Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(200));
                        } else {
                            Uninterruptibles.awaitUninterruptibly(firstElementStarted);
                            Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(50));
                        }
                    } finally {
                        currentConcurrent.decrementAndGet();
                    }
                    return x;
                },
                executorService,
                3)) {
            result = stream.collect(toList());
        }

        assertThat(result).containsExactly(0, 1, 2, 3, 4, 5);
        assertThat(maxConcurrent.get()).isEqualTo(3);
        assertThat(streamClosed).isTrue();
    }

    @Test
    public void testEagerSchedulingStartsNewWorkOnCompletion() throws InterruptedException {
        AtomicInteger elementsStarted = new AtomicInteger(0);
        CountDownLatch element0Started = new CountDownLatch(1);
        CountDownLatch element3Started = new CountDownLatch(1);

        List<Integer> result;
        try (Stream<Integer> stream = MoreStreams.eagerBlockingStreamWithParallelism(
                IntStream.range(0, 5).boxed().onClose(() -> streamClosed.set(true)),
                x -> {
                    int started = elementsStarted.incrementAndGet();
                    if (x == 0) {
                        element0Started.countDown();
                        boolean element3DidStart =
                                Uninterruptibles.awaitUninterruptibly(element3Started, 2, TimeUnit.SECONDS);
                        assertThat(element3DidStart)
                                .as("element 3 starts before element 0 completes")
                                .isTrue();
                    } else if (x == 3) {
                        element3Started.countDown();
                    } else {
                        Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(10));
                    }
                    return x;
                },
                executorService,
                3)) {
            result = stream.collect(toList());
        }

        assertThat(result).containsExactly(0, 1, 2, 3, 4);
        assertThat(streamClosed).isTrue();
    }

    @Test
    public void testConcurrencyRespected() throws InterruptedException {
        AtomicInteger maxConcurrent = new AtomicInteger(0);
        AtomicInteger currentConcurrent = new AtomicInteger(0);
        int maxParallelism = 2;

        List<Integer> result;
        try (Stream<Integer> stream = MoreStreams.eagerBlockingStreamWithParallelism(
                IntStream.range(0, 10).boxed().onClose(() -> streamClosed.set(true)),
                x -> {
                    int concurrent = currentConcurrent.incrementAndGet();
                    maxConcurrent.accumulateAndGet(concurrent, Math::max);
                    try {
                        Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(50));
                    } finally {
                        currentConcurrent.decrementAndGet();
                    }
                    return x;
                },
                executorService,
                maxParallelism)) {
            result = stream.collect(toList());
        }

        assertThat(result).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertThat(maxConcurrent.get()).isLessThanOrEqualTo(maxParallelism);
        assertThat(streamClosed).isTrue();
    }

    @Test
    public void testOriginalBlockingStreamHasHeadOfLineBlocking() throws InterruptedException {
        AtomicInteger elementsStarted = new AtomicInteger(0);
        CountDownLatch element0Started = new CountDownLatch(1);
        CountDownLatch element3Started = new CountDownLatch(1);

        List<Integer> result;
        try (Stream<Integer> stream = MoreStreams.blockingStreamWithParallelism(
                IntStream.range(0, 5).boxed().onClose(() -> streamClosed.set(true)),
                x -> {
                    elementsStarted.incrementAndGet();
                    if (x == 0) {
                        element0Started.countDown();
                        boolean element3DidStart =
                                Uninterruptibles.awaitUninterruptibly(element3Started, 500, TimeUnit.MILLISECONDS);
                        assertThat(element3DidStart)
                                .as("element 3 starts before element 0 completes")
                                .isFalse();
                    } else if (x == 3) {
                        element3Started.countDown();
                    } else {
                        Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(10));
                    }
                    return x;
                },
                executorService,
                3)) {
            result = stream.collect(toList());
        }

        assertThat(result).containsExactly(0, 1, 2, 3, 4);
        assertThat(streamClosed).isTrue();
    }

    @Test
    public void testWithFlatMap() throws InterruptedException {
        List<Integer> result;
        try (Stream<Integer> stream = MoreStreams.eagerBlockingStreamWithParallelism(
                Stream.of(1).flatMap(_ignored -> IntStream.range(0, 5).boxed()).onClose(() -> streamClosed.set(true)),
                x -> x,
                executorService,
                3)) {
            result = stream.collect(toList());
        }

        assertThat(result).containsExactly(0, 1, 2, 3, 4);
        assertThat(streamClosed).isTrue();
    }
}
