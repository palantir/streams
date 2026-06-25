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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.HashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Starts more work as soon as any in-flight future completes, while returning results in source order.
 */
class EagerBufferingSpliterator<T, U> implements Spliterator<T> {
    private final Spliterator<U> source;
    private final Function<U, ListenableFuture<T>> toFuture;
    private final int maxParallelism;

    @GuardedBy("sourceLock")
    private long nextIndexToStart = 0;

    private long nextIndexToReturn = 0;

    @GuardedBy("completedResultsLock")
    private final Map<Long, ListenableFuture<T>> completedResults = new HashMap<>();

    private final Object completedResultsLock = new Object();
    private final Object sourceLock = new Object();
    private final AtomicInteger inFlight = new AtomicInteger(0);
    private volatile boolean sourceExhausted = false;

    EagerBufferingSpliterator(Spliterator<U> source, Function<U, ListenableFuture<T>> toFuture, int maxParallelism) {
        checkArgument(maxParallelism > 0, "maxParallelism must be at least 1 (got %s)", maxParallelism);
        this.source = source;
        this.toFuture = toFuture;
        this.maxParallelism = maxParallelism;
    }

    @Override
    public boolean tryAdvance(Consumer<? super T> action) {
        startWorkEagerly();

        ListenableFuture<T> result;
        while (true) {
            synchronized (completedResultsLock) {
                result = completedResults.remove(nextIndexToReturn);
                if (result != null) {
                    nextIndexToReturn++;
                    break;
                }
                if (sourceExhausted && inFlight.get() == 0) {
                    return false;
                }
                try {
                    completedResultsLock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            startWorkEagerly();
        }

        action.accept(Futures.getUnchecked(result));
        return true;
    }

    private void startWorkEagerly() {
        int toStart = maxParallelism - inFlight.get();
        for (int i = 0; i < toStart; i++) {
            Work<U> work = reserveWork();
            if (work == null) {
                return;
            }

            ListenableFuture<T> future = toFuture.apply(work.input);
            future.addListener(
                    () -> {
                        synchronized (completedResultsLock) {
                            completedResults.put(work.index, future);
                            inFlight.decrementAndGet();
                            completedResultsLock.notifyAll();
                        }
                    },
                    MoreExecutors.directExecutor());
        }
    }

    private Work<U> reserveWork() {
        synchronized (sourceLock) {
            if (sourceExhausted || inFlight.get() >= maxParallelism) {
                return null;
            }

            ValueHolder<U> holder = new ValueHolder<>();
            boolean hasMore = source.tryAdvance(input -> holder.value = input);
            if (!hasMore) {
                sourceExhausted = true;
                return null;
            }

            inFlight.incrementAndGet();
            return new Work<>(nextIndexToStart++, holder.value);
        }
    }

    @Override
    public Spliterator<T> trySplit() {
        return null;
    }

    @Override
    public long estimateSize() {
        long completedCount;
        synchronized (completedResultsLock) {
            completedCount = completedResults.size();
        }
        long estimate = inFlight.get() + completedCount + source.estimateSize();
        if (estimate < 0L) {
            return Long.MAX_VALUE;
        }
        return estimate;
    }

    @Override
    public int characteristics() {
        return Spliterator.ORDERED & source.characteristics();
    }

    private static final class Work<U> {
        private final long index;
        private final U input;

        private Work(long index, U input) {
            this.index = index;
            this.input = input;
        }
    }

    private static final class ValueHolder<U> {
        private U value;
    }
}
