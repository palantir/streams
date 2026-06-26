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
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Starts more work as soon as any in-flight future completes, while returning results in source order.
 */
class EagerBufferingSpliterator<T, U> implements Spliterator<T> {
    private final Spliterator<U> source;
    private final Function<U, T> mapper;
    private final int maxParallelism;
    private final CompletionService<T> completionService;
    private final IdentityHashMap<Future<T>, Long> indexesByFuture = new IdentityHashMap<>();
    private final Map<Long, Future<T>> completedResults = new HashMap<>();

    private long nextIndexToStart = 0;
    private long nextIndexToReturn = 0;
    private int inFlight = 0;
    private boolean sourceExhausted = false;

    EagerBufferingSpliterator(Spliterator<U> source, Function<U, T> mapper, Executor executor, int maxParallelism) {
        checkArgument(maxParallelism > 0, "maxParallelism must be at least 1 (got %s)", maxParallelism);
        this.source = source;
        this.mapper = mapper;
        this.maxParallelism = maxParallelism;
        this.completionService = new ExecutorCompletionService<>(executor);
    }

    @Override
    public boolean tryAdvance(Consumer<? super T> action) {
        startWorkEagerly();

        while (true) {
            Future<T> result = completedResults.remove(nextIndexToReturn);
            if (result != null) {
                nextIndexToReturn++;
                action.accept(Futures.getUnchecked(result));
                return true;
            }

            Future<T> completed = completionService.poll();
            if (completed != null) {
                recordCompleted(completed);
                startWorkEagerly();
                continue;
            }

            if (inFlight == 0) {
                return false;
            }

            recordCompleted(takeCompleted());
            startWorkEagerly();
        }
    }

    private void startWorkEagerly() {
        while (!sourceExhausted && inFlight < maxParallelism) {
            ValueHolder<U> holder = new ValueHolder<>();
            boolean hasMore = source.tryAdvance(input -> holder.value = input);
            if (!hasMore) {
                sourceExhausted = true;
                return;
            }

            long index = nextIndexToStart++;
            Future<T> future = completionService.submit(() -> mapper.apply(holder.value));
            indexesByFuture.put(future, index);
            inFlight++;
        }
    }

    private Future<T> takeCompleted() {
        try {
            return completionService.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void recordCompleted(Future<T> future) {
        Long index = indexesByFuture.remove(future);
        if (index == null) {
            throw new SafeIllegalStateException("Received unknown future from completion service");
        }
        inFlight--;
        completedResults.put(index, future);
    }

    @Override
    public Spliterator<T> trySplit() {
        return null;
    }

    @Override
    public long estimateSize() {
        long estimate = inFlight + completedResults.size() + source.estimateSize();
        if (estimate < 0L) {
            return Long.MAX_VALUE;
        }
        return estimate;
    }

    @Override
    public int characteristics() {
        return source.hasCharacteristics(Spliterator.ORDERED) ? Spliterator.ORDERED : 0;
    }

    private static final class ValueHolder<U> {
        private U value;
    }
}
