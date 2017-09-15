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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Spliterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

class BackpressureSpliterator<U, V> implements Spliterator<V> {
    private final int desiredConcurrency;
    private final BlockingQueue<CompletableFuture<V>> completed;
    private final Spliterator<U> notStarted;
    private final Function<U, CompletableFuture<V>> mapper;

    private int inProgress = 0;

    private BackpressureSpliterator(
            int desiredConcurrency,
            Spliterator<U> arguments,
            Function<U, CompletableFuture<V>> mapper) {
        checkArgument(desiredConcurrency > 0, "desiredConcurrency %s > 0", new Object[] { desiredConcurrency });
        this.desiredConcurrency = desiredConcurrency;
        this.completed = new ArrayBlockingQueue<>(desiredConcurrency);
        this.notStarted = arguments;
        this.mapper = mapper;
    }

    @Override
    public boolean tryAdvance(Consumer<? super V> action) {
        startNewWorkIfNecessary();
        if (inProgress == 0) {
            return false;
        }

        try {
            V element = completed.take().join();
            inProgress--;
            action.accept(element);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public Spliterator<V> trySplit() {
        return null;
    }

    private void startNewWorkIfNecessary() {
        while (inProgress < desiredConcurrency) {
            boolean maybeRemainingElements = notStarted.tryAdvance(nextInput -> {
                CompletableFuture<V> nextFuture = mapper.apply(nextInput);
                inProgress++;
                nextFuture.whenComplete((res, err) -> completed.add(nextFuture));
            });
            if (!maybeRemainingElements) {
                return;
            }
        }
    }

    @Override
    public long estimateSize() {
        long estimate = completed.size() + inProgress + notStarted.estimateSize();
        if (estimate < 0L) {
            return Long.MAX_VALUE;
        }
        return estimate;
    }

    @Override
    public long getExactSizeIfKnown() {
        long delegateResult = notStarted.getExactSizeIfKnown();
        if (delegateResult == -1L) {
            return -1L;
        }
        return completed.size() + inProgress + delegateResult;
    }

    @Override
    public int characteristics() {
        return (Spliterator.SIZED | Spliterator.SUBSIZED) & notStarted.characteristics();
    }

    static <U, V> Spliterator<V> create(
            int desiredConcurrency,
            Stream<U> arguments,
            Function<U, CompletableFuture<V>> mapper) {
        BackpressureSpliterator<U, V> result = new BackpressureSpliterator<>(
                desiredConcurrency, arguments.spliterator(), mapper);
        result.startNewWorkIfNecessary();
        return result;
    }
}
