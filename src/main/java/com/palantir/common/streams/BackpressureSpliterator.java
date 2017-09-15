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

class BackpressureSpliterator<U> implements Spliterator<U> {
    private final int maxParallelism;
    private final BlockingQueue<CompletableFuture<U>> completed;
    private final Spliterator<CompletableFuture<U>> notStarted;

    private int inProgress = 0;

    private BackpressureSpliterator(
            int maxParallelism,
            Spliterator<CompletableFuture<U>> arguments) {
        checkArgument(maxParallelism > 0,
                "maxParallelism must be at least 1 (got %s)", new Object[] {maxParallelism});
        this.maxParallelism = maxParallelism;
        this.completed = new ArrayBlockingQueue<>(maxParallelism);
        this.notStarted = arguments;
    }

    @Override
    public boolean tryAdvance(Consumer<? super U> action) {
        if (inProgress == 0) {
            return false;
        }

        try {
            U element = completed.take().join();
            inProgress--;
            startNewWorkIfNecessary();
            action.accept(element);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public Spliterator<U> trySplit() {
        return null;
    }

    private void startNewWorkIfNecessary() {
        while (inProgress < maxParallelism) {
            boolean maybeRemainingElements = notStarted.tryAdvance(nextInput -> {
                inProgress++;
                nextInput.whenComplete((res, err) -> completed.add(nextInput));
            });
            if (!maybeRemainingElements) {
                return;
            }
        }
    }

    @Override
    public long estimateSize() {
        long estimate = inProgress + notStarted.estimateSize();
        if (estimate < 0L) {
            return Long.MAX_VALUE;
        }
        return estimate;
    }

    @Override
    public int characteristics() {
        return (Spliterator.SIZED | Spliterator.SUBSIZED) & notStarted.characteristics();
    }

    static <U> Spliterator<U> create(int desiredParallelism, Spliterator<CompletableFuture<U>> arguments) {
        BackpressureSpliterator<U> result = new BackpressureSpliterator<>(desiredParallelism, arguments);
        result.startNewWorkIfNecessary();
        return result;
    }
}
