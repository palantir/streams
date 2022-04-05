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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.Spliterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;

class BufferingSpliterator<T, F extends ListenableFuture<T>, U> implements Spliterator<F> {
    private final int maxParallelism;
    private final BlockingQueue<F> completed;
    private final Spliterator<U> notStarted;
    private final Function<U, F> toFuture;
    private final CompletionStrategy completionStrategy;

    private int inProgress = 0;

    BufferingSpliterator(
            CompletionStrategy completionStrategy, Spliterator<U> input, Function<U, F> toFuture, int maxParallelism) {
        this.completionStrategy = completionStrategy;
        checkArgument(maxParallelism > 0, "maxParallelism must be at least 1 (got %s)", new Object[] {maxParallelism});
        this.maxParallelism = maxParallelism;
        this.completed = new ArrayBlockingQueue<>(maxParallelism);
        this.notStarted = input;
        this.toFuture = toFuture;
    }

    @Override
    public boolean tryAdvance(Consumer<? super F> action) {
        startNewWorkIfNecessary();
        if (inProgress == 0) {
            return false;
        }

        try {
            F element = completed.take();
            inProgress--;
            action.accept(element);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public Spliterator<F> trySplit() {
        return null;
    }

    private void startNewWorkIfNecessary() {
        while (inProgress < maxParallelism) {
            boolean maybeRemainingElements = notStarted.tryAdvance(nextInput -> {
                inProgress++;
                completionStrategy.registerCompletion(toFuture.apply(nextInput), completed::add);
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
        return Spliterator.SIZED & notStarted.characteristics();
    }

    @FunctionalInterface
    interface CompletionStrategy {
        <T, F extends ListenableFuture<T>> void registerCompletion(F future, Consumer<F> resultConsumer);
    }

    enum InCompletionOrder implements CompletionStrategy {
        INSTANCE;

        @Override
        public <T, F extends ListenableFuture<T>> void registerCompletion(F future, Consumer<F> resultConsumer) {
            future.addListener(() -> resultConsumer.accept(future), MoreExecutors.directExecutor());
        }
    }

    enum InSourceOrder implements CompletionStrategy {
        INSTANCE;

        @Override
        public <T, F extends ListenableFuture<T>> void registerCompletion(F future, Consumer<F> resultConsumer) {
            resultConsumer.accept(future);
        }
    }
}
