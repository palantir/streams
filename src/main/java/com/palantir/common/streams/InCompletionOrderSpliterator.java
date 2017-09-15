/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Spliterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

class InCompletionOrderSpliterator<U> implements Spliterator<ListenableFuture<U>> {
    private final int maxParallelism;
    private final BlockingQueue<ListenableFuture<U>> completed;
    private final Spliterator<ListenableFuture<U>> notStarted;

    private int inProgress = 0;

    InCompletionOrderSpliterator(Spliterator<ListenableFuture<U>> futures, int maxParallelism) {
        checkArgument(maxParallelism > 0,
                "maxParallelism must be at least 1 (got %s)", new Object[] {maxParallelism});
        this.maxParallelism = maxParallelism;
        this.completed = new ArrayBlockingQueue<>(maxParallelism);
        this.notStarted = futures;
    }

    @Override
    public boolean tryAdvance(Consumer<? super ListenableFuture<U>> action) {
        startNewWorkIfNecessary();
        if (inProgress == 0) {
            return false;
        }

        try {
            ListenableFuture<U> element = completed.take();
            inProgress--;
            action.accept(element);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public Spliterator<ListenableFuture<U>> trySplit() {
        return null;
    }

    private void startNewWorkIfNecessary() {
        while (inProgress < maxParallelism) {
            boolean maybeRemainingElements = notStarted.tryAdvance(nextInput -> {
                inProgress++;
                nextInput.addListener(() -> completed.add(nextInput), MoreExecutors.directExecutor());
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
}
