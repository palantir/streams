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

import com.google.common.util.concurrent.Futures;
import org.junit.Test;

import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class MoreFuturesTests {
    private static final String RESULT = "result";
    private static final RuntimeException exception = new RuntimeException();

    @Test
    public void testBlockOnCompletion_successful() {
        Future<String> future = Futures.immediateFailedFuture(new RuntimeException());
        MoreFutures.blockOnCompletion(future);
        assertThatExceptionOfType(ExecutionException.class).isThrownBy(future::get).withCause(exception);
    }

    @Test
    public void testBlockOnCompletion_failure() {
        Future<String> future = Futures.immediateFuture(RESULT);
        MoreFutures.blockOnCompletion(future);
        assertThat(Futures.getUnchecked(future)).isEqualTo(RESULT);
    }

    @Test
    public void testBlockOnCompletion_cancelled() {
        Future<String> future = Futures.immediateCancelledFuture();
        MoreFutures.blockOnCompletion(future);
        assertThat(future.isCancelled()).isTrue();
    }

    @Test
    public void testBlockOnCompletion_blocking() throws InterruptedException, ExecutionException {
        CompletableFuture<String> future = new CompletableFuture<>();

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            CyclicBarrier barrier = new CyclicBarrier(2);
            Future<?> success = executorService.submit(() -> {
                assertThat(future.isDone()).isFalse();
                await(barrier);
                MoreFutures.blockOnCompletion(future);
                assertThat(future.isDone()).isTrue();
            });

            await(barrier);
            future.complete(RESULT);

            success.get();
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.SECONDS);
        }
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
