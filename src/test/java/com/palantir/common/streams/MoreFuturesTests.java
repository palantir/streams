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
        CountDownLatch latch = new CountDownLatch(2);
        CompletableFuture<String> future = new CompletableFuture<>();

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        try {
            Future<?> success = executorService.submit(() -> {
                assertThat(future.isDone()).isFalse();
                latch.countDown();
                MoreFutures.blockOnCompletion(future);
                assertThat(future.isDone()).isTrue();
            });

            latch.countDown();
            future.complete(RESULT);

            success.get();
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.SECONDS);
        }
    }
}
