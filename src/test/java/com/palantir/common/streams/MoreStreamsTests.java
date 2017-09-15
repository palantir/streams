package com.palantir.common.streams;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Test;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class MoreStreamsTests {
    private static final String DATA = "data";
    private static final ListenableFuture<String> LISTENABLE_FUTURE = Futures.immediateFuture(DATA);

    @Test
    public void testInCompletionOrder_future() {
        assertThat(MoreStreams.inCompletionOrder(Stream.of(LISTENABLE_FUTURE), 1)
                .map(Futures::getUnchecked)).containsExactly(DATA);
    }

    @Test
    public void testInCompletionOrder_transformWithExecutor() {
        assertThat(MoreStreams.inCompletionOrder(Stream.of(DATA), x -> x, MoreExecutors.directExecutor(), 1)
                .map(Futures::getUnchecked)).containsExactly(DATA);
    }
}
