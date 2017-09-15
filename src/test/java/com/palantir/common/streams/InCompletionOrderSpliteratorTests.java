package com.palantir.common.streams;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class InCompletionOrderSpliteratorTests {
    private final SettableFuture<String> future = SettableFuture.create();
    private final SettableFuture<String> otherFuture = SettableFuture.create();

    @Mock private Consumer<ListenableFuture<String>> consumer;

    @Mock private Spliterator<ListenableFuture<String>> sourceSpliterator;

    @Before
    public void before() {
        when(sourceSpliterator.tryAdvance(any())).thenAnswer(x -> {
            Consumer<ListenableFuture<String>> consumer = x.getArgument(0);
            consumer.accept(future);
            return true;
        }).thenAnswer(x -> {
            Consumer<ListenableFuture<String>> consumer = x.getArgument(0);
            consumer.accept(otherFuture);
            return true;
        }).thenReturn(false);
    }

    @Test
    public void returnsFalseWhenAllFuturesCompleted() {
        Spliterator<ListenableFuture<String>> spliterator =
                new InCompletionOrderSpliterator<>(Stream.<ListenableFuture<String>>empty().spliterator(), 1);
        assertThat(spliterator.tryAdvance(consumer)).isFalse();
        verifyZeroInteractions(consumer);
    }

    @Test
    public void onlyRunsUpToDesiredConcurrencyTasksSimultaneously() {
        Spliterator<ListenableFuture<String>> spliterator = new InCompletionOrderSpliterator<>(sourceSpliterator, 1);

        String firstData = "firstData";
        future.set(firstData);
        assertThat(spliterator.tryAdvance(consumer)).isTrue();
        verify(sourceSpliterator, times(1)).tryAdvance(any());
        verify(consumer).accept(argThat(new FutureContains<>(firstData)));

        String secondData = "secondData";
        otherFuture.set(secondData);
        assertThat(spliterator.tryAdvance(consumer)).isTrue();
        verify(consumer).accept(argThat(new FutureContains<>(secondData)));

        assertThat(spliterator.tryAdvance(consumer)).isFalse();
        verifyNoMoreInteractions(consumer);
    }

    @Test
    public void runsDesiredConcurrencyTasksSimultaneously() {
        future.set("some string");
        new InCompletionOrderSpliterator<>(sourceSpliterator, 2).tryAdvance(consumer);

        verify(sourceSpliterator, times(2)).tryAdvance(any());
    }

    @Test
    public void doesNotStartNextTaskUntilDoneWithLastValue() {
        future.set("some string");
        Spliterator<ListenableFuture<String>> spliterator = new InCompletionOrderSpliterator<>(sourceSpliterator, 1);

        future.set("data");
        spliterator.tryAdvance(consumer);

        verify(sourceSpliterator, times(1)).tryAdvance(any());
    }

    // This test exists because of an implementation bug while writing this.
    @Test
    public void canHandleFutureAlreadyCompleted() {
        String data = "data";
        ListenableFuture<String> someFuture = Futures.immediateFuture(data);

        Spliterator<ListenableFuture<String>> spliterator =
                new InCompletionOrderSpliterator<>(Stream.of(someFuture).spliterator(), 1);

        assertThat(spliterator.tryAdvance(consumer)).isTrue();
        verify(consumer).accept(argThat(new FutureContains<>(data)));
        assertThat(spliterator.tryAdvance(consumer)).isFalse();
    }

    @Test
    public void testEstimateSize_hasSize() {
        Spliterator<ListenableFuture<String>> futures =
                Stream.<ListenableFuture<String>>of(future, otherFuture).spliterator();
        Spliterator<ListenableFuture<String>> spliterator = new InCompletionOrderSpliterator<>(futures, 1);
        assertThat(spliterator.estimateSize()).isEqualTo(2);
        future.set("data");
        spliterator.tryAdvance(consumer);
        assertThat(spliterator.estimateSize()).isEqualTo(1);
        otherFuture.set("data");
        spliterator.tryAdvance(consumer);
        assertThat(spliterator.estimateSize()).isEqualTo(0);
    }

    @Test
    public void testEstimateSize_unsized() {
        when(sourceSpliterator.estimateSize()).thenReturn(Long.MAX_VALUE);
        assertThat(new InCompletionOrderSpliterator<>(sourceSpliterator, 2).estimateSize())
                .isEqualTo(Long.MAX_VALUE);
    }

    private static class FutureContains<V> implements ArgumentMatcher<ListenableFuture<V>> {
        private final V value;

        private FutureContains(V value) {
            this.value = value;
        }

        @Override
        public boolean matches(ListenableFuture<V> argument) {
            return Futures.getUnchecked(argument).equals(value);
        }
    }
}
