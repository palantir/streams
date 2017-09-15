package com.palantir.common.streams;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class BackpressureSpliteratorTests {
    private final CompletableFuture<String> future = new CompletableFuture<>();
    private final CompletableFuture<String> otherFuture = new CompletableFuture<>();

    @Mock private Consumer<String> consumer;

    @Mock private Spliterator<CompletableFuture<String>> sourceSpliterator;

    @Before
    public void before() {
        when(sourceSpliterator.tryAdvance(any())).thenAnswer(x -> {
            Consumer<CompletableFuture<String>> consumer = x.getArgument(0);
            consumer.accept(future);
            return true;
        }).thenAnswer(x -> {
            Consumer<CompletableFuture<String>> consumer = x.getArgument(0);
            consumer.accept(otherFuture);
            return true;
        }).thenReturn(false);
    }

    @Test
    public void returnsFalseWhenAllFuturesCompleted() {
        Spliterator<String> spliterator = BackpressureSpliterator.create(
                1,
                Stream.<CompletableFuture<String>>empty().spliterator());
        assertThat(spliterator.tryAdvance(consumer)).isFalse();
        verifyZeroInteractions(consumer);
    }

    @Test
    public void throwsIfSuppliedFutureThrows() {
        CompletableFuture<String> someFuture = new CompletableFuture<>();
        Spliterator<String> spliterator =
                BackpressureSpliterator.create(1, Stream.of(someFuture).spliterator());
        someFuture.completeExceptionally(new RuntimeException());
        assertThatExceptionOfType(CompletionException.class).isThrownBy(() -> spliterator.tryAdvance(consumer));
    }

    @Test
    public void onlyRunsUpToDesiredConcurrencyTasksSimultaneously() {
        Spliterator<String> spliterator = BackpressureSpliterator.create(1, sourceSpliterator);

        verify(sourceSpliterator, times(1)).tryAdvance(any());
        String firstData = "firstData";
        future.complete(firstData);
        assertThat(spliterator.tryAdvance(consumer)).isTrue();
        verify(consumer).accept(firstData);

        String secondData = "secondData";
        otherFuture.complete(secondData);
        assertThat(spliterator.tryAdvance(consumer)).isTrue();
        verify(consumer).accept(secondData);

        assertThat(spliterator.tryAdvance(consumer)).isFalse();
        verifyNoMoreInteractions(consumer);
    }

    @Test
    public void runsDesiredConcurrencyTasksSimultaneously() {
        BackpressureSpliterator.create(
                2,
                sourceSpliterator);

        verify(sourceSpliterator, times(2)).tryAdvance(any());
    }

    @Test
    public void startNextTaskBeforeConsumingValue() {
        Spliterator<String> spliterator = BackpressureSpliterator.create(
                1,
                sourceSpliterator);

        String data = "data";
        future.complete(data);
        otherFuture.complete("some other data");

        // check that we kicked off next task before the consumer has been called
        doAnswer(inv -> {
            verify(sourceSpliterator, times(2)).tryAdvance(any());
            return null;
        }).when(consumer).accept(data);

        spliterator.tryAdvance(consumer);
    }

    // This test exists because of an implementation bug while writing this.
    @Test
    public void canHandleFutureAlreadyCompleted() {
        CompletableFuture<String> someFuture = new CompletableFuture<>();
        String data = "data";
        someFuture.complete(data);

        Spliterator<String> spliterator = BackpressureSpliterator.create(1, Stream.of(someFuture).spliterator());

        assertThat(spliterator.tryAdvance(consumer)).isTrue();
        verify(consumer).accept(data);
        assertThat(spliterator.tryAdvance(consumer)).isFalse();
    }

    @Test
    public void testEstimateSize_hasSize() {
        long estimate = 5L;
        when(sourceSpliterator.estimateSize()).thenReturn(estimate);
        assertThat(BackpressureSpliterator.create(2, sourceSpliterator).estimateSize()).isEqualTo(estimate + 2);
    }

    @Test
    public void testEstimateSize_unsized() {
        when(sourceSpliterator.estimateSize()).thenReturn(Long.MAX_VALUE);
        assertThat(BackpressureSpliterator.create(2, sourceSpliterator).estimateSize())
                .isEqualTo(Long.MAX_VALUE);
    }
}
