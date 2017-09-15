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
        Spliterator<String> spliterator = new BackpressureSpliterator<>(Stream.<CompletableFuture<String>>empty().spliterator(), 1);
        assertThat(spliterator.tryAdvance(consumer)).isFalse();
        verifyZeroInteractions(consumer);
    }

    @Test
    public void throwsIfSuppliedFutureThrows() {
        CompletableFuture<String> someFuture = new CompletableFuture<>();
        Spliterator<String> spliterator =
                new BackpressureSpliterator<>(Stream.of(someFuture).spliterator(), 1);
        someFuture.completeExceptionally(new RuntimeException());
        assertThatExceptionOfType(CompletionException.class).isThrownBy(() -> spliterator.tryAdvance(consumer));
    }

    @Test
    public void onlyRunsUpToDesiredConcurrencyTasksSimultaneously() {
        Spliterator<String> spliterator = new BackpressureSpliterator<>(sourceSpliterator, 1);

        String firstData = "firstData";
        future.complete(firstData);
        assertThat(spliterator.tryAdvance(consumer)).isTrue();
        verify(sourceSpliterator, times(1)).tryAdvance(any());
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
        future.complete("some string");
        new BackpressureSpliterator<>(sourceSpliterator, 2).tryAdvance(consumer);

        verify(sourceSpliterator, times(2)).tryAdvance(any());
    }

    @Test
    public void doesNotStartNextTaskUntilDoneWithLastValue() {
        future.complete("some string");
        Spliterator<String> spliterator = new BackpressureSpliterator<>(sourceSpliterator, 1);

        future.complete("data");
        spliterator.tryAdvance(consumer);

        verify(sourceSpliterator, times(1)).tryAdvance(any());
    }

    // This test exists because of an implementation bug while writing this.
    @Test
    public void canHandleFutureAlreadyCompleted() {
        String data = "data";
        CompletableFuture<String> someFuture = CompletableFuture.completedFuture(data);

        Spliterator<String> spliterator = new BackpressureSpliterator<>(Stream.of(someFuture).spliterator(), 1);

        assertThat(spliterator.tryAdvance(consumer)).isTrue();
        verify(consumer).accept(data);
        assertThat(spliterator.tryAdvance(consumer)).isFalse();
    }

    @Test
    public void testEstimateSize_hasSize() {
        future.complete("some string");
        long estimate = 5L;
        when(sourceSpliterator.estimateSize()).thenReturn(estimate);
        Spliterator<String> spliterator = new BackpressureSpliterator<>(sourceSpliterator, 2);
        spliterator.tryAdvance(consumer);
        assertThat(spliterator.estimateSize()).isEqualTo(estimate + 1);
    }

    @Test
    public void testEstimateSize_unsized() {
        when(sourceSpliterator.estimateSize()).thenReturn(Long.MAX_VALUE);
        assertThat(new BackpressureSpliterator<>(sourceSpliterator, 2).estimateSize())
                .isEqualTo(Long.MAX_VALUE);
    }
}
