package com.palantir.common.streams;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import java.util.function.Function;
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
    @Mock private Function<CompletableFuture<String>, CompletableFuture<String>> identityFunction;

    @Before
    public void before() {
        when(identityFunction.apply(any())).thenAnswer(inv -> inv.getArgument(0));
    }

    @Test
    public void returnsFalseWhenAllFuturesCompleted() {
        Spliterator<String> spliterator = BackpressureSpliterator.create(
                1,
                Stream.<String>empty(),
                CompletableFuture::completedFuture);
        assertThat(spliterator.tryAdvance(consumer)).isFalse();
        verifyZeroInteractions(consumer);
    }

    @Test
    public void throwsIfSuppliedFutureThrows() {
        CompletableFuture<String> someFuture = new CompletableFuture<>();
        Spliterator<String> spliterator = BackpressureSpliterator.create(1, Stream.of(someFuture), identityFunction);
        someFuture.completeExceptionally(new RuntimeException());
        assertThatExceptionOfType(CompletionException.class).isThrownBy(() -> spliterator.tryAdvance(consumer));
    }

    @Test
    public void onlyRunsUpToDesiredConcurrencyTasksSimultaneously() {
        Spliterator<String> spliterator = BackpressureSpliterator.create(
                1,
                Stream.of(future, otherFuture),
                identityFunction);

        verify(identityFunction, never()).apply(otherFuture);
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
    public void doesNotStartNextTaskUntilDoneWithLastValue() {
        Spliterator<String> spliterator = BackpressureSpliterator.create(
                1,
                Stream.of(future, otherFuture),
                identityFunction);

        future.complete("data");
        spliterator.tryAdvance(consumer);

        verify(identityFunction, never()).apply(otherFuture);
    }

    @Test
    public void runsDesiredConcurrencyTasksSimultaneously() {
        BackpressureSpliterator.create(
                2,
                Stream.of(future, otherFuture),
                identityFunction);

        verify(identityFunction).apply(future);
        verify(identityFunction).apply(otherFuture);
    }

    // This test exists because of an implementation bug while writing this.
    @Test
    public void canHandleFutureAlreadyCompleted() {
        CompletableFuture<String> someFuture = new CompletableFuture<>();
        String data = "data";
        someFuture.complete(data);

        Spliterator<String> spliterator = BackpressureSpliterator.create(1, Stream.of(someFuture), identityFunction);

        assertThat(spliterator.tryAdvance(consumer)).isTrue();
        verify(consumer).accept(data);
        assertThat(spliterator.tryAdvance(consumer)).isFalse();
    }
}
