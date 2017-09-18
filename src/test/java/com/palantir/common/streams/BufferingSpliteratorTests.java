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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.common.streams.BufferingSpliterator.CompletionStrategy;
import com.palantir.common.streams.BufferingSpliterator.InCompletionOrder;
import com.palantir.common.streams.BufferingSpliterator.InSourceOrder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(Parameterized.class)
public final class BufferingSpliteratorTests {
    private final SettableFuture<String> future = SettableFuture.create();
    private final SettableFuture<String> otherFuture = SettableFuture.create();

    @Mock private Consumer<ListenableFuture<String>> consumer;

    @Mock private Spliterator<ListenableFuture<String>> sourceSpliterator;

    private final CompletionStrategy completionStrategy;

    @Parameters(name = "strategy: {0}")
    public static Iterable<Object[]> parameters() {
        return ImmutableList.of(
                new Object[] {"InSourceOrder", InSourceOrder.INSTANCE},
                new Object[] {"InCompletionOrder", InCompletionOrder.INSTANCE});
    }

    public BufferingSpliteratorTests(String name, CompletionStrategy completionStrategy) {
        this.completionStrategy = completionStrategy;
    }

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
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
        Spliterator<ListenableFuture<String>> spliterator = new BufferingSpliterator<>(
                completionStrategy, Stream.<ListenableFuture<String>>empty().spliterator(), 1);
        assertThat(spliterator.tryAdvance(consumer)).isFalse();
        verifyZeroInteractions(consumer);
    }

    @Test
    public void onlyRunsUpToDesiredConcurrencyTasksSimultaneously() {
        Spliterator<ListenableFuture<String>> spliterator =
                new BufferingSpliterator<>(completionStrategy, sourceSpliterator, 1);

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
        new BufferingSpliterator<>(completionStrategy, sourceSpliterator, 2).tryAdvance(consumer);

        verify(sourceSpliterator, times(2)).tryAdvance(any());
    }

    @Test
    public void doesNotStartNextTaskUntilDoneWithLastValue() {
        future.set("some string");
        Spliterator<ListenableFuture<String>> spliterator =
                new BufferingSpliterator<>(completionStrategy, sourceSpliterator, 1);

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
                new BufferingSpliterator<>(completionStrategy, Stream.of(someFuture).spliterator(), 1);

        assertThat(spliterator.tryAdvance(consumer)).isTrue();
        verify(consumer).accept(argThat(new FutureContains<>(data)));
        assertThat(spliterator.tryAdvance(consumer)).isFalse();
    }

    @Test
    public void testEstimateSize_hasSize() {
        Spliterator<ListenableFuture<String>> futures =
                Stream.<ListenableFuture<String>>of(future, otherFuture).spliterator();
        Spliterator<ListenableFuture<String>> spliterator =
                new BufferingSpliterator<>(completionStrategy, futures, 1);
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
        Spliterator<ListenableFuture<String>> spliterator =
                new BufferingSpliterator<>(completionStrategy, sourceSpliterator, 1);
        future.set("data");
        spliterator.tryAdvance(consumer);
        assertThat(spliterator.estimateSize()).isEqualTo(Long.MAX_VALUE);
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
