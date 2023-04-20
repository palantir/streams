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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.common.streams.BufferingSpliterator.CompletionStrategy;
import com.palantir.common.streams.BufferingSpliterator.InCompletionOrder;
import com.palantir.common.streams.BufferingSpliterator.InSourceOrder;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mock.Strictness;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class BufferingSpliteratorTests {
    private final SettableFuture<String> future = SettableFuture.create();
    private final SettableFuture<String> otherFuture = SettableFuture.create();

    @Mock(strictness = Strictness.LENIENT)
    private Consumer<ListenableFuture<String>> consumer;

    @Mock(strictness = Strictness.LENIENT)
    private Spliterator<ListenableFuture<String>> sourceSpliterator;

    public static Stream<Arguments> parameters() {
        return Stream.of(Arguments.of(InSourceOrder.INSTANCE), Arguments.of(InCompletionOrder.INSTANCE));
    }

    @BeforeEach
    public void before() {
        when(sourceSpliterator.tryAdvance(any()))
                .thenAnswer(x -> {
                    Consumer<ListenableFuture<String>> con = x.getArgument(0);
                    con.accept(future);
                    return true;
                })
                .thenAnswer(x -> {
                    Consumer<ListenableFuture<String>> con = x.getArgument(0);
                    con.accept(otherFuture);
                    return true;
                })
                .thenReturn(false);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void returnsFalseWhenAllFuturesCompleted(CompletionStrategy completionStrategy) {
        Spliterator<ListenableFuture<String>> spliterator = new BufferingSpliterator<>(
                completionStrategy, Stream.<ListenableFuture<String>>empty().spliterator(), Function.identity(), 1);
        assertThat(spliterator.tryAdvance(consumer)).isFalse();
        verifyNoInteractions(consumer);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void onlyRunsUpToDesiredConcurrencyTasksSimultaneously(CompletionStrategy completionStrategy) {
        Spliterator<ListenableFuture<String>> spliterator =
                new BufferingSpliterator<>(completionStrategy, sourceSpliterator, Function.identity(), 1);

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

    @ParameterizedTest
    @MethodSource("parameters")
    public void runsDesiredConcurrencyTasksSimultaneously(CompletionStrategy completionStrategy) {
        future.set("some string");
        new BufferingSpliterator<>(completionStrategy, sourceSpliterator, Function.identity(), 2).tryAdvance(consumer);

        verify(sourceSpliterator, times(2)).tryAdvance(any());
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void doesNotStartNextTaskUntilDoneWithLastValue(CompletionStrategy completionStrategy) {
        future.set("some string");
        Spliterator<ListenableFuture<String>> spliterator =
                new BufferingSpliterator<>(completionStrategy, sourceSpliterator, Function.identity(), 1);

        future.set("data");
        spliterator.tryAdvance(consumer);

        verify(sourceSpliterator, times(1)).tryAdvance(any());
    }

    // This test exists because of an implementation bug while writing this.
    @ParameterizedTest
    @MethodSource("parameters")
    public void canHandleFutureAlreadyCompleted(CompletionStrategy completionStrategy) {
        String data = "data";
        ListenableFuture<String> someFuture = Futures.immediateFuture(data);

        Spliterator<ListenableFuture<String>> spliterator = new BufferingSpliterator<>(
                completionStrategy, Stream.of(someFuture).spliterator(), Function.identity(), 1);

        assertThat(spliterator.tryAdvance(consumer)).isTrue();
        verify(consumer).accept(argThat(new FutureContains<>(data)));
        assertThat(spliterator.tryAdvance(consumer)).isFalse();
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testEstimateSize_hasSize(CompletionStrategy completionStrategy) {
        Spliterator<ListenableFuture<String>> futures =
                Stream.<ListenableFuture<String>>of(future, otherFuture).spliterator();
        Spliterator<ListenableFuture<String>> spliterator =
                new BufferingSpliterator<>(completionStrategy, futures, Function.identity(), 1);
        assertThat(spliterator.estimateSize()).isEqualTo(2);
        future.set("data");
        spliterator.tryAdvance(consumer);
        assertThat(spliterator.estimateSize()).isEqualTo(1);
        otherFuture.set("data");
        spliterator.tryAdvance(consumer);
        assertThat(spliterator.estimateSize()).isEqualTo(0);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    public void testEstimateSize_unsized(CompletionStrategy completionStrategy) {
        when(sourceSpliterator.estimateSize()).thenReturn(Long.MAX_VALUE);
        Spliterator<ListenableFuture<String>> spliterator =
                new BufferingSpliterator<>(completionStrategy, sourceSpliterator, Function.identity(), 1);
        future.set("data");
        spliterator.tryAdvance(consumer);
        assertThat(spliterator.estimateSize()).isEqualTo(Long.MAX_VALUE);
    }

    private static final class FutureContains<V> implements ArgumentMatcher<ListenableFuture<V>> {
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
