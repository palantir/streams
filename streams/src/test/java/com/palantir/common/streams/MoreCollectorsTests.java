/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@SuppressWarnings("RedundantStreamOptionalCall") // explicitly testing map stream
public class MoreCollectorsTests {

    public static Stream<List<Integer>> inputs() {
        return IntStream.of(0, 1, 1_000, 100_000)
                .mapToObj(size -> IntStream.range(0, size).boxed().collect(Collectors.toUnmodifiableList()));
    }

    private static final Function<Integer, Integer> triple = x -> x * 3;

    @ParameterizedTest
    @MethodSource("inputs")
    @SuppressWarnings("deprecation") // explicitly testing
    public void test_immutable_list(List<Integer> input) {
        List<Integer> list = input.stream().map(x -> x).collect(MoreCollectors.toImmutableList());
        assertThat(list).hasSize(input.size()).isEqualTo(input).containsExactlyElementsOf(input);
    }

    @ParameterizedTest
    @MethodSource("inputs")
    @SuppressWarnings({"DangerousParallelStreamUsage", "deprecation"}) // explicitly testing parallel streams
    public void test_parallel_immutable_list(List<Integer> input) {
        List<Integer> list = input.parallelStream().collect(MoreCollectors.toImmutableList());
        assertThat(list)
                .hasSize(input.size())
                .isEqualTo(input)
                .containsExactlyElementsOf(input)
                .isInstanceOf(ImmutableList.class);
    }

    @ParameterizedTest
    @MethodSource("inputs")
    @SuppressWarnings("deprecation") // explicitly testing
    public void test_immutable_set(List<Integer> input) {
        Set<Integer> set = input.stream().map(x -> x).collect(MoreCollectors.toImmutableSet());
        assertThat(set).hasSize(input.size()).containsExactlyElementsOf(input).isInstanceOf(ImmutableSet.class);
    }

    @ParameterizedTest
    @MethodSource("inputs")
    @SuppressWarnings({"DangerousParallelStreamUsage", "deprecation"}) // explicitly testing parallel streams
    public void test_parallel_immutable_set(List<Integer> input) {
        Set<Integer> set = input.parallelStream().collect(MoreCollectors.toImmutableSet());
        assertThat(set).hasSize(input.size()).containsExactlyElementsOf(input).isInstanceOf(ImmutableSet.class);
    }

    @ParameterizedTest
    @MethodSource("inputs")
    public void test_set_with_expected_size(List<Integer> input) {
        Set<Integer> set = input.stream().map(x -> x).collect(MoreCollectors.toSetWithExpectedSize(input.size()));
        assertThat(set).hasSize(input.size()).containsExactlyElementsOf(input).isInstanceOf(LinkedHashSet.class);
    }

    @ParameterizedTest
    @MethodSource("inputs")
    public void test_immutable_set_with_expected_size(List<Integer> input) {
        ImmutableSet<Integer> set =
                input.stream().map(x -> x).collect(MoreCollectors.toImmutableSetWithExpectedSize(input.size()));
        assertThat(set).hasSize(input.size()).containsExactlyElementsOf(input).isInstanceOf(ImmutableSet.class);
    }

    @ParameterizedTest
    @MethodSource("inputs")
    @SuppressWarnings("DangerousParallelStreamUsage") // explicitly testing parallel streams
    public void test_parallel_immutable_set_with_expected_size(List<Integer> input) {
        ImmutableSet<Integer> set =
                input.parallelStream().collect(MoreCollectors.toImmutableSetWithExpectedSize(input.size()));
        assertThat(set).hasSize(input.size()).containsExactlyElementsOf(input).isInstanceOf(ImmutableSet.class);
    }

    @Nested
    class ToImmutableMapDeprecated {

        @ParameterizedTest
        @MethodSource("com.palantir.common.streams.MoreCollectorsTests#inputs")
        @SuppressWarnings("deprecation") // explicitly testing
        public void test_immutable_map(List<Integer> input) {
            Map<Integer, Integer> map =
                    input.stream().map(x -> x).collect(MoreCollectors.toImmutableMap(k -> k, triple));
            assertThat(map).hasSize(input.size()).containsOnlyKeys(input).isInstanceOf(ImmutableMap.class);
            map.keySet().forEach(k -> assertThat(map.get(k)).as("key '%s'", k).isEqualTo(triple.apply(k)));
        }

        @ParameterizedTest
        @MethodSource("com.palantir.common.streams.MoreCollectorsTests#inputs")
        @SuppressWarnings({"DangerousParallelStreamUsage", "deprecation"}) // explicitly testing parallel streams
        public void test_parallel_immutable_map(List<Integer> input) {
            Map<Integer, Integer> map = input.parallelStream().collect(MoreCollectors.toImmutableMap(k -> k, triple));
            assertThat(map).hasSize(input.size()).containsOnlyKeys(input).isInstanceOf(ImmutableMap.class);
            map.keySet().forEach(k -> assertThat(map.get(k)).as("key '%s'", k).isEqualTo(triple.apply(k)));
        }

        @ParameterizedTest
        @MethodSource("com.palantir.common.streams.MoreCollectorsTests#inputs")
        @SuppressWarnings("deprecation") // explicitly testing
        public void test_immutable_map_with_expected_size(List<Integer> input) {
            Map<Integer, Integer> map =
                    input.stream().map(x -> x).collect(MoreCollectors.toImmutableMap(k -> k, triple));
            assertThat(map).hasSize(input.size()).containsOnlyKeys(input).isInstanceOf(ImmutableMap.class);
            map.keySet().forEach(k -> assertThat(map.get(k)).as("key '%s'", k).isEqualTo(triple.apply(k)));
        }

        @ParameterizedTest
        @MethodSource("com.palantir.common.streams.MoreCollectorsTests#inputs")
        @SuppressWarnings("DangerousParallelStreamUsage") // explicitly testing parallel streams
        public void test_parallel_immutable_map_with_expected_size(List<Integer> input) {
            ImmutableMap<Integer, Integer> map = input.parallelStream()
                    .collect(MoreCollectors.toImmutableMapWithExpectedSize(input.size(), k -> k, triple));
            assertThat(map).hasSize(input.size()).containsOnlyKeys(input).isInstanceOf(ImmutableMap.class);
            map.keySet().forEach(k -> assertThat(map.get(k)).as("key '%s'", k).isEqualTo(triple.apply(k)));
        }

        @Test
        @SuppressWarnings("deprecation") // explicitly testing
        public void test_immutable_map_duplicate_keys() {
            Stream<Integer> stream = Stream.of(1, 1);
            assertThatThrownBy(() -> stream.collect(MoreCollectors.toImmutableMap(k -> k, _k -> 2)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Multiple entries with same key: 1=2 and 1=2");
        }
    }

    private final Function<Integer, Integer> doubleValue = x -> x * 2;

    @Nested
    class ToImmutableMap {

        @ParameterizedTest
        @MethodSource("com.palantir.common.streams.MoreCollectorsTests#inputs")
        void toImmutableMap_preserves_iteration_order(List<Integer> input) {
            ImmutableMap<Integer, Integer> map = input.stream()
                    .map(i -> Maps.immutableEntry(i, doubleValue.apply(i)))
                    .collect(MoreCollectors.toImmutableMap());
            assertThat(map).hasSize(input.size()).isInstanceOf(ImmutableMap.class);
            assertThat(map.keySet())
                    .hasSize(input.size())
                    .containsExactlyElementsOf(input)
                    .isInstanceOf(ImmutableSet.class);
            map.forEach((k, _v) -> assertThat(map.get(k)).isEqualTo(doubleValue.apply(k)));
        }

        @Test
        public void toImmutableMap_throws_on_duplicate_keys() {
            AtomicInteger counter = new AtomicInteger(888);
            Stream<Map.Entry<Integer, Integer>> stream =
                    Stream.of(1, 1).map(i -> Maps.immutableEntry(i, counter.getAndIncrement()));

            assertThatThrownBy(() -> stream.collect(MoreCollectors.toImmutableMap()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Multiple entries with same key: 1=889 and 1=888");
        }

        @Test
        void throws_if_keys_are_null() {
            assertThatThrownBy(() -> Stream.of(Maps.immutableEntry(null, 1)).collect(MoreCollectors.toImmutableMap()))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("null key in entry: null=1");
        }

        @Test
        void throws_if_values_are_null() {
            assertThatThrownBy(() -> Stream.of(Maps.immutableEntry(1, null)).collect(MoreCollectors.toImmutableMap()))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessage("null value in entry: 1=null");
        }
    }

    @ParameterizedTest
    @MethodSource("inputs")
    public void test_list_with_expected_size(List<Integer> input) {
        List<Integer> list = input.stream().map(x -> x).collect(MoreCollectors.toListWithExpectedSize(input.size()));
        assertThat(list)
                .hasSize(input.size())
                .isEqualTo(input)
                .containsExactlyElementsOf(input)
                .isInstanceOf(ArrayList.class);
    }

    @ParameterizedTest
    @MethodSource("inputs")
    @SuppressWarnings("DangerousParallelStreamUsage") // explicitly testing parallel streams
    public void test_parallel_list_with_expected_size(List<Integer> input) {
        List<Integer> list = input.parallelStream().collect(MoreCollectors.toListWithExpectedSize(input.size()));
        assertThat(list)
                .hasSize(input.size())
                .isEqualTo(input)
                .containsExactlyElementsOf(input)
                .isInstanceOf(ArrayList.class);
    }

    @ParameterizedTest
    @MethodSource("inputs")
    public void test_immutable_list_with_expected_size(List<Integer> input) {
        List<Integer> list =
                input.stream().map(x -> x).collect(MoreCollectors.toImmutableListWithExpectedSize(input.size()));
        assertThat(list)
                .hasSize(input.size())
                .isEqualTo(input)
                .containsExactlyElementsOf(input)
                .isInstanceOf(ImmutableList.class);
    }

    @ParameterizedTest
    @MethodSource("inputs")
    @SuppressWarnings("DangerousParallelStreamUsage") // explicitly testing parallel streams
    public void test_parallel_immutable_list_with_expected_size(List<Integer> input) {
        List<Integer> list =
                input.parallelStream().collect(MoreCollectors.toImmutableListWithExpectedSize(input.size()));
        assertThat(list)
                .hasSize(input.size())
                .isEqualTo(input)
                .containsExactlyElementsOf(input)
                .isInstanceOf(ImmutableList.class);
    }
}
