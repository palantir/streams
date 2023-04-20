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
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@SuppressWarnings("RedundantStreamOptionalCall") // explicitly testing map stream
public class MoreCollectorsTests {

    public static Stream<Arguments> list() {
        return IntStream.of(0, 1, 1_000, 100_000)
                .mapToObj(size ->
                        Arguments.of(IntStream.range(0, size).boxed().collect(Collectors.toUnmodifiableList())));
    }

    @ParameterizedTest
    @MethodSource("list")
    @SuppressWarnings("deprecation") // explicitly testing
    public void test_immutable_list(List<Integer> input) {
        List<Integer> list = input.stream().map(x -> x).collect(MoreCollectors.toImmutableList());
        assertThat(list).isEqualTo(input).containsExactlyElementsOf(input);
    }

    @ParameterizedTest
    @MethodSource("list")
    @SuppressWarnings({"DangerousParallelStreamUsage", "deprecation"}) // explicitly testing parallel streams
    public void test_parallel_immutable_list(List<Integer> input) {
        List<Integer> list = input.parallelStream().collect(MoreCollectors.toImmutableList());
        assertThat(list).isEqualTo(input).containsExactlyElementsOf(input);
    }

    @ParameterizedTest
    @MethodSource("list")
    @SuppressWarnings("deprecation") // explicitly testing
    public void test_immutable_set(List<Integer> input) {
        Set<Integer> set = input.stream().map(x -> x).collect(MoreCollectors.toImmutableSet());
        assertThat(set).containsExactlyElementsOf(input);
    }

    @ParameterizedTest
    @MethodSource("list")
    @SuppressWarnings({"DangerousParallelStreamUsage", "deprecation"}) // explicitly testing parallel streams
    public void test_parallel_immutable_set(List<Integer> input) {
        Set<Integer> set = input.parallelStream().collect(MoreCollectors.toImmutableSet());
        assertThat(set).containsExactlyElementsOf(input);
    }

    @ParameterizedTest
    @MethodSource("list")
    public void test_set_with_expected_size(List<Integer> input) {
        Set<Integer> set = input.stream().map(x -> x).collect(MoreCollectors.toSetWithExpectedSize(input.size()));
        assertThat(set).hasSize(input.size()).containsExactlyElementsOf(input).isInstanceOf(LinkedHashSet.class);
    }

    @ParameterizedTest
    @MethodSource("list")
    public void test_immutable_set_with_expected_size(List<Integer> input) {
        ImmutableSet<Integer> set =
                input.stream().map(x -> x).collect(MoreCollectors.toImmutableSetWithExpectedSize(input.size()));
        assertThat(set).hasSize(input.size()).containsExactlyElementsOf(input).isInstanceOf(ImmutableSet.class);
    }

    @ParameterizedTest
    @MethodSource("list")
    @SuppressWarnings("DangerousParallelStreamUsage") // explicitly testing parallel streams
    public void test_parallel_immutable_set_with_expected_size(List<Integer> input) {
        ImmutableSet<Integer> set =
                input.parallelStream().collect(MoreCollectors.toImmutableSetWithExpectedSize(input.size()));
        assertThat(set).hasSize(input.size()).containsExactlyElementsOf(input).isInstanceOf(ImmutableSet.class);
    }

    private final Function<Integer, Integer> triple = x -> x * 3;

    @ParameterizedTest
    @MethodSource("list")
    @SuppressWarnings("deprecation") // explicitly testing
    public void test_immutable_map(List<Integer> input) {
        Map<Integer, Integer> map = input.stream().map(x -> x).collect(MoreCollectors.toImmutableMap(k -> k, triple));
        assertThat(map.keySet()).containsExactlyElementsOf(input);
        map.forEach((k, _v) -> assertThat(map.get(k)).isEqualTo(triple.apply(k)));
    }

    @ParameterizedTest
    @MethodSource("list")
    @SuppressWarnings({"DangerousParallelStreamUsage", "deprecation"}) // explicitly testing parallel streams
    public void test_parallel_immutable_map(List<Integer> input) {
        Map<Integer, Integer> map = input.parallelStream().collect(MoreCollectors.toImmutableMap(k -> k, triple));
        assertThat(map.keySet()).containsExactlyElementsOf(input);
        map.forEach((k, _v) -> assertThat(map.get(k)).isEqualTo(triple.apply(k)));
    }

    @ParameterizedTest
    @MethodSource("list")
    @SuppressWarnings("deprecation") // explicitly testing
    public void test_immutable_map_with_expected_size(List<Integer> input) {
        Map<Integer, Integer> map = input.stream().map(x -> x).collect(MoreCollectors.toImmutableMap(k -> k, triple));
        assertThat(map.keySet()).hasSize(input.size()).containsExactlyElementsOf(input);
        map.forEach((k, _v) -> assertThat(map.get(k)).isEqualTo(triple.apply(k)));
    }

    @ParameterizedTest
    @MethodSource("list")
    @SuppressWarnings("DangerousParallelStreamUsage") // explicitly testing parallel streams
    public void test_parallel_immutable_map_with_expected_size(List<Integer> input) {
        Map<Integer, Integer> map = input.parallelStream()
                .collect(MoreCollectors.toImmutableMapWithExpectedSize(input.size(), k -> k, triple));
        assertThat(map.keySet()).hasSize(input.size()).containsExactlyElementsOf(input);
        map.forEach((k, _v) -> assertThat(map.get(k)).isEqualTo(triple.apply(k)));
    }

    @Test
    @SuppressWarnings("deprecation") // explicitly testing
    public void test_immutable_map_duplicate_keys() {
        Stream<Integer> stream = Stream.of(1, 1);
        assertThatThrownBy(() -> stream.collect(MoreCollectors.toImmutableMap(k -> k, _k -> 2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Multiple entries with same key: 1=2 and 1=2");
    }

    @ParameterizedTest
    @MethodSource("list")
    public void test_list_with_expected_size(List<Integer> input) {
        List<Integer> list = input.stream().map(x -> x).collect(MoreCollectors.toListWithExpectedSize(input.size()));
        assertThat(list)
                .hasSize(input.size())
                .isEqualTo(input)
                .containsExactlyElementsOf(input)
                .isInstanceOf(ArrayList.class);
    }

    @ParameterizedTest
    @MethodSource("list")
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
    @MethodSource("list")
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
    @MethodSource("list")
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
