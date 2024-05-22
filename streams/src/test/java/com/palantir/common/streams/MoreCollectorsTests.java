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

import com.google.common.collect.Maps;
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

public class MoreCollectorsTests {

    private static final List<Integer> LARGE_LIST =
            IntStream.range(0, 100000).boxed().collect(Collectors.toList());

    @Test
    public void test_immutable_list() {
        List<Integer> list = LARGE_LIST.stream().collect(MoreCollectors.toImmutableList());
        assertThat(list).containsExactlyElementsOf(LARGE_LIST);
    }

    @Test
    @SuppressWarnings("DangerousParallelStreamUsage") // explicitly testing parallel streams
    public void test_parallel_immutable_list() {
        List<Integer> list = LARGE_LIST.parallelStream().collect(MoreCollectors.toImmutableList());
        assertThat(list).containsExactlyElementsOf(LARGE_LIST);
    }

    @Test
    public void test_immutable_set() {
        Set<Integer> set = LARGE_LIST.stream().collect(MoreCollectors.toImmutableSet());
        assertThat(set).containsExactlyElementsOf(LARGE_LIST);
    }

    @Test
    @SuppressWarnings("DangerousParallelStreamUsage") // explicitly testing parallel streams
    public void test_parallel_immutable_set() {
        Set<Integer> set = LARGE_LIST.parallelStream().collect(MoreCollectors.toImmutableSet());
        assertThat(set).containsExactlyElementsOf(LARGE_LIST);
    }

    private Function<Integer, Integer> valueMap = x -> x * 2;

    @Nested
    class ToImmutableMapDeprecated {
        @Test
        public void test_immutable_map() {
            Map<Integer, Integer> map = LARGE_LIST.stream().collect(MoreCollectors.toImmutableMap(k -> k, valueMap));
            assertThat(map.keySet()).containsExactlyElementsOf(LARGE_LIST);
            map.forEach((k, _v) -> assertThat(map.get(k)).isEqualTo(valueMap.apply(k)));
        }

        @Test
        @SuppressWarnings("DangerousParallelStreamUsage") // explicitly testing parallel streams
        public void test_parallel_immutable_map() {
            Map<Integer, Integer> map =
                    LARGE_LIST.parallelStream().collect(MoreCollectors.toImmutableMap(k -> k, valueMap));
            assertThat(map.keySet()).containsExactlyElementsOf(LARGE_LIST);
            map.forEach((k, _v) -> assertThat(map.get(k)).isEqualTo(valueMap.apply(k)));
        }

        @Test
        public void test_immutable_map_duplicate_keys() {
            Stream<Integer> stream = Stream.of(1, 1);
            assertThatThrownBy(() -> stream.collect(MoreCollectors.toImmutableMap(k -> k, _k -> 2)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Multiple entries with same key: 1=2 and 1=2");
        }
    }

    @Nested
    class ToImmutableMap {

        @Test
        void toImmutableMap_preserves_iteration_order() {
            Map<Integer, Integer> map = LARGE_LIST.stream()
                    .map(i -> Maps.immutableEntry(i, valueMap.apply(i)))
                    .collect(MoreCollectors.toImmutableMap());
            assertThat(map.keySet()).containsExactlyElementsOf(LARGE_LIST);
            map.forEach((k, _v) -> assertThat(map.get(k)).isEqualTo(valueMap.apply(k)));
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
}
