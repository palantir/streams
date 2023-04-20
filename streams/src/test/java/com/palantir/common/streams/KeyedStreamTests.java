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

import static com.google.common.collect.Maps.immutableEntry;
import static com.palantir.common.streams.KeyedStream.toKeyedStream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

public class KeyedStreamTests {

    @Test
    public void test_collect_single_value_as_map() {
        Map<Integer, Integer> map = Stream.of(6).collect(toKeyedStream()).collectToMap();
        assertThat(map).isEqualTo(ImmutableMap.of(6, 6));
    }

    @Test
    public void test_collect_multiple_values_as_map() {
        Map<Integer, Integer> map = Stream.of(6, 8, 12).collect(toKeyedStream()).collectToMap();
        assertThat(map).isEqualTo(ImmutableMap.of(6, 6, 8, 8, 12, 12));
    }

    @Test
    public void test_collect_duplicate_keys_as_map() {
        KeyedStream<Integer, Integer> stream = Stream.of(6, 6).collect(toKeyedStream());
        assertThatThrownBy(stream::collectToMap)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Duplicate key 6");
    }

    @Test
    public void test_stream_of_single_value_as_map() {
        Map<Integer, Integer> map = KeyedStream.of(Stream.of(6)).collectToMap();
        assertThat(map).isEqualTo(ImmutableMap.of(6, 6));
    }

    @Test
    public void test_stream_multiple_values_as_map() {
        Map<Integer, Integer> map =
                KeyedStream.stream(ImmutableMap.of(4, 8, 3, 6, 6, 12)).collectToMap();
        assertThat(map).isEqualTo(ImmutableMap.of(3, 6, 4, 8, 6, 12));
    }

    @Test
    public void test_stream_multiple_values_as_multimap() {
        SetMultimap<Integer, Integer> map =
                KeyedStream.stream(ImmutableSetMultimap.of(1, 2, 3, 4)).collectToSetMultimap();
        assertThat(map).isEqualTo(ImmutableSetMultimap.of(3, 4, 1, 2));
    }

    @Test
    public void test_collect_multiple_values_as_sorted_map() {
        Map<String, Integer> map = KeyedStream.of(Stream.of(4, 3, 5))
                .mapKeys(n -> String.join("", Iterables.limit(Iterables.cycle("x"), n)))
                .collectTo(TreeMap::new);
        assertThat(map.entrySet())
                .containsExactly(immutableEntry("xxx", 3), immutableEntry("xxxx", 4), immutableEntry("xxxxx", 5));
    }

    @Test
    public void test_can_collect_keys() {
        Set<String> keys = ImmutableSet.of("first", "second");

        Map<String, Integer> map = KeyedStream.of(keys).map(String::length).collectToMap();

        assertThat(KeyedStream.stream(map).keys().collect(Collectors.toSet())).isEqualTo(keys);
    }

    @Test
    public void test_can_collect_values() {
        Set<String> values = ImmutableSet.of("first", "second");

        Map<Integer, String> map =
                KeyedStream.of(values).mapKeys(String::length).collectToMap();

        assertThat(KeyedStream.stream(map).values().collect(Collectors.toSet())).isEqualTo(values);
    }

    @Test
    public void test_map() {
        Map<Integer, Integer> map =
                Stream.of(6, 8, 12).collect(toKeyedStream()).map(v -> v * 2).collectToMap();
        assertThat(map).isEqualTo(ImmutableMap.of(6, 12, 8, 16, 12, 24));
    }

    @Test
    public void test_filter() {
        Map<Integer, Integer> map = Stream.of(6, 8, 12)
                .collect(toKeyedStream())
                .map(v -> v * 2)
                .filter(v -> v == 16)
                .collectToMap();
        assertThat(map).isEqualTo(ImmutableMap.of(8, 16));
    }

    @Test
    public void test_map_keys() {
        Map<Integer, Integer> map =
                Stream.of(6, 8, 12).collect(toKeyedStream()).mapKeys(k -> k + 2).collectToMap();
        assertThat(map).isEqualTo(ImmutableMap.of(8, 6, 10, 8, 14, 12));
    }

    @Test
    public void test_stream_entries() {
        Stream<Map.Entry<Integer, Integer>> entryStream =
                ImmutableSet.of(1, 2, 3).stream().map(i -> new AbstractMap.SimpleEntry<>(i, i * i));
        Map<Integer, Integer> map = KeyedStream.ofEntries(entryStream).collectToMap();
        assertThat(map).isEqualTo(ImmutableMap.of(1, 1, 2, 4, 3, 9));
    }

    @Test
    public void test_for_each_entry() {
        List<Integer> values = ImmutableList.of(1, 2, 3);
        List<Integer> result = new ArrayList<>();

        KeyedStream.of(values).map(x -> x + 3).forEach((left, right) -> {
            result.add(left);
            result.add(right);
        });

        assertThat(result).containsExactly(1, 4, 2, 5, 3, 6);
    }
}
