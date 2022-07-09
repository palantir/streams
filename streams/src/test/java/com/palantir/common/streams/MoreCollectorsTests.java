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

import static com.google.common.truth.Truth.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.rules.ExpectedException;

public class MoreCollectorsTests {

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    private static final List<Integer> LARGE_LIST =
            IntStream.range(0, 100000).boxed().collect(Collectors.toList());

    @Test
    public void test_immutable_list() {
        List<Integer> list = LARGE_LIST.stream().collect(MoreCollectors.toImmutableList());
        assertThat(list).isEqualTo(LARGE_LIST);
    }

    @Test
    public void test_parallel_immutable_list() {
        List<Integer> list = LARGE_LIST.parallelStream().collect(MoreCollectors.toImmutableList());
        assertThat(list).isEqualTo(LARGE_LIST);
    }

    @Test
    public void test_immutable_set() {
        Set<Integer> set = LARGE_LIST.stream().collect(MoreCollectors.toImmutableSet());
        assertThat(set).containsExactlyElementsIn(LARGE_LIST);
    }

    @Test
    public void test_parallel_immutable_set() {
        Set<Integer> set = LARGE_LIST.parallelStream().collect(MoreCollectors.toImmutableSet());
        assertThat(set).containsExactlyElementsIn(LARGE_LIST);
    }

    private Function<Integer, Integer> valueMap = x -> x * 2;

    @Test
    public void test_immutable_map() {
        Map<Integer, Integer> map = LARGE_LIST.stream().collect(MoreCollectors.toImmutableMap(k -> k, valueMap));
        assertThat(map.keySet()).containsExactlyElementsIn(LARGE_LIST);
        map.forEach((k, _v) -> assertThat(map.get(k)).isEqualTo(valueMap.apply(k)));
    }

    @Test
    public void test_parallel_immutable_map() {
        Map<Integer, Integer> map =
                LARGE_LIST.parallelStream().collect(MoreCollectors.toImmutableMap(k -> k, valueMap));
        assertThat(map.keySet()).containsExactlyElementsIn(LARGE_LIST);
        map.forEach((k, _v) -> assertThat(map.get(k)).isEqualTo(valueMap.apply(k)));
    }

    @Test
    public void test_immutable_map_duplicate_keys() {
        thrown.expect(IllegalArgumentException.class);
        Stream.of(1, 1).collect(MoreCollectors.toImmutableMap(k -> k, _k -> 2));
    }
}
