/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Java 8 {@link Collector} for immutable collections.
 */
public class MoreCollectors {

    /**
     * This collector has similar semantics to {@link Collectors#toSet} except that the resulting set will be
     * immutable.
     *
     * @deprecated Use {@link ImmutableSet#toImmutableSet}, available in Guava 21+
     */
    @Deprecated
    public static <T> Collector<T, ?, Set<T>> toImmutableSet() {
        return Collector.of(
                ImmutableSet::<T>builder,
                ImmutableSet.Builder::<T>add,
                (left, right) -> left.addAll(right.build()),
                ImmutableSet.Builder::build,
                Collector.Characteristics.UNORDERED);
    }

    /**
     * This collector has similar semantics to {@link Collectors#toList} except that the resulting list will be
     * immutable.
     *
     * @deprecated Use {@link ImmutableList#toImmutableList}, available in Guava 21+
     */
    @Deprecated
    public static <T> Collector<T, ?, List<T>> toImmutableList() {
        return Collector.of(
                ImmutableList::<T>builder,
                ImmutableList.Builder::<T>add,
                (left, right) -> left.addAll(right.build()),
                ImmutableList.Builder::build);
    }

    /**
     * This collector has similar semantics to {@link Collectors#toMap} except that the resulting map will be
     * immutable. Duplicate keys will result in an error.
     *
     * @deprecated Use {@link ImmutableMap#toImmutableMap}, available in Guava 21+
     */
    @Deprecated
    public static <T, K, V> Collector<T, ?, Map<K, V>> toImmutableMap(Function<T, K> keyFunction,
                                                                      Function<T, V> valueFunction) {
        return Collector.of(
                ImmutableMap::<K, V>builder,
                (builder, value) -> builder.put(keyFunction.apply(value), valueFunction.apply(value)),
                (left, right) -> left.putAll(right.build()),
                ImmutableMap.Builder::build);
    }

    private MoreCollectors() {
    }
}
