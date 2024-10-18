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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Java 8 {@link Collector} for immutable collections.
 */
public final class MoreCollectors {

    /**
     * This collector has similar semantics to {@link Collectors#toSet()}; however,
     * the builder will be presized with the expected size to avoid resizing while collecting.
     */
    public static <T> Collector<T, ?, Set<T>> toSetWithExpectedSize(int expectedSize) {
        return Collectors.toCollection(() -> Sets.newLinkedHashSetWithExpectedSize(expectedSize));
    }

    /**
     * This collector has similar semantics to {@link ImmutableSet#toImmutableSet()}; however,
     * the builder will be presized with the expected size to avoid resizing while collecting.
     */
    public static <T> Collector<T, ?, ImmutableSet<T>> toImmutableSetWithExpectedSize(int expectedSize) {
        return Collector.of(
                () -> ImmutableSet.<T>builderWithExpectedSize(expectedSize),
                ImmutableSet.Builder::add,
                (left, right) -> left.addAll(right.build()),
                ImmutableSet.Builder::build);
    }

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
     * This collector has similar semantics to {@link Collectors#toList()}; however,
     * the builder will be presized with the expected size to avoid resizing while collecting.
     */
    public static <T> Collector<T, ?, List<T>> toListWithExpectedSize(int expectedSize) {
        return Collectors.toCollection(() -> new ArrayList<>(expectedSize));
    }

    /**
     * This collector has similar semantics to {@link ImmutableList#toImmutableList()}; however,
     * the builder will be presized with the expected size to avoid resizing while collecting.
     */
    public static <T> Collector<T, ?, ImmutableList<T>> toImmutableListWithExpectedSize(int expectedSize) {
        return Collector.of(
                () -> ImmutableList.<T>builderWithExpectedSize(expectedSize),
                ImmutableList.Builder::add,
                (left, right) -> left.addAll(right.build()),
                ImmutableList.Builder::build);
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
     * Collect a Stream of Map.Entry (e.g. a StreamEx EntryStream) into a Guava {@link ImmutableMap}, which preserves
     * iteration order. Duplicate keys will result in an error. Throws NullPointerException if any key or value is null.
     *
     * For behaviour details, see docs on {@link ImmutableMap#toImmutableMap}.
     *
     * Beware that {@code EntryStream#toImmutableMap()} does NOT preserve iteration order, as it uses a regular HashMap.
     */
    public static <K, V> Collector<Map.Entry<K, V>, ?, ImmutableMap<K, V>> toImmutableMap() {
        return ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue);
    }

    /**
     * This collector has similar semantics to {@link Collectors#toMap} except that the resulting map will be
     * immutable. Duplicate keys will result in an error.
     *
     * @deprecated Use {@link ImmutableMap#toImmutableMap}, available in Guava 21+
     */
    @Deprecated
    public static <T, K, V> Collector<T, ?, Map<K, V>> toImmutableMap(
            Function<T, K> keyFunction, Function<T, V> valueFunction) {
        return Collector.of(
                ImmutableMap::<K, V>builder,
                (builder, value) -> builder.put(keyFunction.apply(value), valueFunction.apply(value)),
                (left, right) -> left.putAll(right.buildOrThrow()),
                ImmutableMap.Builder::build);
    }

    /**
     * This collector has similar semantics to {@link ImmutableMap#toImmutableMap(Function, Function)}; however,
     * the builder will be presized with the expected size to avoid resizing while collecting.
     * Duplicate keys will result in an error.
     */
    public static <T, K, V> Collector<T, ?, ImmutableMap<K, V>> toImmutableMapWithExpectedSize(
            int expectedSize, Function<T, K> keyFunction, Function<T, V> valueFunction) {
        return Collector.of(
                () -> ImmutableMap.<K, V>builderWithExpectedSize(expectedSize),
                (builder, value) -> builder.put(keyFunction.apply(value), valueFunction.apply(value)),
                (left, right) -> left.putAll(right.buildOrThrow()),
                ImmutableMap.Builder::build);
    }

    private MoreCollectors() {}
}
