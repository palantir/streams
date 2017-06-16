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

import static com.google.common.collect.Maps.immutableEntry;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;

/**
 * A stream of values, each of which has an associated key that is preserved as the values are
 * filtered and mapped.
 *
 * <p>Equivalently, a stream of map entries with convenience methods for the common operations
 * of filtering and mapping keys and values independently. Uniqueness of keys is only enforced when
 * the stream is collected into a map.
 *
 * <p>This class is very incomplete; please feel free to add obviously missing functionality, like
 * supporting more of {@link Stream}'s list of methods, and streaming to/from multimaps.
 */
public interface KeyedStream<K, V> {

    /**
     * Returns a keyed stream consisting of the entries of this stream whose values match
     * the given predicate.
     */
    default KeyedStream<K, V> filter(Predicate<? super V> predicate) {
        return filterEntries((key, value) -> predicate.test(value));
    }

    /**
     * Returns a keyed stream consisting of the entries of this stream whose values match
     * the given predicate.
     */
    default KeyedStream<K, V> filterKeys(Predicate<? super K> predicate) {
        return filterEntries((key, value) -> predicate.test(key));
    }

    /**
     * Returns a keyed stream consisting of the entries of this stream whose values match
     * the given predicate.
     */
    KeyedStream<K, V> filterEntries(BiPredicate<? super K, ? super V> predicate);

    /**
     * Returns a keyed stream consisting of the results of replacing each value of this stream with
     * the value produced by applying the provided mapping function.
     */
    default <R> KeyedStream<K, R> map(Function<? super V, ? extends R> mapper) {
        return mapEntries((key, value) -> immutableEntry(key, mapper.apply(value)));
    }

    /**
     * Returns a keyed stream consisting of the results of replacing each value of this stream with
     * the value produced by applying the provided mapping function to that entry.
     */
    default <R> KeyedStream<K, R> map(BiFunction<? super K, ? super V, ? extends R> entryMapper) {
        return mapEntries((key, value) -> immutableEntry(key, entryMapper.apply(key, value)));
    }

    /**
     * Returns a keyed stream consisting of the results of replacing each key of this stream with
     * the key produced by applying the provided mapping function.
     */
    default <R> KeyedStream<R, V> mapKeys(Function<? super K, ? extends R> keyMapper) {
        return mapEntries((key, value) -> immutableEntry(keyMapper.apply(key), value));
    }

    /**
     * Returns a keyed stream consisting of the results of replacing each key of this stream with
     * the key produced by applying the provided mapping function to that entry.
     */
    default <R> KeyedStream<R, V> mapKeys(BiFunction<? super K, ? super V, ? extends R> keyMapper) {
        return mapEntries((key, value) -> immutableEntry(keyMapper.apply(key, value), value));
    }

    /**
     * Returns a keyed stream consisting of the entries returned by applying the given function to each
     * entry.
     */
    <K2, V2> KeyedStream<K2, V2> mapEntries(
            BiFunction<? super K, ? super V, ? extends Map.Entry<? extends K2, ? extends V2>> entryMapper);

    /**
     * Returns a keyed stream consisting of the results of replacing each value of this stream with the
     * contents of a mapped stream produced by applying the provided mapping function.
     */
    default <R> KeyedStream<K, R> flatMap(Function<? super V, ? extends Stream<? extends R>> mapper) {
        return flatMapEntries((key, value) -> mapper.apply(value).map(newValue -> immutableEntry(key, newValue)));
    }

    /**
     * Returns a keyed stream consisting of the results of replacing each value of this stream with the
     * contents of a mapped stream produced by applying the provided mapping function to that entry.
     */
    default <R> KeyedStream<K, R> flatMap(BiFunction<? super K, ? super V, ? extends Stream<? extends R>> entryMapper) {
        return flatMapEntries((key, value) -> entryMapper.apply(key, value).map(newValue -> immutableEntry(key, newValue)));
    }

    /**
     * Returns a keyed stream consisting of the results of replacing each key of this stream with the
     * contents of a mapped stream produced by applying the provided mapping function.
     */
    default <R> KeyedStream<R, V> flatMapKeys(Function<? super K, ? extends Stream<? extends R>> keyMapper) {
        return flatMapEntries((key, value) -> keyMapper.apply(key).map(newKey -> immutableEntry(newKey, value)));
    }

    /**
     * Returns a keyed stream consisting of the results of replacing each key of this stream with the
     * contents of a mapped stream produced by applying the provided mapping function to that entry.
     */
    default <R> KeyedStream<R, V> flatMapKeys(BiFunction<? super K, ? super V, ? extends Stream<? extends R>> keyMapper) {
        return flatMapEntries((key, value) -> keyMapper.apply(key, value).map(newKey -> immutableEntry(newKey, value)));
    }

    /**
     * Returns a keyed stream consisting of the results of replacing each entry of this stream with the
     * contents of a mapped stream produced by applying the provided mapping function.
     */
    <K2, V2> KeyedStream<K2, V2> flatMapEntries(
            BiFunction<? super K, ? super V, ? extends Stream<? extends Map.Entry<? extends K2, ? extends V2>>> entryMapper);

    /**
     * Accumulates the entries of this stream into a new map.
     *
     * <p>There are no guarantees on the type, mutability, serializability, or thread-safety of the
     * {@code Map} returned; if more control is required, use {@link #collectTo(Supplier)}.
     */
    default Map<K, V> collectToMap() {
        return collectTo(LinkedHashMap::new);
    }

    /**
     * Accumulates the entries of this stream into a new {@code SetMultimap}.
     *
     * <p>There are no guarantees on the type, mutability, serializability, or thread-safety of the
     * {@code SetMultimap} returned; if more control is required, use {@link #collectToMultimap(Supplier)}.
     */
    default SetMultimap<K, V> collectToSetMultimap() {
        return collectToMultimap(LinkedHashMultimap::create);
    }

    /**
     * Accumulates the entries of this stream into a new {@link Map}, in encounter order. The
     * {@code Map} is created by the provided factory.
     *
     * @throws IllegalStateException if duplicate keys are encountered
     */
    <M extends Map<K, V>> M collectTo(Supplier<M> mapFactory);

    /**
     * Accumulates the entries of this stream into a new {@link Multimap}, in encounter order. The
     * {@code Multimap} is created by the provided factory.
     */
    <M extends Multimap<K, V>> M collectToMultimap(Supplier<M> multimapFactory);

    /**
     * Returns a stream of the keys of each entry of this stream, dropping the associated values.
     */
    Stream<K> keys();

    /**
     * Returns a stream of the values of each entry of this stream, dropping the associated keys.
     */
    Stream<V> values();

    /**
     * Returns a keyed stream with matching keys and values taken from {@code stream}.
     */
    static <V> KeyedStream<V, V> of(Stream<V> stream) {
        return new KeyedStreamImpl<>(stream.map(value -> immutableEntry(value, value)));
    }

    /**
     * Returns a keyed stream with matching keys and values taken from {@code items}.
     */
    static <V> KeyedStream<V, V> of(Iterable<? extends V> items) {
        return KeyedStream.of(MoreStreams.stream(items));
    }

    /**
     * Returns a keyed stream of {@code map}'s entries.
     */
    static <K, V> KeyedStream<K, V> stream(Map<K, V> map) {
        return ofEntries(map.entrySet().stream());
    }

    /**
     * Returns a keyed stream of every entry in all {@code maps}.
     */
    static <K, V> KeyedStream<K, V> streamAll(Iterable<? extends Map<? extends K, ? extends V>> maps) {
        @SuppressWarnings("unchecked")  // Safe to discard the wildcards
        KeyedStream<K, V> result = ofEntries(MoreStreams.stream(maps)
                .flatMap(map -> ((Map<K, V>)(Map<?, ?>) map).entrySet().stream()));
        return result;
    }

    /**
     * Returns a keyed stream of every entry in all maps passed in.
     */
    @SafeVarargs
    static <K, V> KeyedStream<K, V> streamAll(
            Map<? extends K, ? extends V> map1,
            Map<? extends K, ? extends V> map2,
            Map<? extends K, ? extends V>... otherMaps) {
        return streamAll(ImmutableList.<Map<? extends K, ? extends V>>builder()
                .add(map1)
                .add(map2)
                .add(otherMaps)
                .build());
    }

    /**
     * Returns a keyed stream of Entries.
     */
    static <K, V> KeyedStream<K, V> ofEntries(Stream<Map.Entry<K, V>> entries) {
        return new KeyedStreamImpl<>(entries);
    }

    /**
     * Returns a keyed stream of {@code multimap}'s entries.
     */
    static <K, V> KeyedStream<K, V> stream(Multimap<K, V> multimap) {
        return new KeyedStreamImpl<>(multimap.entries().stream());
    }

    /**
     * Collects a stream and restreams it as a keyed stream, where key and value are the same.
     *
     * <p>Consider the (less fluent) {@link #of(Stream)} if the stream is likely to be
     * large and/or concurrent, as it avoids serializing into a temporary collection.
     */
    static <V> Collector<V, List<V>, KeyedStream<V, V>> toKeyedStream() {
        return Collector.of(
                ArrayList::new,
                List::add,
                (List<V> left, List<V> right) -> {
                    left.addAll(right);
                    return left;
                },
                (List<V> values) -> KeyedStream.of(values.stream()));
    }

}
