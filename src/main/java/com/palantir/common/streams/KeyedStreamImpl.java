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

import static com.google.common.base.Preconditions.checkState;

import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.google.common.collect.Multimap;

class KeyedStreamImpl<K, V> implements KeyedStream<K, V> {

    private final Stream<Entry<? extends K, ? extends V>> entries;

    public KeyedStreamImpl(Stream<Entry<? extends K, ? extends V>> entries) {
        this.entries = entries;
    }

    @Override
    public KeyedStream<K, V> filterEntries(BiPredicate<? super K, ? super V> predicate) {
        return new KeyedStreamImpl<>(entries.filter(entry -> predicate.test(entry.getKey(), entry.getValue())));
    }

    @Override
    public <T extends Map<K, V>> T collectTo(Supplier<T> supplier) {
        return entries.collect(supplier, KeyedStreamImpl::accumulate, KeyedStreamImpl::combine);
    }

    @Override
    public <M extends Multimap<K, V>> M collectToMultimap(Supplier<M> supplier) {
        return entries.collect(supplier::get, KeyedStreamImpl::accumulate, KeyedStreamImpl::combine);
    }

    @Override
    public <K2, V2> KeyedStream<K2, V2> mapEntries(
            BiFunction<? super K, ? super V, ? extends Entry<? extends K2, ? extends V2>> entryMapper) {
        return new KeyedStreamImpl<K2, V2>(entries.<Map.Entry<? extends K2, ? extends V2>>map(entry ->
                entryMapper.apply(entry.getKey(), entry.getValue())));
    }

    @Override
    public <K2, V2> KeyedStream<K2, V2> flatMapEntries(
            BiFunction<? super K, ? super V, ? extends Stream<? extends Entry<? extends K2, ? extends V2>>> entryMapper) {
        return new KeyedStreamImpl<K2, V2>(entries.flatMap(entry ->
                entryMapper.apply(entry.getKey(), entry.getValue())));
    }

    @Override
    public Stream<K> keys() {
        return entries.map(Entry::getKey);
    }

    @Override
    public Stream<V> values() {
        return entries.map(Entry::getValue);
    }

    private static <K, V> void accumulate(Map<K, V> map, Map.Entry<? extends K, ? extends V> entry) {
        checkState(!map.containsKey(entry.getKey()), "Duplicate key %s", entry.getKey());
        map.put(entry.getKey(), entry.getValue());
    }

    private static <K, V> void combine(Map<K, V> map, Map<K, V> entries) {
        map.keySet().stream().filter(entries::containsKey).findAny().map(duplicate -> {
            throw new IllegalStateException("Duplicate key " + duplicate);
        });
        map.putAll(entries);
    }

    private static <K, V> void accumulate(Multimap<K, V> multimap, Map.Entry<? extends K, ? extends V> entry) {
        multimap.put(entry.getKey(), entry.getValue());
    }

    private static <K, V> void combine(Multimap<K, V> multimap, Multimap<K, V> entries) {
        multimap.putAll(entries);
    }
}
