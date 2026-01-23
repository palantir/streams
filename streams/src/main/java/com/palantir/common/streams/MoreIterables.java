/*
 * (c) Copyright 2026 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.jspecify.annotations.Nullable;

public final class MoreIterables {
    private MoreIterables() {}

    /**
     * Divides an iterable into unmodifiable sublists of the given size (the final iterable may be
     * smaller). For example, partitioning an iterable containing {@code [a, b, c, d, e]} with a
     * partition size of 3 yields {@code [[a, b, c], [d, e]]} -- an outer iterable containing two
     * inner lists of three and two elements, all in the original order.
     * <p>
     * Iterators returned by the returned iterable do not support the {@link java.util.Iterator#remove()}
     * method. The returned lists implement {@link java.util.RandomAccess}, if the input list does.
     * <p>
     * Similar to Guava {@link com.google.common.collect.Iterables#partition(Iterable, int)} and
     * {@link com.google.common.collect.Lists#partition(List, int)};
     * however, {@link Iterables#partition(Iterable, int)} eagerly allocates storage while this implementation
     * avoids excess allocations and delegates to {@link Lists#partition(List, int)} where possible.
     *
     * @param items the collection to return a partitioned view of
     * @param size the desired size of each sublist (the last may be smaller)
     * @return an iterable of unmodifiable lists containing the elements of {@code iterable} divided into partitions
     */
    public static <T extends @Nullable Object> Iterable<? extends List<T>> partition(Iterable<T> items, int size) {
        if (items instanceof Collection<T> collection) {
            // use Lists.partition if possible, which will return sublist without allocating entire
            // array of partition size.
            if (collection.isEmpty()) {
                return ImmutableList.of();
            }
            if (collection instanceof ImmutableCollection<T> immutableCollection) {
                // immutable collections have an internal list that can be partitioned without allocating sublists
                return Lists.partition(immutableCollection.asList(), size);
            }
            if (collection instanceof List<T> list) {
                return Lists.partition(Collections.unmodifiableList(list), size);
            }
            if (collection.size() <= size) {
                // Iterables.partition pre-allocates array of `size` waste when `items.size()` does not need partitioned
                return ImmutableList.of(Collections.unmodifiableList(new ArrayList<>(collection)));
            }
        }
        return Iterables.partition(items, size);
    }
}
