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

import static com.palantir.logsafe.Preconditions.checkArgument;
import static com.palantir.logsafe.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.palantir.logsafe.SafeArg;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import org.jspecify.annotations.NonNull;
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
    public static <T extends @Nullable Object> Iterable<List<T>> partition(Iterable<T> items, int size) {
        if (items instanceof Collection<T> collection) {
            // use Lists.partition if possible, which will return sublist without allocating entire
            // array of partition size.
            if (collection.isEmpty()) {
                return ImmutableList.of();
            }
            if (collection instanceof ImmutableCollection<@NonNull T> immutableCollection) {
                // immutable collections have an internal list that can be partitioned without allocating sublists
                return Lists.partition(immutableCollection.asList(), size);
            }
            if (collection instanceof List<T> list) {
                return Lists.transform(Lists.partition(list, size), Collections::unmodifiableList);
            }
            if (collection.size() <= size) {
                // Iterables.partition pre-allocates array of `size` waste when `items.size()` does not need partitioned
                return ImmutableList.of(Collections.unmodifiableList(new ArrayList<>(collection)));
            }
        }
        return Iterables.partition(items, size);
    }

    /**
     * Divides an iterable into unmodifiable sublists of the given size (the final sublist may be smaller) and passes
     * each sublist to the given consumer. For example, partitioning an iterable containing {@code [a, b, c, d, e]} with
     * a partition size of 3 invokes the consumer twice, once on {@code [a, b, c]} and once on {@code [d, e]}. All
     * elements remain in the original order. The consumer is never invoked if the iterable is empty.
     * <p>
     * Unlike {@link MoreIterables#partition(Iterable, int)}, each sublist must be processed independently, and
     * references to sublists must not be captured outside the consumer. For non-list iterables,
     * {@link #partition(Iterable, int)} allocates a new list per sublist while this implementation allocates only
     * enough storage for one sublist by reusing a single backing array across sublists. Prefer this method if sublists
     * are processed independently.
     *
     * @param items the iterable to partition and consume
     * @param size the desired size of each sublist (the last may be smaller)
     * @param consumer the consumer of each sublist
     */
    public static <T extends @Nullable Object> void forEachPartition(
            Iterable<T> items, int size, Consumer<List<T>> consumer) {
        checkNotNull(items, "items must not be null");
        checkNotNull(consumer, "consumer must not be null");
        checkArgument(size > 0, "size must be greater than zero", SafeArg.of("size", size));

        if (items instanceof ImmutableCollection<@NonNull T> immutableCollection) {
            // Many immutable collections have an internal list that can leverage Lists.partition.
            Lists.partition(immutableCollection.asList(), size).forEach(consumer);
            return;
        }
        if (items instanceof List<T> list) {
            // Lists.partition creates sublist views without copying elements.
            Lists.partition(list, size).forEach(partition -> consumer.accept(Collections.unmodifiableList(partition)));
            return;
        }

        Iterator<T> iterator = items.iterator();
        if (!iterator.hasNext()) {
            return;
        }

        // Avoid over-allocation when the iterable (collection) size is less than the partition size.
        int arraySize = (items instanceof Collection<T> collection) ? Math.min(size, collection.size()) : size;

        @SuppressWarnings("unchecked") // We put only Ts in the array.
        T[] array = (T[]) new Object[arraySize];

        List<T> partition = Collections.unmodifiableList(Arrays.asList(array));
        do { // We already confirmed that the iterator has an item.
            int count = 0;
            for (; count < size && iterator.hasNext(); ++count) {
                array[count] = iterator.next();
            }
            consumer.accept(count == size ? partition : partition.subList(0, count));
        } while (iterator.hasNext());
    }
}
