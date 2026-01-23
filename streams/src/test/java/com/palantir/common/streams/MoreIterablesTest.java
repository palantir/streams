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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.iterable;
import static org.assertj.core.api.InstanceOfAssertFactories.list;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class MoreIterablesTest {

    @Nested
    class EmptyCollectionTests {

        @Test
        void shouldReturnEmptyIterableForEmptyList() {
            assertThat(MoreIterables.partition(new ArrayList<String>(), 3)).isEmpty();
            assertThat(MoreIterables.partition(List.of(), 3)).isEmpty();
            assertThat(MoreIterables.partition(ImmutableList.of(), 3)).isEmpty();
            assertThat(MoreIterables.partition(Iterables.concat(ImmutableList.of(), List.of()), 3))
                    .isEmpty();
        }

        @Test
        void shouldReturnEmptyIterableForEmptySet() {
            assertThat(MoreIterables.partition(new HashSet<String>(), 5)).isEmpty();
            assertThat(MoreIterables.partition(Set.of(), 5)).isEmpty();
            assertThat(MoreIterables.partition(ImmutableSet.of(), 5)).isEmpty();
            assertThat(MoreIterables.partition(Iterables.concat(ImmutableSet.of(), Set.of()), 3))
                    .isEmpty();
        }
    }

    @Nested
    class ListPartitionTests {

        @Test
        void shouldPartitionListIntoEvenChunks() {
            List<Integer> list = List.of(1, 2, 3, 4, 5, 6);

            assertThat(MoreIterables.partition(list, 2))
                    .hasSize(3)
                    .isEqualTo(List.of(List.of(1, 2), List.of(3, 4), List.of(5, 6)));
        }

        @Test
        void shouldPartitionListWithRemainder() {
            List<String> list = List.of("a", "b", "c", "d", "e");

            assertThat(MoreIterables.partition(list, 3))
                    .hasSize(2)
                    .isEqualTo(List.of(List.of("a", "b", "c"), List.of("d", "e")));
        }

        @Test
        void shouldHandleListSmallerThanPartitionSize() {
            List<Integer> list = List.of(1, 2);

            assertThat(MoreIterables.partition(list, 5)).hasSize(1).isEqualTo(List.of(List.of(1, 2)));
        }

        @Test
        void shouldHandleListEqualToPartitionSize() {
            assertThat(MoreIterables.partition(List.of("a", "b", "c"), 3))
                    .hasSize(1)
                    .isEqualTo(List.of(List.of("a", "b", "c")));
        }

        @Test
        void shouldHandleSingleElementList() {
            assertThat(MoreIterables.partition(List.of(42), 1)).hasSize(1).isEqualTo(List.of(List.of(42)));
        }

        @Test
        @SuppressWarnings("JdkObsolete") // explicitly testing LinkedList
        void shouldHandleLinkedList() {
            assertThat(MoreIterables.partition(new LinkedList<String>(List.of("x", "y", "z", "w")), 2))
                    .hasSize(2)
                    .isEqualTo(List.of(List.of("x", "y"), List.of("z", "w")));
        }
    }

    @Nested
    class ImmutableCollectionTests {

        @Test
        void shouldPartitionImmutableList() {
            assertThat(MoreIterables.partition(ImmutableList.of(1, 2, 3, 4, 5), 2))
                    .hasSize(3)
                    .isEqualTo(List.of(List.of(1, 2), List.of(3, 4), List.of(5)));
        }

        @Test
        void shouldPartitionImmutableSet() {
            assertThat(MoreIterables.partition(ImmutableSet.of("a", "b", "c", "d"), 3))
                    .hasSize(2)
                    .allSatisfy(partition -> assertThat(partition)
                            .asInstanceOf(list(String.class))
                            .isSubsetOf("a", "b", "c", "d"));
        }

        @Test
        void shouldHandleImmutableListSmallerThanPartitionSize() {
            assertThat(MoreIterables.partition(ImmutableList.of(1, 2), 10))
                    .hasSize(1)
                    .isEqualTo(List.of(List.of(1, 2)));
        }
    }

    @Nested
    class NonListCollectionTests {

        @Test
        void shouldPartitionHashSetLargerThanPartitionSize() {
            Set<Integer> set = new HashSet<>(List.of(1, 2, 3, 4, 5, 6, 7));
            int partitionSize = 3;

            List<Integer> allElements = new ArrayList<>();
            assertThat(MoreIterables.partition(set, partitionSize))
                    .hasSize(3)
                    .asInstanceOf(iterable(Object.class))
                    .allSatisfy(batch -> assertThat(batch)
                            .asInstanceOf(list(Integer.class))
                            .hasSizeLessThanOrEqualTo(partitionSize)
                            .satisfies(allElements::addAll));

            assertThat(allElements).containsExactlyInAnyOrderElementsOf(set);
        }

        @Test
        void shouldHandleSetSmallerThanPartitionSize() {
            Set<String> set = new HashSet<>(List.of("a", "b"));
            int partitionSize = 5;

            List<String> allElements = new ArrayList<>();
            assertThat(MoreIterables.partition(set, partitionSize))
                    .hasSize(1)
                    .asInstanceOf(iterable(Object.class))
                    .allSatisfy(batch -> assertThat(batch)
                            .asInstanceOf(list(String.class))
                            .hasSizeLessThanOrEqualTo(partitionSize)
                            .satisfies(allElements::addAll));
        }

        @Test
        void shouldHandleSetEqualToPartitionSize() {
            Set<Integer> set = new HashSet<>(List.of(1, 2, 3));

            assertThat(MoreIterables.partition(set, 3)).hasSize(1).allSatisfy(partition -> assertThat(partition)
                    .asInstanceOf(list(Integer.class))
                    .containsExactlyInAnyOrderElementsOf(set));
        }
    }

    @Nested
    class NonCollectionIterableTests {

        @Test
        @SuppressWarnings({"RedundantMethodReference", "UnnecessaryMethodReference"}) // explicitly testing
        void shouldPartitionCustomIterable() {
            List<String> list = List.of("a", "b", "c", "d", "e");
            int partitionSize = 2;

            List<String> allElements = new ArrayList<>();
            assertThat(MoreIterables.partition(list::iterator, partitionSize))
                    .hasSize(3)
                    .asInstanceOf(iterable(Object.class))
                    .allSatisfy(batch -> assertThat(batch)
                            .asInstanceOf(list(String.class))
                            .hasSizeLessThanOrEqualTo(partitionSize)
                            .satisfies(allElements::addAll));
            assertThat(allElements).containsExactlyElementsOf(list).isEqualTo(list);
        }

        @Test
        @SuppressWarnings({"RedundantMethodReference", "UnnecessaryMethodReference"}) // explicitly testing
        void shouldHandleIterableThatIsNotCollection() {
            List<Integer> list = List.of(1, 2, 3);
            int partitionSize = 10;

            List<Integer> allElements = new ArrayList<>();
            assertThat(MoreIterables.partition(list::iterator, partitionSize))
                    .hasSize(1)
                    .asInstanceOf(iterable(Object.class))
                    .allSatisfy(batch -> assertThat(batch)
                            .asInstanceOf(list(Integer.class))
                            .hasSizeLessThanOrEqualTo(partitionSize)
                            .satisfies(allElements::addAll));
            assertThat(allElements).containsExactlyElementsOf(list).isEqualTo(list);
        }
    }

    @Nested
    class UnmodifiabilityTests {

        @Test
        void shouldReturnUnmodifiableSublistsForCollection() {
            List<Integer> list = new ArrayList<>(List.of(1, 2, 3));

            List<Integer> firstPartition =
                    MoreIterables.partition(list, 2).iterator().next();

            assertThatThrownBy(() -> firstPartition.add(99)).isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        void shouldReturnUnmodifiableSublistsForSmallCollection() {
            Set<String> set = new HashSet<>(List.of("a", "b"));

            List<String> partition = MoreIterables.partition(set, 5).iterator().next();

            assertThatThrownBy(partition::clear).isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Nested
    class EdgeCaseTests {

        @Test
        void shouldHandlePartitionSizeOfOne() {
            assertThat(MoreIterables.partition(List.of(1, 2, 3), 1))
                    .hasSize(3)
                    .isEqualTo(List.of(List.of(1), List.of(2), List.of(3)));
        }

        @Test
        void shouldHandleLargePartitionSize() {
            List<String> list = List.of("a", "b", "c");

            assertThat(MoreIterables.partition(list, 1000)).hasSize(1).isEqualTo(List.of(List.of("a", "b", "c")));
        }

        @Test
        void shouldHandleNullElements() {
            assertThat(MoreIterables.partition(Arrays.asList("a", null, "c", null, "e"), 2))
                    .hasSize(3)
                    .isEqualTo(List.of(Arrays.asList("a", null), Arrays.asList("c", null), List.of("e")));
        }
    }

    @Nested
    class IteratorBehaviorTests {

        @Test
        void shouldNotSupportRemoveOnIterator() {
            Iterator<? extends List<Integer>> iterator =
                    MoreIterables.partition(List.of(1, 2, 3, 4), 2).iterator();

            iterator.next();
            assertThatThrownBy(iterator::remove).isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        void shouldAllowMultipleIterations() {
            Iterable<? extends List<String>> result = MoreIterables.partition(List.of("a", "b", "c", "d"), 2);

            List<List<String>> firstIteration = new ArrayList<>();
            result.forEach(firstIteration::add);

            List<List<String>> secondIteration = new ArrayList<>();
            result.forEach(secondIteration::add);

            assertThat(firstIteration).isEqualTo(secondIteration);
        }
    }
}
