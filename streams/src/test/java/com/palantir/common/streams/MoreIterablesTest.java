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
import static org.assertj.core.api.AssertionsForClassTypes.fail;
import static org.assertj.core.api.InstanceOfAssertFactories.iterable;
import static org.assertj.core.api.InstanceOfAssertFactories.list;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableSet;
import java.util.RandomAccess;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;
import org.jspecify.annotations.Nullable;
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

        @Test
        void shouldNotInvokeConsumerForEmptyList() {
            MoreIterables.forEachPartition(new ArrayList<>(), 3, _partition -> fail());
            MoreIterables.forEachPartition(List.of(), 3, _partition -> fail());
            MoreIterables.forEachPartition(ImmutableList.of(), 3, _partition -> fail());
            MoreIterables.forEachPartition(Iterables.concat(ImmutableList.of(), List.of()), 3, _partition -> fail());
        }

        @Test
        void shouldNotInvokeConsumerForEmptySet() {
            MoreIterables.forEachPartition(new HashSet<>(), 3, _partition -> fail());
            MoreIterables.forEachPartition(Set.of(), 3, _partition -> fail());
            MoreIterables.forEachPartition(ImmutableSet.of(), 3, _partition -> fail());
            MoreIterables.forEachPartition(Iterables.concat(ImmutableSet.of(), Set.of()), 3, _partition -> fail());
        }
    }

    @Nested
    class ListPartitionTests {

        @Test
        void shouldPartitionListIntoEvenChunks() {
            List<Integer> list = List.of(1, 2, 3, 4, 5, 6);

            assertThat(MoreIterables.partition(list, 2))
                    .hasSize(3)
                    .containsExactly(List.of(1, 2), List.of(3, 4), List.of(5, 6));
        }

        @Test
        void shouldPartitionListWithRemainder() {
            List<String> list = List.of("a", "b", "c", "d", "e");

            assertThat(MoreIterables.partition(list, 3))
                    .hasSize(2)
                    .containsExactly(List.of("a", "b", "c"), List.of("d", "e"));
        }

        @Test
        void shouldHandleListSmallerThanPartitionSize() {
            List<Integer> list = List.of(1, 2);

            assertThat(MoreIterables.partition(list, 5)).hasSize(1).containsExactly(List.of(1, 2));
        }

        @Test
        void shouldHandleListEqualToPartitionSize() {
            assertThat(MoreIterables.partition(List.of("a", "b", "c"), 3))
                    .hasSize(1)
                    .containsExactly(List.of("a", "b", "c"));
        }

        @Test
        void shouldHandleSingleElementList() {
            assertThat(MoreIterables.partition(List.of(42), 1)).hasSize(1).containsExactly(List.of(42));
        }

        @Test
        @SuppressWarnings("JdkObsolete") // explicitly testing LinkedList
        void shouldHandleLinkedList() {
            assertThat(MoreIterables.partition(new LinkedList<>(List.of("x", "y", "z", "w")), 2))
                    .hasSize(2)
                    .containsExactly(List.of("x", "y"), List.of("z", "w"));
        }

        @Test
        void shouldConsumeListInEvenChunks() {
            List<List<Integer>> partitions = new ArrayList<>();
            MoreIterables.forEachPartition(
                    List.of(1, 2, 3, 4, 5, 6), 2, partition -> partitions.add(List.copyOf(partition)));

            assertThat(partitions).hasSize(3).containsExactly(List.of(1, 2), List.of(3, 4), List.of(5, 6));
        }

        @Test
        void shouldConsumeListWithRemainder() {
            List<List<String>> partitions = new ArrayList<>();
            MoreIterables.forEachPartition(
                    List.of("a", "b", "c", "d", "e"), 3, partition -> partitions.add(List.copyOf(partition)));

            assertThat(partitions).hasSize(2).containsExactly(List.of("a", "b", "c"), List.of("d", "e"));
        }

        @Test
        void shouldConsumeListSmallerThanPartitionSize() {
            List<List<Integer>> partitions = new ArrayList<>();
            MoreIterables.forEachPartition(List.of(1, 2), 5, partition -> partitions.add(List.copyOf(partition)));

            assertThat(partitions).hasSize(1).containsExactly(List.of(1, 2));
        }

        @Test
        void shouldConsumeListEqualToPartitionSize() {
            List<List<String>> partitions = new ArrayList<>();
            MoreIterables.forEachPartition(
                    List.of("a", "b", "c"), 3, partition -> partitions.add(List.copyOf(partition)));

            assertThat(partitions).hasSize(1).containsExactly(List.of("a", "b", "c"));
        }
    }

    @Nested
    class ImmutableCollectionTests {

        @Test
        void shouldPartitionImmutableList() {
            assertThat(MoreIterables.partition(ImmutableList.of(1, 2, 3, 4, 5), 2))
                    .hasSize(3)
                    .containsExactly(List.of(1, 2), List.of(3, 4), List.of(5));
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
                    .containsExactly(List.of(1, 2));
        }

        @Test
        void shouldConsumeImmutableList() {
            List<List<Integer>> partitions = new ArrayList<>();
            MoreIterables.forEachPartition(
                    ImmutableList.of(1, 2, 3, 4, 5), 2, partition -> partitions.add(List.copyOf(partition)));

            assertThat(partitions).hasSize(3).containsExactly(List.of(1, 2), List.of(3, 4), List.of(5));
        }

        @Test
        void shouldConsumeImmutableSet() {
            List<List<String>> partitions = new ArrayList<>();
            MoreIterables.forEachPartition(
                    ImmutableSet.of("a", "b", "c", "d"), 3, partition -> partitions.add(List.copyOf(partition)));

            assertThat(partitions)
                    .hasSize(2)
                    .allSatisfy(partition -> assertThat(partition)
                            .asInstanceOf(list(String.class))
                            .isSubsetOf("a", "b", "c", "d"));
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
                            .satisfies(ints -> {
                                assertThat(ints).hasSizeLessThanOrEqualTo(partitionSize);
                                allElements.addAll(ints);
                            }));
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
                            .satisfies(strings -> {
                                assertThat(strings).hasSizeLessThanOrEqualTo(partitionSize);
                                allElements.addAll(strings);
                            }));
            assertThat(allElements).containsExactlyInAnyOrderElementsOf(set);
        }

        @Test
        void shouldHandleSetEqualToPartitionSize() {
            Set<Integer> set = new HashSet<>(List.of(1, 2, 3));

            assertThat(MoreIterables.partition(set, 3))
                    .hasSize(1)
                    .allSatisfy(partition -> assertThat(partition)
                            .asInstanceOf(list(Integer.class))
                            .containsExactlyInAnyOrderElementsOf(set));
        }

        @Test
        void shouldConsumeHashSetLargerThanPartitionSize() {
            Set<Integer> set = new HashSet<>(List.of(1, 2, 3, 4, 5, 6, 7));
            int partitionSize = 3;

            List<List<Integer>> partitions = new ArrayList<>();
            MoreIterables.forEachPartition(set, partitionSize, partition -> partitions.add(List.copyOf(partition)));

            assertThat(partitions)
                    .hasSize(3)
                    .allSatisfy(partition -> assertThat(partition).hasSizeLessThanOrEqualTo(partitionSize));
            assertThat(partitions.stream().flatMap(List::stream).toList()).containsExactlyInAnyOrderElementsOf(set);
        }

        @Test
        void shouldConsumeHashSetSmallerThanPartitionSize() {
            Set<String> set = new HashSet<>(List.of("a", "b"));
            int partitionSize = 5;

            List<List<String>> partitions = new ArrayList<>();
            MoreIterables.forEachPartition(set, partitionSize, partition -> partitions.add(List.copyOf(partition)));

            assertThat(partitions).hasSize(1);
            assertThat(partitions.stream().flatMap(List::stream).toList()).containsExactlyInAnyOrderElementsOf(set);
        }

        @Test
        void shouldConsumeHashSetEqualToPartitionSize() {
            Set<Integer> set = new HashSet<>(List.of(1, 2, 3));
            int partitionSize = 3;

            List<List<Integer>> partitions = new ArrayList<>();
            MoreIterables.forEachPartition(set, partitionSize, partition -> partitions.add(List.copyOf(partition)));

            assertThat(partitions).hasSize(1);
            assertThat(partitions.get(0)).containsExactlyInAnyOrderElementsOf(set);
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
            assertThat(allElements).containsExactlyElementsOf(list).containsExactlyElementsOf(list);
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
            assertThat(allElements).containsExactlyElementsOf(list).containsExactlyElementsOf(list);
        }

        @Test
        @SuppressWarnings({"RedundantMethodReference", "UnnecessaryMethodReference"}) // explicitly testing
        void shouldConsumeCustomIterable() {
            List<String> list = List.of("a", "b", "c", "d", "e");
            int partitionSize = 2;

            List<List<String>> partitions = new ArrayList<>();
            MoreIterables.forEachPartition(
                    list::iterator, partitionSize, partition -> partitions.add(List.copyOf(partition)));

            assertThat(partitions)
                    .hasSize(3)
                    .allSatisfy(partition -> assertThat(partition).hasSizeLessThanOrEqualTo(partitionSize));
            assertThat(partitions.stream().flatMap(List::stream).toList()).containsExactlyElementsOf(list);
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

        @Test
        void shouldConsumeUnmodifiableSublistsForList() {
            MoreIterables.forEachPartition(new ArrayList<>(List.of(1, 2, 3)), 2, partition -> {
                assertThatThrownBy(() -> partition.add(99)).isInstanceOf(UnsupportedOperationException.class);
            });
        }

        @Test
        void shouldConsumeUnmodifiableSublistsForHashSet() {
            MoreIterables.forEachPartition(new HashSet<>(List.of(1, 2, 3)), 2, partition -> {
                assertThatThrownBy(() -> partition.add(99)).isInstanceOf(UnsupportedOperationException.class);
            });
        }
    }

    @Nested
    class EdgeCaseTests {

        @Test
        void shouldHandlePartitionSizeOfOne() {
            assertThat(MoreIterables.partition(List.of(1, 2, 3), 1))
                    .hasSize(3)
                    .containsExactly(List.of(1), List.of(2), List.of(3));
        }

        @Test
        void shouldHandleLargePartitionSize() {
            List<String> list = List.of("a", "b", "c");

            assertThat(MoreIterables.partition(list, 1000)).hasSize(1).containsExactly(List.of("a", "b", "c"));
        }

        @Test
        void shouldHandleNullElements() {
            assertThat(MoreIterables.partition(Arrays.asList("a", null, "c", null, "e"), 2))
                    .hasSize(3)
                    .containsExactly(Arrays.asList("a", null), Arrays.asList("c", null), List.of("e"));
        }

        @Test
        void shouldConsumePartitionSizeOfOne() {
            List<List<Integer>> partitions = new ArrayList<>();
            MoreIterables.forEachPartition(List.of(1, 2, 3), 1, partition -> partitions.add(List.copyOf(partition)));

            assertThat(partitions).hasSize(3).containsExactly(List.of(1), List.of(2), List.of(3));
        }

        @Test
        void shouldConsumeLargePartitionSize() {
            List<List<String>> partitions = new ArrayList<>();
            MoreIterables.forEachPartition(
                    List.of("a", "b", "c"), 1000, partition -> partitions.add(List.copyOf(partition)));

            assertThat(partitions).hasSize(1).containsExactly(List.of("a", "b", "c"));
        }

        @Test
        void shouldConsumeNullElements() {
            List<@Nullable String> listWithNullElements = Arrays.asList("a", null, "c", null, "e");
            List<List<@Nullable String>> partitions = new ArrayList<>();
            MoreIterables.forEachPartition(
                    listWithNullElements, 2, partition -> partitions.add(new ArrayList<>(partition)));

            assertThat(partitions)
                    .hasSize(3)
                    .containsExactly(Arrays.asList("a", null), Arrays.asList("c", null), List.of("e"));
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

            assertThat(firstIteration).containsExactlyElementsOf(secondIteration);
        }
    }

    @Nested
    class InvalidInputTests {

        @Test
        void shouldThrowExceptionForZeroPartitionSize() {
            assertThatThrownBy(() -> MoreIterables.partition(List.of(1, 2, 3), 0))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void shouldThrowExceptionForNegativePartitionSize() {
            assertThatThrownBy(() -> MoreIterables.partition(List.of(1, 2, 3), -1))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void shouldThrowExceptionForNullIterable() {
            assertThatThrownBy(() -> MoreIterables.partition(null, 3)).isInstanceOf(NullPointerException.class);
        }

        @Test
        void forEachShouldThrowExceptionForZeroPartitionSize() {
            assertThatThrownBy(() -> MoreIterables.forEachPartition(Set.of(1, 2, 3), 0, _partition -> fail()))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void forEachShouldThrowExceptionForNegativePartitionSize() {
            assertThatThrownBy(() -> MoreIterables.forEachPartition(Set.of(1, 2, 3), -1, _partition -> fail()))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void forEachShouldThrowExceptionForNullIterable() {
            assertThatThrownBy(() -> MoreIterables.forEachPartition(null, 1, _partition -> fail()))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void forEachShouldThrowExceptionForNullConsumer() {
            assertThatThrownBy(() -> MoreIterables.forEachPartition(Set.of(1, 2, 3), 1, null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    class RandomAccessTests {

        @Test
        void shouldReturnRandomAccessListsForArrayList() {
            List<Integer> arrayList = new ArrayList<>(List.of(1, 2, 3, 4, 5));

            assertThat(MoreIterables.partition(arrayList, 2))
                    .allSatisfy(partition -> assertThat(partition).isInstanceOf(RandomAccess.class));
        }

        @Test
        @SuppressWarnings("JdkObsolete") // explicitly testing Vector
        void shouldReturnRandomAccessListsForVector() {
            Vector<String> vector = new Vector<>(List.of("a", "b", "c", "d"));

            assertThat(MoreIterables.partition(vector, 2))
                    .allSatisfy(partition -> assertThat(partition).isInstanceOf(RandomAccess.class));
        }

        @Test
        @SuppressWarnings("JdkObsolete") // explicitly testing LinkedList
        void shouldNotReturnRandomAccessListsForLinkedList() {
            List<Integer> linkedList = new LinkedList<>(List.of(1, 2, 3, 4));

            // LinkedList doesn't implement RandomAccess, so partitions shouldn't either
            assertThat(MoreIterables.partition(linkedList, 2))
                    .allSatisfy(partition -> assertThat(partition).isNotInstanceOf(RandomAccess.class));
        }

        @Test
        void shouldReturnRandomAccessListsForImmutableList() {
            ImmutableList<Integer> immutableList = ImmutableList.of(1, 2, 3, 4, 5, 6);

            assertThat(MoreIterables.partition(immutableList, 2))
                    .allSatisfy(partition -> assertThat(partition).isInstanceOf(RandomAccess.class));
        }
    }

    @Nested
    class AdditionalCollectionTypeTests {

        @Test
        void shouldPartitionTreeSet() {
            NavigableSet<Integer> treeSet = new TreeSet<>(List.of(5, 2, 8, 1, 9, 3, 7));
            int partitionSize = 3;

            List<Integer> allElements = new ArrayList<>();
            assertThat(MoreIterables.partition(treeSet, partitionSize))
                    .hasSize(3)
                    .asInstanceOf(iterable(Object.class))
                    .allSatisfy(batch -> assertThat(batch)
                            .asInstanceOf(list(Integer.class))
                            .hasSizeLessThanOrEqualTo(partitionSize)
                            .satisfies(allElements::addAll));

            // TreeSet maintains sorted order
            assertThat(allElements).containsExactly(1, 2, 3, 5, 7, 8, 9);
        }

        @Test
        void shouldPartitionArrayDeque() {
            ArrayDeque<String> deque = new ArrayDeque<>(List.of("a", "b", "c", "d", "e"));
            int partitionSize = 2;

            List<String> allElements = new ArrayList<>();
            assertThat(MoreIterables.partition(deque, partitionSize))
                    .hasSize(3)
                    .asInstanceOf(iterable(Object.class))
                    .allSatisfy(batch -> assertThat(batch)
                            .asInstanceOf(list(String.class))
                            .hasSizeLessThanOrEqualTo(partitionSize)
                            .satisfies(allElements::addAll));

            assertThat(allElements).containsExactlyElementsOf(deque);
        }

        @Test
        @SuppressWarnings("JdkObsolete") // explicitly testing Vector
        void shouldPartitionVector() {
            Vector<Integer> vector = new Vector<>(List.of(10, 20, 30, 40, 50, 60, 70));

            assertThat(MoreIterables.partition(vector, 3))
                    .hasSize(3)
                    .containsExactly(List.of(10, 20, 30), List.of(40, 50, 60), List.of(70));
        }

        @Test
        void shouldPartitionSmallTreeSet() {
            NavigableSet<String> treeSet = new TreeSet<>(List.of("b", "a"));

            assertThat(MoreIterables.partition(treeSet, 5))
                    .hasSize(1)
                    .allSatisfy(partition -> assertThat(partition)
                            .asInstanceOf(list(String.class))
                            .containsExactly("a", "b"));
        }

        @Test
        void shouldPartitionArrayDequeSmallerThanPartitionSize() {
            ArrayDeque<Integer> deque = new ArrayDeque<>(List.of(1, 2));

            assertThat(MoreIterables.partition(deque, 10))
                    .hasSize(1)
                    .allSatisfy(partition -> assertThat(partition)
                            .asInstanceOf(list(Integer.class))
                            .containsExactly(1, 2));
        }

        @Test
        void shouldConsumeTreeSet() {
            NavigableSet<Integer> treeSet = new TreeSet<>(List.of(5, 2, 8, 1, 9, 3, 7));
            int partitionSize = 3;

            List<List<Integer>> partitions = new ArrayList<>();
            MoreIterables.forEachPartition(treeSet, partitionSize, partition -> partitions.add(List.copyOf(partition)));

            assertThat(partitions)
                    .hasSize(3)
                    .allSatisfy(partition -> assertThat(partition).hasSizeLessThanOrEqualTo(partitionSize));
            // TreeSet maintains sorted order
            assertThat(partitions.stream().flatMap(List::stream).toList()).containsExactly(1, 2, 3, 5, 7, 8, 9);
        }

        @Test
        void shouldConsumeArrayDeque() {
            ArrayDeque<String> deque = new ArrayDeque<>(List.of("a", "b", "c", "d", "e"));
            int partitionSize = 2;

            List<List<String>> partitions = new ArrayList<>();
            MoreIterables.forEachPartition(deque, partitionSize, partition -> partitions.add(List.copyOf(partition)));

            assertThat(partitions)
                    .hasSize(3)
                    .allSatisfy(partition -> assertThat(partition).hasSizeLessThanOrEqualTo(partitionSize));
            assertThat(partitions.stream().flatMap(List::stream).toList()).containsExactlyElementsOf(deque);
        }
    }

    @Nested
    class LargerDatasetTests {

        @Test
        void shouldPartitionLargeList() {
            List<Integer> largeList = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                largeList.add(i);
            }

            Iterable<? extends List<Integer>> partitions = MoreIterables.partition(largeList, 100);

            assertThat(partitions).hasSize(10);

            int index = 0;
            for (List<Integer> partition : partitions) {
                assertThat(partition).hasSize(100);
                for (int value : partition) {
                    assertThat(value).isEqualTo(index++);
                }
            }
        }

        @Test
        void shouldPartitionLargeListWithRemainder() {
            List<String> largeList = new ArrayList<>();
            for (int i = 0; i < 1547; i++) {
                largeList.add("item-" + i);
            }

            Iterable<? extends List<String>> partitions = MoreIterables.partition(largeList, 200);

            assertThat(partitions).hasSize(8);

            List<String> allElements = new ArrayList<>();
            partitions.forEach(allElements::addAll);

            assertThat(allElements).containsExactlyElementsOf(largeList);
        }

        @Test
        void shouldPartitionLargeImmutableList() {
            ImmutableList.Builder<Integer> builder = ImmutableList.builder();
            for (int i = 0; i < 500; i++) {
                builder.add(i);
            }
            ImmutableList<Integer> immutableList = builder.build();

            assertThat(MoreIterables.partition(immutableList, 50)).hasSize(10);
        }

        @Test
        void shouldPartitionLargeHashSet() {
            Set<Integer> largeSet = new HashSet<>();
            for (int i = 0; i < 750; i++) {
                largeSet.add(i);
            }

            Iterable<? extends List<Integer>> partitions = MoreIterables.partition(largeSet, 100);

            List<Integer> allElements = new ArrayList<>();
            partitions.forEach(allElements::addAll);

            assertThat(allElements).containsExactlyInAnyOrderElementsOf(largeSet);
        }

        @Test
        void shouldConsumeLargeList() {
            List<Integer> largeList = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                largeList.add(i);
            }

            List<List<Integer>> partitions = new ArrayList<>();
            MoreIterables.forEachPartition(largeList, 100, partition -> partitions.add(List.copyOf(partition)));

            assertThat(partitions).hasSize(10);
            int index = 0;
            for (List<Integer> partition : partitions) {
                assertThat(partition).hasSize(100);
                for (int value : partition) {
                    assertThat(value).isEqualTo(index++);
                }
            }
        }

        @Test
        void shouldConsumeLargeImmutableList() {
            ImmutableList.Builder<Integer> builder = ImmutableList.builder();
            for (int i = 0; i < 1000; i++) {
                builder.add(i);
            }
            ImmutableList<Integer> largeImmutableList = builder.build();

            List<List<Integer>> partitions = new ArrayList<>();
            MoreIterables.forEachPartition(
                    largeImmutableList, 100, partition -> partitions.add(List.copyOf(partition)));

            assertThat(partitions).hasSize(10);
            int index = 0;
            for (List<Integer> partition : partitions) {
                assertThat(partition).hasSize(100);
                for (int value : partition) {
                    assertThat(value).isEqualTo(index++);
                }
            }
        }

        @Test
        void shouldConsumeLargeHashSet() {
            Set<Integer> largeSet = new HashSet<>();
            for (int i = 0; i < 1000; i++) {
                largeSet.add(i);
            }

            List<List<Integer>> partitions = new ArrayList<>();
            MoreIterables.forEachPartition(largeSet, 100, partition -> partitions.add(List.copyOf(partition)));

            assertThat(partitions)
                    .hasSize(10)
                    .allSatisfy(partition -> assertThat(partition).hasSize(100));
            assertThat(partitions.stream().flatMap(List::stream).toList())
                    .containsExactlyInAnyOrderElementsOf(largeSet);
        }
    }

    @Nested
    class SpecificCodePathTests {

        @Test
        void shouldUseArrayListPathForNonListCollection() {
            // This specifically tests the collection.size() <= size path for non-list collections
            // which creates an ArrayList from the collection
            Set<String> smallSet = new HashSet<>(List.of("x", "y", "z"));

            Iterable<? extends List<String>> result = MoreIterables.partition(smallSet, 10);

            assertThat(result).hasSize(1);
            List<String> partition = result.iterator().next();
            assertThat(partition).containsExactlyInAnyOrderElementsOf(smallSet);
            assertThatThrownBy(() -> partition.add("new")).isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        void shouldUseFallbackPathForNonCollectionIterable() {
            // Custom iterable that is not a Collection
            Iterable<Integer> customIterable = () -> List.of(1, 2, 3, 4, 5).iterator();

            List<Integer> allElements = new ArrayList<>();
            assertThat(MoreIterables.partition(customIterable, 2))
                    .hasSize(3)
                    .allSatisfy(batch -> assertThat(batch)
                            .asInstanceOf(list(Integer.class))
                            .satisfies(ints -> {
                                assertThat(ints).hasSizeBetween(1, 2);
                                allElements.addAll(ints);
                            }));
            assertThat(allElements).containsExactly(1, 2, 3, 4, 5);
        }

        @Test
        void shouldHandleEmptyIteratorCorrectly() {
            Iterable<? extends List<String>> emptyPartition = MoreIterables.partition(List.of(), 5);

            Iterator<? extends List<String>> iterator = emptyPartition.iterator();
            assertThat(iterator.hasNext()).isFalse();
        }
    }
}
