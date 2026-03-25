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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

// The benchmarks demonstrate that:
//  1. forEachPartition and partition.forEach allocate roughly equal memory for list and immutable collection types
//  2. forEachPartition allocates less memory than partition.forEach for other collection types, such as HashSet
//  3. forEachPartition space complexity is O(min(partitionSize, size)) compared to O(size) for partition.forEach
//
// gc.alloc.rate.norm (B/op), partitionSize=1000
//
// size=10000
// Collection Type                  forEachPartition    partition.forEach
// ---------------------------------------------------------------------
// ARRAY_LIST                             13,678              13,598
// IMMUTABLE_LIST                         10,318               9,918
// IMMUTABLE_SET                          10,336               9,936
// IMMUTABLE_MAP_ENTRIES                  10,318               9,918
// IMMUTABLE_SET_MULTIMAP_ENTRIES      5,211,357           5,210,954
// HASH_SET                               48,307             414,147
// TREE_SET                               48,195             414,147
// HASH_MAP_ENTRIES                       48,307             414,147
// CUSTOM_ITERABLE                        48,307             414,147
//
// size=50000
// Collection Type                  forEachPartition    partition.forEach
// ---------------------------------------------------------------------
// ARRAY_LIST                             36,067              36,037
// IMMUTABLE_LIST                         19,907              19,606
// IMMUTABLE_SET                          20,045              19,546
// IMMUTABLE_MAP_ENTRIES                  20,059              19,610
// IMMUTABLE_SET_MULTIMAP_ENTRIES     26,020,782          26,020,333
// HASH_SET                               47,915           2,039,747
// TREE_SET                               47,907           2,039,747
// HASH_MAP_ENTRIES                       47,768           2,039,747
// CUSTOM_ITERABLE                        48,211           2,039,747
@State(Scope.Benchmark)
@Fork(value = 2)
@Warmup(iterations = 1, batchSize = 10)
@Measurement(iterations = 5, batchSize = 10)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class MoreIterablesBenchmark {

    public enum IterableType {
        ARRAY_LIST,
        IMMUTABLE_LIST,
        IMMUTABLE_SET,
        IMMUTABLE_MAP_ENTRIES,
        IMMUTABLE_SET_MULTIMAP_ENTRIES,
        HASH_SET,
        TREE_SET,
        HASH_MAP_ENTRIES,
        CUSTOM_ITERABLE
    }

    @Param({"10000", "50000"})
    private int size;

    @Param("1000")
    private int partitionSize;

    @Param
    private IterableType iterableType;

    private Iterable<?> iterable;

    @Setup
    public final void before() {
        List<Integer> elements = new ArrayList<>();
        for (int i = 0; i < size; ++i) {
            elements.add(i);
        }

        iterable = switch (iterableType) {
            case ARRAY_LIST -> new ArrayList<>(elements);
            case IMMUTABLE_LIST -> ImmutableList.copyOf(elements);
            case IMMUTABLE_SET -> ImmutableSet.copyOf(elements);
            case IMMUTABLE_MAP_ENTRIES ->
                ImmutableMap.copyOf(
                                elements.stream().collect(Collectors.toMap(Function.identity(), Function.identity())))
                        .entrySet();
            case IMMUTABLE_SET_MULTIMAP_ENTRIES ->
                elements.stream()
                        .collect(ImmutableSetMultimap.toImmutableSetMultimap(Function.identity(), Function.identity()))
                        .entries();
            case HASH_SET -> new HashSet<>(elements);
            case TREE_SET -> new TreeSet<>(elements);
            case HASH_MAP_ENTRIES ->
                new HashMap<>(elements.stream().collect(Collectors.toMap(Function.identity(), Function.identity())))
                        .entrySet();
            case CUSTOM_ITERABLE -> (Iterable<Integer>) elements::iterator;
        };
    }

    @Benchmark
    public final void forEachPartition(Blackhole blackhole) {
        MoreIterables.forEachPartition(iterable, partitionSize, blackhole::consume);
    }

    @Benchmark
    public final void partitionForEach(Blackhole blackhole) {
        MoreIterables.partition(iterable, partitionSize).forEach(blackhole::consume);
    }
}
