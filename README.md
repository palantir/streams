<p align="right">
<a href="https://autorelease.general.dmz.palantir.tech/palantir/streams"><img src="https://img.shields.io/badge/Perform%20an-Autorelease-success.svg" alt="Autorelease"></a>
</p>

# streams
Utilities for working with [Java 8 streams][Stream]. [KeyedStream](#keyedstream) makes
working with streams of Map entries readable.

[![CircleCI Build Status](https://circleci.com/gh/palantir/streams/tree/develop.svg)](https://circleci.com/gh/palantir/streams)
[![Download](https://img.shields.io/maven-central/v/com.palantir.common/streams) ](https://central.sonatype.com/artifact/com.palantir.common/streams/)


## KeyedStream

*Consider using [streamex's EntryStream] instead of this class.*

[streamex's EntryStream]: http://amaembo.github.io/streamex/javadoc/one/util/streamex/EntryStream.html

    import com.palantir.common.streams.KeyedStream;

A `KeyedStream<K, V>` is syntactic sugar around a `Stream<Map.Entry<K, V>>`, with methods for filtering/updating keys,
values and whole entries. You can create a KeyedStream from a [Map], or from a [Stream] or [Iterable], giving initially
identical keys and values.

    KeyedStream.of(Stream.of(1, 2, 3))  // keys and values are initially identical
        .map(v -> v * 2)                // keys remain unchanged, values are doubled
        .mapKeys(Integer::toString)     // values remain unchanged
        .collectToMap();                // returns a Map<String, Integer>

Each map function also accepts a [BiFunction], making it easy to modify keys based on values, and vice versa:

    KeyedStream.stream(map)
        .map((k, v) -> new FooType(k, v))  // keys remain unchanged
        .collectToMap();

## MoreStreams

Utility methods for streams. Currently supported are `inCompletionOrder` and `blockingStreamWithParallelism`.
It is tricky to handle streams of futures. Running

    foos.stream().map(executorService::submit).map(Futures::getUnchecked).collect(toList());

will only execute one task at a time, losing the benefit of concurrency.

On the other hand, collecting to a [List] in the meantime can lead to other issues - not only it is inconvenient to
stream twice, but there are plenty of issues that can appear, especially if the returned objects are large - there is
no back-pressure mechanism.

Instead, when a [ListenableFuture] can be returned by a function, consider calling

    MoreStreams.inCompletionOrder(foos.stream().map(service::getBarAsync), maxParallelism)
        .map(Futures::getUnchecked)
        .collect(toList());

which will provide a new stream which looks ahead up to `maxParallelism` items in the provided stream of Guava
futures, ensuring that only `maxParallelism` futures exist at any one time.

In some cases, it may not be beneficial to push down the async computation. Here, one can provide their own
executor, calling

    MoreStreams.inCompletionOrder(foos.stream(), service::getBar, executor, maxParallelism).collect(toList());

The difference between `inCompletionOrder` and `blockingStreamWithParallelism` is that the `blockStreamWithParallelism`
methods keep the futures in the order they were provided, whilst the `inCompletionOrder` methods return futures in
the order in which they complete.

[BiFunction]: https://docs.oracle.com/javase/8/docs/api/java/util/function/BiFunction.html
[Iterable]: https://docs.oracle.com/javase/8/docs/api/java/lang/Iterable.html
[List]: https://docs.oracle.com/javase/8/docs/api/java/util/List.html
[ListenableFuture]: https://google.github.io/guava/releases/23.0/api/docs/com/google/common/util/concurrent/ListenableFuture.html
[Map]: https://docs.oracle.com/javase/8/docs/api/java/util/Map.html
[Stream]: https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html

## MoreCollectors

### `MoreCollectors.toImmutableMap()`

Collect a Stream of Map.Entry (e.g. a StreamEx EntryStream) into a Guava ImmutableMap, which preserves iteration order.

Beware that using StreamEx's `EntryStream#toImmutableMap()` does NOT preserve iteration order, as it uses a regular HashMap under the hood.

```diff
 StreamEx.of(items)
        .mapToEntry(
                key -> computeValue(key.foo())
-       .toImmutableMap() // does not preserve iteration order
+       .collect(MoreCollectors.toImmutableMap()); // preserves iteration order
```

This is equivalent to writing out the slightly more verbose:

```java
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
```
