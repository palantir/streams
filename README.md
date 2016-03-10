# streams
Utilities for working with [Java 8 streams][Stream]. [MoreStreams](#morestreams) provides a few convenient static methods; [KeyedStream](#keyedstream) makes
working with streams of Map entries readable.

[![CircleCI Build Status](https://circleci.com/gh/palantir/streams/tree/master.svg)](https://circleci.com/gh/palantir/streams)
[![Download](https://api.bintray.com/packages/palantir/releases/streams/images/download.svg) ](https://bintray.com/palantir/releases/streams/_latestVersion)

## KeyedStream

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

    import static com.palantir.common.streams.MoreStreams.*;

`MoreStreams::stream` creates streams from:

 * [Iterables][Iterable] (equivalent to the wordy `StreamSupport.stream(iterable.spliterator(), false)`)
 * [Optionals][] (equivalent to `optionalValue.map(Stream::of).orElse(Stream.of())`)

[BiFunction]: https://docs.oracle.com/javase/8/docs/api/java/util/function/BiFunction.html
[Iterable]: https://docs.oracle.com/javase/8/docs/api/java/lang/Iterable.html
[Map]: https://docs.oracle.com/javase/8/docs/api/java/util/Map.html
[Optionals]: https://docs.oracle.com/javase/8/docs/api/java/util/Optional.html
[Stream]: https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html
