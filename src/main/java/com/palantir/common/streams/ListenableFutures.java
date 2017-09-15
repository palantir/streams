package com.palantir.common.streams;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

class ListenableFutures {
    private ListenableFutures() {}

    static <U, V> Function<U, CompletableFuture<V>> toJava8AsyncFunction(
            AsyncFunction<U, V> asyncFunction) {
        return x -> {
            try {
                return toCompletableFuture(asyncFunction.apply(x));
            } catch (Exception e) {
                CompletableFuture<V> result = new CompletableFuture<>();
                result.completeExceptionally(e);
                return result;
            }
        };
    }

    private static <V> CompletableFuture<V> toCompletableFuture(ListenableFuture<V> listenableFuture) {
        CompletableFuture<V> future = new CompletableFuture<>();

        Futures.addCallback(listenableFuture, new FutureCallback<V>() {

            @Override
            public void onSuccess(V result) {
                future.complete(result);
            }

            @Override
            public void onFailure(Throwable t) {
                future.completeExceptionally(t);
            }
        });

        return future;
    }
}
