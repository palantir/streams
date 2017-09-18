package com.palantir.common.streams;

import com.google.common.util.concurrent.Futures;

import java.util.concurrent.Future;

class MoreFutures {
    private MoreFutures() {}

    static <T extends Future<U>, U> T blockOnCompletion(T future) {
        try {
            Futures.getUnchecked(future);
        } catch (RuntimeException ignored) {}
        return future;
    }
}
