/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Test;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class MoreStreamsTests {
    private static final String DATA = "data";
    private static final ListenableFuture<String> LISTENABLE_FUTURE = Futures.immediateFuture(DATA);

    @Test
    public void testInCompletionOrder_future() {
        assertThat(MoreStreams.inCompletionOrder(Stream.of(LISTENABLE_FUTURE), 1)
                .map(Futures::getUnchecked)).containsExactly(DATA);
    }

    @Test
    public void testInCompletionOrder_transformWithExecutor() {
        assertThat(MoreStreams.inCompletionOrder(Stream.of(DATA), x -> x, MoreExecutors.directExecutor(), 1))
                .containsExactly(DATA);
    }
}
