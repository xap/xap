/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.internal.server.space.repartitioning;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CopyBarrier {

    private final int totalThreads;
    private volatile Exception exception;
    private final CountDownLatch countDownLatch;

    CopyBarrier(int totalThreads) {
        this.countDownLatch = new CountDownLatch(totalThreads);
        this.totalThreads = totalThreads;
    }

    public void complete() {
        countDownLatch.countDown();
    }

    void completeExceptionally(Exception e) {
        exception = e;
        while (countDownLatch.getCount() > 0) {
            countDownLatch.countDown();
        }
    }

    public void await(long millis) throws Exception {

        boolean await = countDownLatch.await(millis, TimeUnit.MILLISECONDS);

        if (exception != null) {
            throw exception;
        }

        if (!await) {//timed out
            throw new TimeoutException("Timeout while waiting for copy consumers , " + (totalThreads - countDownLatch.getCount()) + " out of " + totalThreads + " finished successfully");
        }
    }
}