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

import java.util.concurrent.TimeoutException;

public class CopyBarrier {

    private int totalThreads;
    private int count;
    private Exception exception;

    public CopyBarrier(int totalThreads) {
        this.totalThreads = totalThreads;
    }

    public synchronized void complete() {
        count++;
        notifyAll();
    }

    public synchronized void completeExceptionally(Exception e) {
        count++;
        exception = e;
        notifyAll();
    }

    public synchronized void await(long milis) throws Exception {
        long start = System.currentTimeMillis();
        while (count < totalThreads && exception == null && System.currentTimeMillis() - start < milis) {
            wait(milis);
        }

        if (exception != null) {
            throw exception;
        }

        if(count < totalThreads){//timed out
            throw new TimeoutException("Timeout while waiting for copy consumers , "+count+" out of "+totalThreads+" finished successfully");
        }
    }
}