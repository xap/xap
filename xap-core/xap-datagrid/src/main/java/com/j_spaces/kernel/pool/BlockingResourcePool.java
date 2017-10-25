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

package com.j_spaces.kernel.pool;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.LockSupport;


/**
 * a resource pool that blocks clients when all resources are busy.
 *
 * @author asy ronen
 * @version 1.0
 * @since 6.6
 */
@com.gigaspaces.api.InternalApi
public class BlockingResourcePool<R extends IResource> extends ResourcePool<R> {

    private final Queue<Thread> waitingQueue;

    private static int noWaitParkTime;

    static {
        try {
            LockSupport.parkUntil(-1);
            noWaitParkTime = -1;
        } catch (Exception e) {
            noWaitParkTime = 0;
        }
    }

    /**
     * Creates a new Resources Pool with the specified resourceFactory, and max Resources.
     *
     * @param resourceFactory resource factory instance for new resources
     * @param minResources    resources to pre-allocate; can be zero
     * @param maxResources    upper bound on the number of resources
     */
    public BlockingResourcePool(IResourceFactory<R> resourceFactory, int minResources, int maxResources) {
        this(resourceFactory, minResources, maxResources, null);
    }

    /**
     * Creates a new Resources Pool with the specified resourceFactory, and max Resources.
     *
     * @param resourceFactory  resource factory instance for new resources
     * @param minResources     resources to pre-allocate; can be zero
     * @param maxResources     upper bound on the number of resources
     * @param initialResources initial array of resources to init the pool with
     */
    public BlockingResourcePool(IResourceFactory<R> resourceFactory, int minResources, int maxResources, R[] initialResources) {
        super(resourceFactory, minResources, maxResources, initialResources);
        waitingQueue = new ConcurrentLinkedQueue<Thread>();
    }

    @Override
    protected R handleFullPool(boolean waitIfNeeded) {
        final Thread thread = Thread.currentThread();
        while (true) {
            waitingQueue.add(thread);
            R resource = findFreeResource();
            if (resource != null) {
                waitingQueue.remove(thread);

                // clears the "memory" of the thread's park state
                // in case some resource(s) was returned and "unparked"
                // the current thread.
                // state must be cleared so that next time park() will
                // be called the thread will actually park.
                // value of parkUntil() MUST be -1, for sun(like) JVMs(0 blocks forever!)
                // and 0 for BEA.
                LockSupport.parkUntil(noWaitParkTime);

                return resource;
            }

            if (!waitIfNeeded){
                waitingQueue.remove(thread);
                throw new ResourceNotAvailableException();
            }
            LockSupport.park();
            resource = findFreeResource();
            if (resource != null) {
                return resource;
            }
        }
    }

    @Override
    public void freeResource(R resourceToFree) {
        super.freeResource(resourceToFree);
        Thread thread = waitingQueue.poll();
        if (thread != null) {
            LockSupport.unpark(thread);
        }
    }

}
