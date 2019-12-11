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

package com.gigaspaces.internal.utils.pool;

import com.j_spaces.kernel.pool.IResourceProcedure;
import com.j_spaces.kernel.pool.ResourcePool;

import java.util.concurrent.atomic.AtomicLong;

/**
 * An extension of the {@link ResourcePool} which has memory boundaries, and it will not pool new
 * resources if the memory boundaries have reached.
 *
 * @author eitany
 * @since 9.6
 */
@com.gigaspaces.api.InternalApi
public class MemoryBoundedResourcePool<T extends IMemoryAwareResource> extends ResourcePool<T> implements IMemoryAwareResourcePool {

    private final long _poolMemoryBounds;
    private final Object _allocateCheckLock = new Object();
    private final IMemoryAwareResourceFactory<T> _resourceFactory;

    public MemoryBoundedResourcePool(IMemoryAwareResourceFactory<T> resourceFactory,
                                     int minResources, int maxResources, long poolMemoryBounds) {
        super(resourceFactory, minResources, maxResources);
        _resourceFactory = resourceFactory;
        _poolMemoryBounds = poolMemoryBounds;
    }

    @Override
    protected T tryAllocateNewPooledResource() {
        synchronized (_allocateCheckLock) {
            final long usedMemory = CalculateUsedMemory();

            if (usedMemory >= _poolMemoryBounds)
                return null;

            return _resourceFactory.allocate(this);
        }
    }

    protected long CalculateUsedMemory() {
        final AtomicLong usedMemory = new AtomicLong(0);
        forAllResources(new IResourceProcedure<T>() {

            @Override
            public void invoke(T resource) {
                usedMemory.addAndGet(resource.getUsedMemory());

            }
        });
        return usedMemory.get();
    }

    @Override
    public boolean isLimitReached(long length) {
        //not accurate, since lock is not held but good enough, if we obtain lock we risk in future deadlocks since this code is called by a listener
        return (CalculateUsedMemory() + length) >= _poolMemoryBounds;
    }
}
