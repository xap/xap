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

/*
 * Title:        ResourcesPool.java
 * Description:  Resource Pool for reuse of resources
 * Company:      GigaSpaces Technologies
 * 
 * @author		 Guy korland
 * @author       Moran Avigdor
 * @version      1.0 01/06/2005
 * @since		 4.1 Build#1173
 */

package com.j_spaces.kernel.pool;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Bound Pool of {@linkplain com.j_spaces.kernel.pool.Resource Resource} objects. At construction,
 * the pool consumes as many as <tt>minResources</tt> resources. When a resource requests is
 * submitted, and less than <tt>maxResources</tt> are being used, a free resource will be returned
 * from the pool. When all resources are in use, a new Resource will be allocated by the {@linkplain
 * com.j_spaces.kernel.pool.IResourceFactory resourceFactory}. There can be no more than
 * <tt>maxResources</tt> in the pool, and excessive resources are discarded.
 */
@com.gigaspaces.api.InternalApi
public class ResourcePool<R extends IResource> implements IResourcePool<R> {
    /**
     * pool of resources
     */
    private final R[] _resourcesPool;
    /**
     * max number of resources to keep in pool
     */
    private final int _maxResources;
    /**
     * next free index for new resource allocation
     */
    private final AtomicInteger _nextFreeIndex;
    /**
     * factory for creating new resources
     */
    private final IResourceFactory<R> _resourceFactory;
    /**
     * true if reached maxResources
     */
    private boolean _full;

    /**
     * Creates a new Resources Pool with the specified resourceFactory, and max Resources.
     *
     * @param resourceFactory resource factory instance for new resources
     * @param minResources    resources to pre-allocate; can be zero
     * @param maxResources    upper bound on the number of resources
     */
    public ResourcePool(IResourceFactory<R> resourceFactory, int minResources, int maxResources) {
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
    protected ResourcePool(IResourceFactory<R> resourceFactory,
                           int minResources, int maxResources, R[] initialResources) {
        _resourceFactory = resourceFactory;
        _maxResources = maxResources;

        if (initialResources != null && initialResources.length > maxResources)
            throw new IllegalArgumentException("initialResources length cannot exceed maxResources");
        // allocate a fixed pool of peers
        //noinspection unchecked
        _resourcesPool = (R[]) new IResource[maxResources];

        int i = 0;

        if (initialResources != null) {
            for (; i < initialResources.length; ++i) {
                _resourcesPool[i] = initialResources[i];
                _resourcesPool[i].setFromPool(true);
            }
        }

        for (; i < minResources; ++i) {
            _resourcesPool[i] = _resourceFactory.allocate();
            _resourcesPool[i].setFromPool(true);
        }

        _nextFreeIndex = new AtomicInteger(i);
    }

    @Override
    public R getResource() {
        return getResource(true);
    }

    /**
     * Returns a Resource from the pool. If there is an un-used Resource in the pool, it is
     * returned; otherwise, a new Resource is created and added to the pool.
     *
     * @return free Resource allocated to this request
     */
    @Override
    public R getResource(boolean waitIfNeeded) {
        R resource = findFreeResource();

        /* retry on unsuccessful set - race condition on free resource. */
        if (resource == null) // free resource not found
        {
            // save in pool
            if (!_full) // we need this check so we won't get integer overflow on getAndIncrement()
            {
                // creates new peer
                resource = tryAllocateNewPooledResource();
                if (resource == null)
                    return handleFullPool(waitIfNeeded);
                else {
                    resource.setAcquired(true); // to avoid stealing need to set before adding to pool

                    int newIndex = _nextFreeIndex.getAndIncrement();
                    if (newIndex < _maxResources) {
                        resource.setFromPool(true);
                        _resourcesPool[newIndex] = resource;
                    } else {
                        _full = true;
                    }
                }
            } else {
                // creates new peer
                return handleFullPool(waitIfNeeded);
            }
        }

        return resource;
    }

    protected R tryAllocateNewPooledResource() {
        return _resourceFactory.allocate();
    }

    R findFreeResource() {
        R resource;
        int i = 0;

        do     // if resource is in use, try the next
        {
            resource = _resourcesPool[i];
        }
        while (resource != null && !resource.acquire() && ++i < _maxResources);

        if (i >= _maxResources)
            resource = null;

        return resource;
    }

    protected R handleFullPool(boolean waitIfNeeded) {
        R resource = _resourceFactory.allocate();
        resource.setAcquired(true); // to avoid stealing need to set before adding to pool
        return resource;
    }

    /**
     * Free the specified resource. Same as calling {@link IResource#release()}
     */
    @Override
    public void freeResource(R resourceToFree) {
        resourceToFree.release();
    }

    @Override
    public int availableResources() {
        int counter = 0;
        for (int i = 0; i < _maxResources; i++) {
            R resource = _resourcesPool[i];
            if (resource != null && !resource.isAcquired()) {
                counter++;
            }
        }

        return counter;
    }

    @Override
    public int size() {
        return _nextFreeIndex.get();
    }

    @Override
    public void forAllResources(IResourceProcedure<R> procedure) {
        for (R resource : _resourcesPool) {
            if (resource == null)
                return;

            procedure.invoke(resource);
        }
    }
}