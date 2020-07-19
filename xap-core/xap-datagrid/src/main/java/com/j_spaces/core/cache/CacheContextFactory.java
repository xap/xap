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

package com.j_spaces.core.cache;

import com.gigaspaces.internal.utils.collections.WeakHashSet;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.exception.ClosedResourceException;
import com.j_spaces.kernel.SystemProperties;

import java.util.Collections;
import java.util.Set;


@com.gigaspaces.api.InternalApi
public class CacheContextFactory {
    private final String _fullSpaceName;
    private final Set<Context> _createdContexts;
    private volatile boolean _isClosed;

    private final ThreadLocal<Context> _context = new ThreadLocal<Context>() {
        @Override
        protected Context initialValue() {
            Context context = new Context();
            _createdContexts.add(context);
            return context;
        }
    };

    public CacheContextFactory(@SuppressWarnings("UnusedParameters") CacheManager cacheManager, String fullSpaceName) {
        _fullSpaceName = fullSpaceName;
        _createdContexts = Collections.synchronizedSet(new WeakHashSet<>());
    }

    /**
     * returns a free Context from the pool. If none are available, one is allocated on demand. This
     * call *MUST* be followed by a freeCacheContext() in a finally() block!
     *
     * @return a free Context object
     */
    public Context getCacheContext() {
        if (_isClosed)    //closeAllContext called
            throw new ClosedResourceException("Space [" + _fullSpaceName + "] is not available. Internal resources are being closed.");

        Context context = _context.get();
        if (context.isLocalActive()) // this thread already has an active context --> need another one.
        {
            // no need to save in the _createdContexts
            // since we assume the other context will be released after this one.
            context = new Context();
        }

        context.setActive(true);
        context.setOwningThreadName(Thread.currentThread().getName());
        return context;
    }


    /**
     * returns the active Context of this thread or null if none
     *
     * @return an active Context object
     */
    public Context viewCacheContext() {
        if (_isClosed)    //closeAllContext called
            return null;

        Context context = _context.get();
        return context.isLocalActive() ? context : null;
    }


    /**
     * Free a Context acquired by getCacheContext(). If Context was taken from the pool, it will be
     * returned to it. Otherwise, it was allocated on demand, and will be collected by the JVM GC.
     * Make sure to call this method from within a finally block to release any acquired Contexts.
     *
     * @param ctx A Context object in use, and to be freed.
     */
    public void freeCacheContext(Context ctx) {

        ctx.clean();
        ctx.setActive(false);
    }

    /**
     * close all contexts free and occupied, waits until all Contexts are returned.
     */
    public void closeAllContexts() {

        long closeMaxWait = Long.getLong(SystemProperties.CACHE_CONTEXT_CLOSE_MAX_WAIT, SystemProperties.CACHE_CONTEXT_CLOSE_MAX_WAIT_DEFAULT);
        long startTime = System.currentTimeMillis();
        long endTime = (closeMaxWait * 1000) + startTime;

        _isClosed = true;    //volatile write

        synchronized (_createdContexts) {
            //wait for all contexts (not from pool)
            for (Context context : _createdContexts) {
                while (context.isActive()) {
                    try {
                        //noinspection BusyWait
                        Thread.sleep(100);
                    } catch (InterruptedException ignored) {
                    }
                    // wait time is zero
                    if (closeMaxWait <= 0) {
                        CacheManager.getLogger().warn(_fullSpaceName + ": not all cache context closed, forced exit, owning thread is " + context.getOwningThreadName() + ", wait time is zero");
                        return;
                    }
                    // waited enough
                    if (0 < (System.currentTimeMillis() - endTime)) {
                        CacheManager.getLogger().warn(_fullSpaceName + ": not all cache context closed, forced exit, owning thread is " + context.getOwningThreadName() + ", waited enough");
                        return;
                    }
                }
            }
        }
    }
}
