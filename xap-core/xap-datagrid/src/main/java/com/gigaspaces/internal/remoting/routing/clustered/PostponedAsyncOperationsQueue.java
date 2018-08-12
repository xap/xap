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

package com.gigaspaces.internal.remoting.routing.clustered;

import com.gigaspaces.internal.remoting.routing.clustered.ClusterRemoteOperationRouter.AsyncOperationExecutor;
import com.gigaspaces.internal.utils.concurrent.GSThreadFactory;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.exception.ClosedResourceException;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author eitany
 * @since 9.0.1
 */
@com.gigaspaces.api.InternalApi
public class PostponedAsyncOperationsQueue {
    public static final String THREADS_NAME_PREFIX = "PostponedAsyncOperationsQueue-";
    private static final int CAPACITY = 10000;
    private final Logger _logger;
    private final ThreadPoolExecutor _threadPoolExecutor;
    private final Object _lock = new Object();
    private volatile boolean _closed;

    public PostponedAsyncOperationsQueue(String name) {
        _threadPoolExecutor = new ThreadPoolExecutor(0, 1,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingDeque<Runnable>(CAPACITY),
                new GSThreadFactory(THREADS_NAME_PREFIX + name, true));

        this._logger = Logger.getLogger(Constants.LOGGER_SPACEPROXY_ROUTER + '.' + name);
    }

    public void enqueue(final AsyncOperationExecutor<?> execotor) {
        try {
            _threadPoolExecutor.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        execotor.executeAsync();
                    } catch (Exception e) {
                        if (_logger.isLoggable(Level.WARNING))
                            _logger.log(Level.WARNING, "Unexpected exception caught in PostponedAsyncOperationsHandler", e);
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            if (!_closed)
                throw e;

            throw new ClosedResourceException("Proxy is being closed");
        }
    }

    public void close() {
        synchronized (_lock) {
            if (_closed)
                return;

            _closed = true;
            _threadPoolExecutor.shutdownNow();
        }
    }

}