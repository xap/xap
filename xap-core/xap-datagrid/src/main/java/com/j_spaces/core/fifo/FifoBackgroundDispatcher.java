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

package com.j_spaces.core.fifo;

import com.gigaspaces.internal.server.space.SpaceEngine;
import com.j_spaces.core.cache.CacheManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 8.0.4
 */
public abstract class FifoBackgroundDispatcher {
    protected static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_FIFO);

    protected final FifoWorkerThread[] _notifyFifoThreads;
    protected final FifoWorkerThread[] _nonNotifyFifoThreads;

    protected FifoBackgroundDispatcher(int numNotifyFifoThreads, int numNonNotifyFifoThreads,
                                       CacheManager cacheManager, SpaceEngine engine) {
        // start  working threads that will take from the new events
        // & match fifo templates
        _notifyFifoThreads = new FifoWorkerThread[numNotifyFifoThreads];
        for (int i = 0; i < _notifyFifoThreads.length; i++) {
            _notifyFifoThreads[i] = createFifoWorkerThread(i, true, cacheManager, engine);
            _notifyFifoThreads[i].start();
        }

        _nonNotifyFifoThreads = new FifoWorkerThread[numNonNotifyFifoThreads];
        for (int i = 0; i < _nonNotifyFifoThreads.length; i++) {
            _nonNotifyFifoThreads[i] = createFifoWorkerThread(i, false, cacheManager, engine);
            _nonNotifyFifoThreads[i].start();
        }
    }

    protected abstract FifoWorkerThread createFifoWorkerThread(int num, boolean isNotify,
                                                               CacheManager cacheManager, SpaceEngine engine);

    public void close() {
        for (int i = 0; i < _nonNotifyFifoThreads.length; i++)
            _nonNotifyFifoThreads[i].close();

        for (int i = 0; i < _notifyFifoThreads.length; i++)
            _notifyFifoThreads[i].close();
    }

    public int getNumNotifyFifoThreads() {
        return _notifyFifoThreads.length;
    }

    public int getNumNonNotifyFifoThreads() {
        return _nonNotifyFifoThreads.length;
    }

    public abstract void positionRequest(FifoBackgroundRequest rd);

    public abstract void activateRequest(FifoBackgroundRequest rd);

    public abstract void positionAndActivateRequest(FifoBackgroundRequest rd);

    public abstract void cancelRequest(FifoBackgroundRequest rd);

    protected static void positionRequest(FifoBackgroundRequest rd, FifoWorkerThread[] workers) {
        for (int i = 0; i < workers.length; i++) {
            //if there is a template in rd store only in its thread according to fifo partition
            if (rd.getTemplate() != null && rd.getTemplate().getFifoThreadPartition() != i)
                continue;
            workers[i].positionRequest(rd);
        }
    }

    protected static void activateRequest(FifoBackgroundRequest rd, FifoWorkerThread[] workers) {
        for (int i = 0; i < workers.length; i++) {
            //if there is a template in rd store only in its thread according to fifo partition
            if (rd.getTemplate() != null && rd.getTemplate().getFifoThreadPartition() != i)
                continue;
            workers[i].activateFifoThread();
        }
    }

    protected static void positionAndActivateRequest(FifoBackgroundRequest rd, FifoWorkerThread[] workers) {
        for (int i = 0; i < workers.length; i++) {
            //if there is a template in rd store only in its thread according to fifo partition
            if (rd.getTemplate() != null && rd.getTemplate().getFifoThreadPartition() != i)
                continue;
            workers[i].positionAndActivateRequest(rd);
        }
    }
}
