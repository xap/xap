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

import com.gigaspaces.events.NotifyActionType;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.EntryDeletedException;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.exception.ClosedResourceException;
import com.j_spaces.core.sadapter.SAException;

import java.util.concurrent.LinkedBlockingQueue;


/**
 * position fifo requests for notify and non-notify templates background search and consume
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class DefaultFifoBackgroundDispatcher extends FifoBackgroundDispatcher {
    public static final long NON_FIFO_EVENTS_MAX_WAIT_TIME = 1000;

    public DefaultFifoBackgroundDispatcher(int numNotifyFifoThreads, int numNonNotifyFifoThreads,
                                           CacheManager cacheManager, SpaceEngine engine) {
        super(numNotifyFifoThreads, numNonNotifyFifoThreads, cacheManager, engine);
    }

    @Override
    protected FifoWorkerThread createFifoWorkerThread(int num, boolean isNotify,
                                                      CacheManager cacheManager, SpaceEngine engine) {
        return new BackgroundFifoThread(num, isNotify, cacheManager, engine);
    }

    @Override
    public void positionRequest(FifoBackgroundRequest rd) {
        throw new UnsupportedOperationException("This operation is not supported for " + this.getClass().getName());
    }

    @Override
    public void activateRequest(FifoBackgroundRequest rd) {
        throw new UnsupportedOperationException("This operation is not supported for " + this.getClass().getName());
    }

    @Override
    public void positionAndActivateRequest(FifoBackgroundRequest rd) {
        if (rd.isNonNotifyRequest())
            positionAndActivateRequest(rd, _nonNotifyFifoThreads);

        if (rd.isNotifyRequest()) {
            if (rd.getTime() == 0)
                rd.setTime(SystemTime.timeMillis());
            positionAndActivateRequest(rd, _notifyFifoThreads);
        }
    }

    @Override
    public void cancelRequest(FifoBackgroundRequest rd) {
        throw new UnsupportedOperationException("This operation is not supported for " + this.getClass().getName());
    }

    /**
     * Handles the new requests - processing each entry FIFO.
     */
    private static class BackgroundFifoThread extends FifoWorkerThread {
        private final LinkedBlockingQueue<FifoBackgroundRequest> _fifoRecents;

        BackgroundFifoThread(int num, boolean isNotify, CacheManager cacheManager,
                             SpaceEngine engine) {
            super(num, isNotify, "BackgroundFifoThread", cacheManager, engine);
            _fifoRecents = new LinkedBlockingQueue<FifoBackgroundRequest>();
        }

        @Override
        public void positionRequest(FifoBackgroundRequest rd) {
            throw new UnsupportedOperationException("This operation is not supported for " + this.getClass().getName());
        }

        @Override
        public void activateFifoThread() {
            throw new UnsupportedOperationException("This operation is not supported for " + this.getClass().getName());
        }

        @Override
        public void positionAndActivateRequest(FifoBackgroundRequest rd) {
            _fifoRecents.add(rd);
        }

        @Override
        protected void handleNotifyRequest(FifoBackgroundRequest rd)
                throws SAException, EntryDeletedException {
            NotifyActionType notifyType = getNotifyType(rd.getSpaceOperation());

            if (notifyType != null) {
                if (rd.getAllowFifoNotificationsForNonFifoEvents() != null)
                    rd.getAllowFifoNotificationsForNonFifoEvents().waitTillAllowed(NON_FIFO_EVENTS_MAX_WAIT_TIME);
                if (rd.isCancelled())
                    return;

                handleNotifyRequest(rd, notifyType);
            }
        }

        @Override
        public void close() {
            _closeThread = true;
            FifoBackgroundRequest rd = new FifoBackgroundRequest();
            rd.setCancelled();
            _fifoRecents.add(rd);

            synchronized (_shutDown) {
                try {
                    while (!_closed) {
                        _shutDown.wait();
                    }
                } catch (InterruptedException ie) {
                    if (_logger.isTraceEnabled())
                        _logger.trace(Thread.currentThread().getName() + " interrupted.", ie);
                    //Restore interrupted state
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public void run() {
            // loop until shutdown
            while (true) {
                if (isInterrupted())
                    break; ///???????????????????????

                try {
                    if (_closeThread)
                        break;  //time to finish
                    try {
                        FifoBackgroundRequest rd = null;
                        try {
                            rd = _fifoRecents.take();
                        } catch (InterruptedException ex) {
                            if (_logger.isTraceEnabled())
                                _logger.trace(this.getName() + " interrupted.", ex);

                            //Restore the interrupted status
                            interrupt();

                            //fall through
                            break;
                        }

                        if (rd == null || rd.isCancelled())
                            continue;

                        handleRequest(rd);
                    } catch (ClosedResourceException ex) {
                        if (_logger.isErrorEnabled())
                            _logger.error("Caught Exception", ex);
                        continue;
                    }
                } finally {
                    try {
                        if (_context != null && (_closeThread || _fifoRecents.peek() == null))
                            _context = _cacheManager.freeCacheContext(_context);
                    } catch (Exception ex) {
                        if (_logger.isErrorEnabled())
                            _logger.error("Recent FIFO thread caught Exception.", ex);
                        _context = null;
                        continue;
                    }
                }
            }

            // notify i am finished
            synchronized (_shutDown) {
                _closed = true;
                _shutDown.notifyAll();
            }
        }
    }
}
