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

package com.gigaspaces.internal.client.spaceproxy.events;

import com.gigaspaces.events.GSEventRegistration;
import com.gigaspaces.events.ManagedRemoteEventListener;
import com.gigaspaces.events.batching.BatchRemoteEvent;
import com.gigaspaces.events.batching.BatchRemoteEventListener;
import com.gigaspaces.internal.utils.concurrent.ContextClassLoaderCallable;

import net.jini.core.event.RemoteEvent;
import net.jini.core.event.RemoteEventListener;
import net.jini.core.event.UnknownEventException;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 8.0.4
 */
public abstract class DataEventListener implements ManagedRemoteEventListener, BatchRemoteEventListener {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_CLIENT);

    protected final RemoteEventListener _listener;
    protected final BatchRemoteEventListener _batchListener;
    protected final ExecutorService _threadPool;

    public DataEventListener(RemoteEventListener listener, ExecutorService threadPool) {
        this._listener = listener;
        this._batchListener = listener instanceof BatchRemoteEventListener ? (BatchRemoteEventListener) listener : null;
        this._threadPool = threadPool;
    }

    @Override
    public void init(GSEventRegistration registration) {
        if (_listener instanceof ManagedRemoteEventListener)
            ((ManagedRemoteEventListener) _listener).init(registration);
    }

    @Override
    public void shutdown(GSEventRegistration registration) {
        if (_listener instanceof ManagedRemoteEventListener)
            ((ManagedRemoteEventListener) _listener).shutdown(registration);
    }

    @Override
    public void notify(RemoteEvent event) throws UnknownEventException, RemoteException {
        _listener.notify(event);
    }

    @Override
    public void notifyBatch(BatchRemoteEvent batchEvent) throws UnknownEventException, RemoteException {
        if (_batchListener != null)
            _batchListener.notifyBatch(batchEvent);
        else
            dispatchRemoteEventsConcurrently(batchEvent.getEvents());
    }

    private void dispatchRemoteEventsConcurrently(RemoteEvent[] events)
            throws UnknownEventException, RemoteException {
        List<Future<Object>> results = new ArrayList<Future<Object>>();
        for (RemoteEvent event : events) {
            // the context class loader here should be the one the user registered to notification with
            Future<Object> future = _threadPool.submit(new BatchNotifyTask(_listener, event));
            results.add(future);
        }

        for (Future<Object> result : results) {
            try {
                result.get();
            } catch (InterruptedException e) {
                _logger.debug("caught exception during event delivery", e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof UnknownEventException)
                    throw ((UnknownEventException) cause);
                if (cause instanceof RemoteException)
                    throw ((RemoteException) cause);

                _logger.debug("caught exception during event delivery", e);
            }
        }
    }

    private static class BatchNotifyTask extends ContextClassLoaderCallable<Object> {
        private final RemoteEventListener listener;
        private final RemoteEvent event;

        private BatchNotifyTask(RemoteEventListener listener, RemoteEvent event) {
            this.listener = listener;
            this.event = event;
        }

        @Override
        protected Object execute() throws Exception {
            listener.notify(event);
            return null;
        }
    }
}
