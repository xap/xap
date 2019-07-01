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

package com.gigaspaces.internal.utils.concurrent;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;


/**
 * @author eitany
 * @since 9.0.1
 */
public abstract class AbstractAsyncHandlerProvider implements IAsyncHandlerProvider {

    protected final Object _stateLock = new Object();
    private final Set<IAsyncHandlerProviderStateListener> _listeners = new HashSet<IAsyncHandlerProviderStateListener>();
    private volatile boolean _closed;

    @Override
    public IAsyncHandler start(AsyncCallable callable, long idleDelayMilis, String name,
                               boolean waitIdleDelayBeforeStart) {
        if (idleDelayMilis == 0)
            throw new IllegalArgumentException("Cannot start async callable with idle delay of 0 ms, callable type - " + callable + ", callable name - " + name);
        synchronized (_stateLock) {
            if (isClosed())
                return ClosedProviderAsyncHandler.INSTANCE;

            return startImpl(callable, idleDelayMilis, name, waitIdleDelayBeforeStart);
        }
    }

    protected abstract IAsyncHandler startImpl(AsyncCallable callable, long idleDelayMilis, String name, boolean waitIdleDelayBeforeStart);

    @Override
    public IAsyncHandler startMayBlock(AsyncCallable callable, long idleDelayMilis, String name,
                                       boolean waitIdleDelayBeforeStart) {
        if (idleDelayMilis == 0)
            throw new IllegalArgumentException("Cannot start async callable with idle delay of 0 ms, callable type - " + callable + ", callable name - " + name);
        synchronized (_stateLock) {
            if (isClosed())
                return ClosedProviderAsyncHandler.INSTANCE;

            return startMayBlockImpl(callable, idleDelayMilis, name, waitIdleDelayBeforeStart);
        }
    }

    protected abstract IAsyncHandler startMayBlockImpl(AsyncCallable callable, long idleDelayMilis, String name, boolean waitIdleDelayBeforeStart);

    @Override
    public void close() {
        synchronized (_stateLock) {
            if (_closed)
                return;

            _closed = true;
            onClose();
        }

        //We do not trigger this from within the lock as we want to avoid risk of deadlocks by calling external code while holding
        //an internal lock. The underlying list is not thread safe but the add/remove state listener method should prevent
        //changing this list after it is closed
        for (IAsyncHandlerProviderStateListener listener : _listeners) {
            listener.onClosed();
        }
        _listeners.clear();
    }

    protected abstract void onClose();

    @Override
    public boolean isClosed() {
        return _closed;
    }

    @Override
    public boolean addStateListener(IAsyncHandlerProviderStateListener listener) {
        synchronized (_stateLock) {
            if (_closed)
                return false;

            _listeners.add(listener);

            return true;
        }
    }

    @Override
    public void removeStateListener(IAsyncHandlerProviderStateListener listener) {
        synchronized (_stateLock) {
            if (_closed)
                return;

            _listeners.remove(listener);
        }
    }

    public static class ClosedProviderAsyncHandler implements IAsyncHandler {

        public static final ClosedProviderAsyncHandler INSTANCE = new ClosedProviderAsyncHandler();

        @Override
        public void wakeUp() {
        }

        @Override
        public boolean wakeUpAndWait(long timeout, TimeUnit units) {
            return false;
        }

        @Override
        public void stop(long timeout, TimeUnit units) {
        }

        @Override
        public void resume() {
        }

        @Override
        public void resumeNow() {
        }

        @Override
        public boolean isTerminated() {
            return true;
        }

    }
}