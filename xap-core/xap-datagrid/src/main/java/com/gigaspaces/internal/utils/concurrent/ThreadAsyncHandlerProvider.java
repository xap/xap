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

import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.logger.Constants;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

@com.gigaspaces.api.InternalApi
public class ThreadAsyncHandlerProvider extends AbstractAsyncHandlerProvider {
    private static final Logger _logger = Logger.getLogger(Constants.LOGGER_REPLICATION);

    private final Set<Thread> _threads = new HashSet<Thread>();

    @Override
    public IAsyncHandler startImpl(AsyncCallable callable,
                                   long idleDelayMilis, String name, boolean waitIdleDelayBeforeStart) {
        AsyncRunnableWrapper wrapper = new AsyncRunnableWrapper(this,
                callable,
                idleDelayMilis, waitIdleDelayBeforeStart);
        callable.setHandler(wrapper);
        GSThread thread;
        if (StringUtils.hasText(name))
            thread = new GSThread(wrapper, name);
        else
            thread = new GSThread(wrapper);

        thread.setDaemon(true);
        _threads.add(thread);
        wrapper.setThread(thread);
        thread.start();
        return wrapper;
    }

    @Override
    public IAsyncHandler startMayBlockImpl(AsyncCallable callable,
                                           long idleDelayMilis, String name, boolean waitIdleDelayBeforeStart) {
        return startImpl(callable, idleDelayMilis, name, waitIdleDelayBeforeStart);
    }

    public void removeCurrent() {
        synchronized (_stateLock) {
            _threads.remove(Thread.currentThread());
        }
    }

    @Override
    protected void onClose() {
        for (Thread thread : _threads) {
            thread.interrupt();
        }
    }

    public int activeThreadsCount() {
        synchronized (_stateLock) {
            return _threads.size();
        }
    }

    public static class AsyncRunnableWrapper
            implements Runnable, IAsyncHandler {

        private final Callable<CycleResult> _runnable;
        private final ThreadAsyncHandlerProvider _provider;
        private final long _idleDelayMilis;
        private final Lock _lock = new ReentrantLock(false);
        private final Condition _condition;
        private final boolean _waitIdleDelayBeforeStart;
        private boolean _wakenUp;
        private int _cycle;
        private volatile boolean _stopped;
        private volatile Thread _thread;
        private volatile boolean _terminated;
        private boolean _resumeRequest;
        private boolean _resumeNowRequest;

        public AsyncRunnableWrapper(ThreadAsyncHandlerProvider provider,
                                    Callable<CycleResult> runnable, long idleDelayMilis, boolean waitIdleDelayBeforeStart) {
            _provider = provider;
            _runnable = runnable;
            _idleDelayMilis = idleDelayMilis;
            _waitIdleDelayBeforeStart = waitIdleDelayBeforeStart;
            _condition = _lock.newCondition();
        }

        public void setThread(Thread thread) {
            _thread = thread;
        }

        public void run() {
            try {
                if (_waitIdleDelayBeforeStart) {
                    _lock.lock();
                    try {
                        _condition.await(_idleDelayMilis, TimeUnit.MILLISECONDS);
                        if (_stopped)
                            return;
                    } finally {
                        _lock.unlock();
                    }
                }

                CycleResult cycleResult;
                do {
                    cycleResult = _runnable.call();
                    _lock.lock();
                    try {
                        _cycle++;
                        _condition.signalAll();
                        if (cycleResult == CycleResult.SUSPEND) {
                            while (!_stopped && !_resumeRequest && !_resumeNowRequest)
                                _condition.await();

                            if (!_stopped)
                                cycleResult = _resumeRequest ? CycleResult.IDLE_CONTINUE : CycleResult.CONTINUE;
                        }
                        if (cycleResult == CycleResult.IDLE_CONTINUE) {
                            if (!_wakenUp)
                                _condition.await(_idleDelayMilis,
                                        TimeUnit.MILLISECONDS);
                        }
                        _resumeRequest = false;
                        _resumeNowRequest = false;
                        _wakenUp = false;
                    } finally {
                        _lock.unlock();
                    }
                } while (!_stopped && cycleResult != CycleResult.TERMINATE);
                _terminated = true;
            } catch (InterruptedException e) {
                // Break loop on interrupted
            } catch (Exception e) {
                if (_logger.isLoggable(Level.SEVERE))
                    _logger.log(Level.SEVERE,
                            "Error occurred while executing async cycle",
                            e);
            }
            _terminated = true;
            _provider.removeCurrent();
        }

        @Override
        public void wakeUp() {
            _lock.lock();
            try {
                _wakenUp = true;
                _condition.signalAll();
            } finally {
                _lock.unlock();
            }
        }

        @Override
        public boolean wakeUpAndWait(long timeout, TimeUnit units) {
            if (_terminated || _stopped)
                return false;
            _lock.lock();
            try {
                int cycleBeforeWakeup = _cycle;
                wakeUp();
                while (_cycle == cycleBeforeWakeup) {
                    try {
                        if (!_condition.await(timeout, units))
                            return false;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                }
                return true;
            } finally {
                _lock.unlock();
            }
        }

        @Override
        public void stop(long timeout, TimeUnit units) {
            if (_terminated || _stopped)
                return;
            _stopped = true;
            wakeUp();
            try {
                Thread thread = _thread;
                if (thread != null)
                    thread.join(units.toMillis(timeout));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void resume() {
            if (_terminated || _stopped)
                return;

            _lock.lock();
            try {
                _resumeRequest = true;
                _condition.signalAll();
            } finally {
                _lock.unlock();
            }
        }

        @Override
        public void resumeNow() {
            if (_terminated || _stopped)
                return;

            _lock.lock();
            try {
                _resumeNowRequest = true;
                _condition.signalAll();
            } finally {
                _lock.unlock();
            }
        }

        @Override
        public boolean isTerminated() {
            return _terminated || _stopped;
        }

    }
}
