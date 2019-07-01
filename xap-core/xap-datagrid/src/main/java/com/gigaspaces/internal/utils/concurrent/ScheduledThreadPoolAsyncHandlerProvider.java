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

import com.gigaspaces.logger.Constants;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Scheduled thread pool based implementation for {@link IAsyncHandlerProvider}
 *
 * @author eitany
 * @since 8.0.5
 */
@com.gigaspaces.api.InternalApi
public class ScheduledThreadPoolAsyncHandlerProvider extends AbstractAsyncHandlerProvider {
    private static final Logger _logger = Logger.getLogger(Constants.LOGGER_REPLICATION);

    final private ScheduledExecutorService _executorService;
    final private ThreadAsyncHandlerProvider _threadAsyncHandlerProvider;

    public ScheduledThreadPoolAsyncHandlerProvider(String name, int corePoolSize) {
        _threadAsyncHandlerProvider = new ThreadAsyncHandlerProvider();
        _executorService = Executors.newScheduledThreadPool(corePoolSize,
                new GSThreadFactory(name,
                        true));
    }

    @Override
    public IAsyncHandler startImpl(AsyncCallable callable,
                                   long idleDelayMilis, String name, boolean waitIdleDelayBeforeStart) {
        AsyncRunnableWrapper wrapper = new AsyncRunnableWrapper(callable,
                idleDelayMilis, name);
        callable.setHandler(wrapper);
        wrapper.start(waitIdleDelayBeforeStart);
        return wrapper;
    }

    @Override
    public IAsyncHandler startMayBlockImpl(AsyncCallable callable,
                                           long idleDelayMilis, String name, boolean waitIdleDelayBeforeStart) {
        return _threadAsyncHandlerProvider.startMayBlock(callable, idleDelayMilis, name, waitIdleDelayBeforeStart);
    }

    @Override
    protected void onClose() {
        _executorService.shutdownNow();
        _threadAsyncHandlerProvider.close();
    }

    private enum State {
        STOPPED,
        SCHEDULED,
        SUBMITTED,
        RUNNING,
        SUSPENDED
    }

    private class AsyncRunnableWrapper
            implements IAsyncHandler {

        private final Callable<CycleResult> _runnable;
        private final long _idleDelayMilis;
        private final String _name;
        private final Lock _lock = new ReentrantLock(true);
        private final Condition _condition;
        private final AtomicBoolean _singleRunnerFlag = new AtomicBoolean(false);
        private Future<?> _future;
        private int _iteration;
        private volatile State _state;
        private CycleResult _pendingSuspendedResponse;

        public AsyncRunnableWrapper(Callable<CycleResult> runnable,
                                    long idleDelayMilis, String name) {
            _runnable = runnable;
            _idleDelayMilis = idleDelayMilis;
            _name = name;
            _condition = _lock.newCondition();
        }

        public void start(boolean waitIdleDelayBeforeStart) {
            _lock.lock();
            try {
                if (waitIdleDelayBeforeStart)
                    reschedule();
                else
                    resubmit();
            } finally {
                _lock.unlock();
            }
        }

        @Override
        public void wakeUp() {
            _lock.lock();
            try {
                switch (_state) {
                    case STOPPED:
                    case SUBMITTED:
                    case RUNNING:
                    case SUSPENDED:
                        break;
                    case SCHEDULED:
                        if (_future.cancel(false))
                            resubmit();
                        break;
                    default:
                        throw new IllegalStateException("unknown state " + _state);
                }
            } finally {
                _lock.unlock();
            }
        }

        private void resubmit() {
            try {
                if (changeState(State.SUBMITTED))
                    _future = _executorService.submit(createExecutionTask());
            } catch (RejectedExecutionException e) {
                if (!isClosed())
                    throw e;
            }
        }

        private void reschedule() {
            try {
                if (changeState(State.SCHEDULED))
                    _future = _executorService.schedule(createExecutionTask(),
                            _idleDelayMilis,
                            TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                if (!isClosed())
                    throw e;
            }
        }

        @Override
        public boolean wakeUpAndWait(long timeout, TimeUnit units) {
            _lock.lock();
            try {
                if (_state == State.STOPPED)
                    return false;

                int startIteration = _iteration;
                wakeUp();
                //Wakeup and wait until there's a complete iteration
                while (startIteration == _iteration) {
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
            if (_state == State.STOPPED)
                return;
            _lock.lock();
            try {
                if (_state == State.STOPPED)
                    return;
                while (_state == State.RUNNING) {
                    if (!_condition.await(timeout, units))
                        //If condition not triggered, we are at running state, interrupt it
                        break;
                }
                if (!changeState(State.STOPPED))
                    return;
                //cancel the scheduled task
                if (_future != null)
                    _future.cancel(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                _lock.unlock();
            }
        }

        @Override
        public void resume() {
            _lock.lock();
            try {
                if (_state == State.RUNNING) {
                    _pendingSuspendedResponse = CycleResult.IDLE_CONTINUE;
                    return;
                }
                if (_state != State.SUSPENDED)
                    return;
                reschedule();
            } finally {
                _lock.unlock();
            }
        }

        @Override
        public void resumeNow() {
            _lock.lock();
            try {
                if (_state == State.RUNNING) {
                    _pendingSuspendedResponse = CycleResult.CONTINUE;
                    return;
                }
                if (_state != State.SUSPENDED)
                    return;
                resubmit();
            } finally {
                _lock.unlock();
            }

        }

        @Override
        public boolean isTerminated() {
            if (isClosed())
                return true;

            return _state == State.STOPPED;
        }

        private Runnable createExecutionTask() {
            return new ExecutionTask(_iteration);
        }


        /**
         * Must be run under lock
         */
        private boolean changeState(State state) {
            if (_state == State.STOPPED)
                return false;

            _state = state;
            return true;
        }


        private class ExecutionTask implements Runnable {
            private final int _iterationAtGeneration;

            public ExecutionTask(int iteration) {
                _iterationAtGeneration = iteration;
            }

            @Override
            public void run() {
                try {
                    _lock.lock();
                    try {
                        if (_iteration != _iterationAtGeneration || !changeState(State.RUNNING)) {
                            return;
                        }
                    } finally {
                        _lock.unlock();
                    }
                    if (!_singleRunnerFlag.compareAndSet(false, true))
                        return;

                    CycleResult cycleResult = _runnable.call();

                    _lock.lock();
                    try {
                        if (_state == State.STOPPED)
                            return;

                        if (cycleResult == CycleResult.SUSPEND && _pendingSuspendedResponse != null)
                            cycleResult = _pendingSuspendedResponse;

                        _pendingSuspendedResponse = null;

                        _iteration++;
                        _condition.signalAll();
                        switch (cycleResult) {
                            case IDLE_CONTINUE:
                                //Schedule consecutive iteration with delay
                                reschedule();
                                break;
                            case CONTINUE:
                                //Continue immediately
                                resubmit();
                                break;
                            case SUSPEND:
                                //Suspend, will not run until resume or waken up
                                changeState(State.SUSPENDED);
                                break;
                            case TERMINATE:
                                changeState(State.STOPPED);
                                break;
                        }
                    } finally {
                        if (!_singleRunnerFlag.compareAndSet(true, false)) {
                            if (_logger.isLoggable(Level.SEVERE))
                                _logger.severe("Unexpected single flag runner result on ScheduledThreadPoolAsyncHandlerProvider. task is " + _runnable);
                        }
                        _lock.unlock();
                    }
                } catch (InterruptedException e) {
                    if (_logger.isLoggable(Level.FINER))
                        _logger.log(Level.FINER,
                                "async handler is interrupted",
                                e);
                } catch (Exception e) {
                    if (_logger.isLoggable(Level.SEVERE))
                        _logger.log(Level.SEVERE,
                                "Error occurred while executing async cycle",
                                e);
                }
            }
        }
    }

}
