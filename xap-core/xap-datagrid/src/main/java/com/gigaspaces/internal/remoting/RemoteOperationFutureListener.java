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

package com.gigaspaces.internal.remoting;

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.async.internal.DefaultAsyncResult;
import com.gigaspaces.internal.remoting.routing.clustered.PostponedAsyncOperationsQueue;
import com.gigaspaces.internal.remoting.routing.clustered.RemoteOperationsExecutorProxy;
import com.gigaspaces.internal.utils.concurrent.ContextClassLoaderRunnable;
import com.gigaspaces.lrmi.LRMIRuntime;
import com.j_spaces.core.exception.internal.ProxyInternalSpaceException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.gigaspaces.internal.remoting.routing.clustered.PostponedAsyncOperationsQueue.THREADS_NAME_PREFIX;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class RemoteOperationFutureListener<T extends RemoteOperationResult> implements AsyncFuture<Object> {
    protected final Logger _logger;
    private final CountDownLatch _completionLatch;
    private final boolean _getResultOnCompletion;
    private volatile Object _result;
    private volatile ExecutionException _exception;
    private volatile AsyncFutureListener<Object> _listener;
    private volatile boolean _triggeredListener;

    public RemoteOperationFutureListener(Logger logger, AsyncFutureListener<Object> listener) {
        this(logger, listener, true);
    }

    public RemoteOperationFutureListener(Logger logger, AsyncFutureListener<Object> listener, boolean getResultOnCompletion) {
        this._logger = logger;
        this._listener = listener;
        this._getResultOnCompletion = getResultOnCompletion;
        this._completionLatch = new CountDownLatch(1);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        // This future implementation cannot be cancelled.
        return false;
    }

    @Override
    public boolean isCancelled() {
        // This future implementation cannot be cancelled.
        return false;
    }

    @Override
    public boolean isDone() {
        return _completionLatch.getCount() == 0;
    }

    @Override
    public Object get() throws InterruptedException, ExecutionException {
        _completionLatch.await();
        if (_exception != null)
            throw _exception;
        return _result;
    }

    @Override
    public Object get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException, ExecutionException {
        if (!_completionLatch.await(timeout, unit))
            throw new TimeoutException();
        if (_exception != null)
            throw _exception;
        return _result;
    }

    @Override
    public void setListener(final AsyncFutureListener<Object> listener) {
        if (listener == null)
            return;
        this._listener = listener;
        if (isDone() && !_triggeredListener) {
            LRMIRuntime.getRuntime().getThreadPool().execute(new ContextClassLoaderRunnable() {
                @Override
                protected void execute() {
                    //Make sure listener is triggered only once
                    synchronized (_completionLatch) {
                        if (_triggeredListener)
                            return;

                        _triggeredListener = true;
                    }
                    invokeListener(listener);
                }
            });
        }
    }

    public void waitForCompletion() throws InterruptedException {
        this._completionLatch.await();
        if (_exception != null) {
            final Throwable executionException = _exception.getCause();
            if (executionException instanceof RuntimeException)
                throw (RuntimeException) executionException;
            throw new ProxyInternalSpaceException(executionException);
        }
    }

    public boolean waitForCompletion(long timeout, TimeUnit unit)
            throws InterruptedException {
        return _completionLatch.await(timeout, unit);
    }

    public void onOperationCompletion(RemoteOperationRequest<T> request, RemoteOperationsExecutorProxy sourceProxy) {
        AsyncFutureListener<Object> listener = null;
        // Synchronize result processing to protect concurrect execution.
        synchronized (_completionLatch) {
            // If already completed, ignore this result:
            if (isDone()) {
                if (_logger.isLoggable(Level.FINEST))
                    _logger.log(Level.FINEST, "Operation already completed, ignoring " + request.getRemoteOperationResult() + ((sourceProxy != null) ? " from " + sourceProxy.toLogMessage(request) : ""));
                return;
            }

            boolean isCompleted;

            try {
                if (_logger.isLoggable(Level.FINEST))
                    _logger.log(Level.FINEST, "Received " + request.getRemoteOperationResult() + ((sourceProxy != null) ? " from " + sourceProxy.toLogMessage(request) : ""));
                isCompleted = onOperationResultArrival(request);
                if (isCompleted && _getResultOnCompletion)
                    this._result = getResult(request);
            } catch (ExecutionException e) {
                isCompleted = true;
                this._exception = e;
            } catch (Exception e) {
                isCompleted = true;
                this._exception = new ExecutionException(e);
            }

            if (isCompleted) {
                _completionLatch.countDown();
                //Make sure listener is triggered only once
                if (!_triggeredListener) {
                    listener = _listener;
                    if (listener != null)
                        _triggeredListener = true;
                }
            }
        }
        //Trigger listener outside of lock, avoid potential deadlocks
        if (listener != null) {
            //Issue GS-13614 - PostponedAsyncOperationsQueue should delegate the invoke listener to an LRMI thread
            if(Thread.currentThread().getName().contains(THREADS_NAME_PREFIX)) {
                final AsyncFutureListener<Object> listnr = listener;
                LRMIRuntime.getRuntime().getThreadPool().execute(new ContextClassLoaderRunnable() {
                    @Override
                    public void execute() {
                        invokeListener(listnr);
                    }
                });
            }
            else {
                invokeListener(listener);
            }
        }
    }

    protected boolean onOperationResultArrival(RemoteOperationRequest<T> request) {
        return true;
    }

    protected Object getResult(RemoteOperationRequest<T> request) throws Exception {
        return request.getAsyncFinalResult();
    }

    private void invokeListener(AsyncFutureListener<Object> listener) {
        try {
            Exception exception = _exception == null ? null : (Exception) _exception.getCause();
            listener.onResult(new DefaultAsyncResult<Object>(_result, exception));
        } catch (Exception e) {
            // TOLOG LB: log listener invocation exception
        }
    }

}
