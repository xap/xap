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

import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.remoting.RemoteOperationFutureListener;
import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.remoting.RemoteOperationResult;
import com.gigaspaces.internal.remoting.routing.AbstractRemoteOperationRouter;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.UnknownTypeException;
import com.j_spaces.core.UnknownTypesException;
import com.j_spaces.core.exception.internal.InterruptedSpaceException;

import java.rmi.RemoteException;
import java.util.List;
import java.util.logging.Level;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class ClusterRemoteOperationRouter extends AbstractRemoteOperationRouter {
    protected enum ExecutionStatus {COMPLETED, RETRY_OTHER, RETRY_SAME}

    protected final RemoteOperationsExecutorsCluster _cluster;
    private final PostponedAsyncOperationsQueue _postponedAsyncOperationsQueue;

    public ClusterRemoteOperationRouter(RemoteOperationsExecutorsCluster cluster, PostponedAsyncOperationsQueue posponedAsyncOperationsQueue) {
        super(cluster.getName());
        this._cluster = cluster;
        this._postponedAsyncOperationsQueue = posponedAsyncOperationsQueue;
        if (_logger.isLoggable(Level.CONFIG))
            _logger.log(Level.CONFIG, "Initialized clustered router" + _cluster.getPartitionDesc() + " - members=" + _cluster.getMembersNames());
    }

    @Override
    public <T extends RemoteOperationResult> void execute(RemoteOperationRequest<T> request)
            throws InterruptedException {
        executeImpl(request, false);
    }

    private <T extends RemoteOperationResult> void executeImpl(RemoteOperationRequest<T> request, boolean oneway)
            throws InterruptedException {
        long initialFailureTime = 0;

        RemoteOperationsExecutorProxy proxy = _cluster.getLoadBalancer().getCandidate(request);
        if (proxy == null) {
            initialFailureTime = SystemTime.timeMillis();
            if (_logger.isLoggable(Level.WARNING))
                _logger.log(Level.WARNING, "No active server" + _cluster.getPartitionDesc() + " - attempting to locate an active server...");
            proxy = _cluster.getLoadBalancer().findActiveMember(request, initialFailureTime, proxy);
            if (proxy == null)
                return;
        }

        while (true) {
            T result = null;
            Exception executionException = null;

            try {
                if (!beforeOperationExecution(request, proxy))
                    return;

                logBeforeExecute(proxy, request, oneway);
                if (oneway) {
                    proxy.executeOneway(request);
                    logAfterExecute(proxy, request, result, oneway);
                    return;
                } else {
                    result = proxy.execute(request);
                    logAfterExecute(proxy, request, result, oneway);
                }

            } catch (RemoteException e) {
                logExecutionFailure(proxy, request, e, oneway);
                executionException = e;
            } catch (InterruptedSpaceException e) {
                logInterruptedExecution(proxy, request, e, oneway);
                result = request.createRemoteOperationResult();
                result.setExecutionException(e);
            }

            if (result != null)
                executionException = result.getExecutionException();

            ExecutionStatus status = processResult(request, result, executionException);
            afterOperationExecution(request, proxy, status);

            if (status == ExecutionStatus.COMPLETED)
                return;

            logExecutionStatus("Operation", status, executionException, proxy, request);
            // Validate remaining time:
            if (initialFailureTime == 0)
                initialFailureTime = SystemTime.timeMillis();
            if (_cluster.getRemainingTime(request, initialFailureTime) <= 0) {
                if (_logger.isLoggable(Level.SEVERE))
                    _logger.log(Level.SEVERE, _cluster.generateTimeoutErrorMessage(initialFailureTime, request));
                return;
            }

            if (status == ExecutionStatus.RETRY_OTHER) {
                proxy = _cluster.getLoadBalancer().findActiveMember(request, initialFailureTime, proxy);
                if (proxy == null)
                    return;
            }
        }
    }

    private void logExecutionStatus(String prefix, ExecutionStatus status, Exception exception, RemoteOperationsExecutorProxy proxy, RemoteOperationRequest<?> request) {
        if (_logger.isLoggable(Level.FINEST))
            _logger.log(Level.FINEST, prefix +
                    " ExecutionStatus=" + status +
                    (exception != null ? " exception=" + exception : "") +
                    " for " + proxy.toLogMessage(request));
    }

    protected void afterOperationExecution(RemoteOperationRequest<?> request, RemoteOperationsExecutorProxy proxy, ExecutionStatus status) {
    }

    protected <T extends RemoteOperationResult> ExecutionStatus processResult(
            RemoteOperationRequest<T> request, T result, Exception executionException) {
        if (executionException != null) {
            if (executionException instanceof RemoteException)
                return ExecutionStatus.RETRY_OTHER;
            if (executionException instanceof UnknownTypeException)
                return request.processUnknownTypeException(null) ? ExecutionStatus.RETRY_SAME : ExecutionStatus.COMPLETED;
            if (executionException instanceof UnknownTypesException) {
                List<Integer> positions = ((UnknownTypesException) executionException).getPositions();
                return request.processUnknownTypeException(positions) ? ExecutionStatus.RETRY_SAME : ExecutionStatus.COMPLETED;
            }
        }
        request.setRemoteOperationResult(result);
        return ExecutionStatus.COMPLETED;
    }

    protected boolean beforeOperationExecution(RemoteOperationRequest<?> request, RemoteOperationsExecutorProxy proxy)
            throws RemoteException {
        return true;
    }

    @Override
    public <T extends RemoteOperationResult> RemoteOperationFutureListener<T> createFutureListener(RemoteOperationRequest<T> request, AsyncFutureListener<Object> listener) {
        return new RemoteOperationFutureListener<T>(_logger, listener);
    }

    @Override
    public <T extends RemoteOperationResult> void executeAsync(RemoteOperationRequest<T> request, RemoteOperationFutureListener<T> futureListener) {
        AsyncOperationExecutor<T> asyncExecutor = new AsyncOperationExecutor<T>(request, futureListener);
        asyncExecutor.executeAsync();
    }

    @Override
    public void executeOneway(RemoteOperationRequest<?> request) throws InterruptedException {
        executeImpl(request, true);
    }


    @Override
    public RemoteOperationsExecutorProxy getCachedMember() {
        return _cluster.getLoadBalancer().getCandidate(null);
    }

    @Override
    public RemoteOperationsExecutorProxy getAnyAvailableMember() {
        return _cluster.getLoadBalancer().findAnyAvailableMember(false);
    }

    @Override
    public RemoteOperationsExecutorProxy getAnyActiveMember() {
        return _cluster.getLoadBalancer().findAnyAvailableMember(true);
    }

    @Override
    public void getAllAvailableMembers(List<RemoteOperationsExecutorProxy> availableMembers) {
        _cluster.getAllAvailableMembers(availableMembers);
    }

    @Override
    public void close() {
        _cluster.close();
    }

    public class AsyncOperationExecutor<T extends RemoteOperationResult> implements AsyncFutureListener<T> {
        private final RemoteOperationRequest<T> _request;
        private final RemoteOperationFutureListener<T> _futureListener;

        private volatile RemoteOperationsExecutorProxy _proxy;
        private volatile ExecutionStatus _lastExecutionStatus;
        private volatile long _initialFailureTime;

        public AsyncOperationExecutor(RemoteOperationRequest<T> request, RemoteOperationFutureListener<T> listener) {
            this._request = request;
            this._futureListener = listener;
            this._proxy = _cluster.getLoadBalancer().getCandidate(request);
        }

        public void executeAsync() {
            if (_proxy == null || _lastExecutionStatus == ExecutionStatus.RETRY_OTHER) {
                if (_initialFailureTime == 0)
                    _initialFailureTime = SystemTime.timeMillis();
                _proxy = _cluster.getLoadBalancer().findActiveMemberUninterruptibly(_request, _initialFailureTime, _proxy);
                if (_proxy == null) {
                    onOperationCompletion();
                    return;
                }
            }

            while (true) {
                try {
                    if (!beforeOperationExecution(_request, _proxy)) {
                        onOperationCompletion();
                        return;
                    }

                    logBeforeExecuteAsync(_proxy, _request);
                    _proxy.executeAsync(_request, this);
                    return;
                } catch (RemoteException e) {
                    logAsyncExecutionFailure(_proxy, _request, e);

                    if (_initialFailureTime == 0)
                        _initialFailureTime = SystemTime.timeMillis();

                    _proxy = _cluster.getLoadBalancer().findActiveMemberUninterruptibly(_request, _initialFailureTime, _proxy);
                    if (_proxy == null) {
                        onOperationCompletion();
                        return;
                    }
                } catch (InterruptedSpaceException e) {
                    logInterruptedAsyncExecution(_proxy, _request, e);
                    _request.setRemoteOperationExecutionError((Exception) e.getCause());
                    onOperationCompletion();
                    return;
                } catch (Exception e) {
                    logUnexpectedAsyncExecution(_proxy, _request, e);
                    _request.setRemoteOperationExecutionError(e);
                    onOperationCompletion();
                    return;
                }
            }
        }

        @Override
        public void onResult(AsyncResult<T> asyncResult) {
            try {
                final T result = asyncResult.getResult();
                Exception exception = asyncResult.getException();
                if (exception != null)
                    logAsyncExecutionFailure(_proxy, _request, exception);
                else
                    logAfterExecuteAsync(_proxy, _request, result);

                if (exception == null)
                    exception = result != null ? result.getExecutionException() : new IllegalStateException("Async operation completed without result or exception");

                final ExecutionStatus status = processResult(_request, result, exception);
                afterOperationExecution(_request, _proxy, status);

                if (status == ExecutionStatus.COMPLETED) {
                    if (result == null)
                        _request.setRemoteOperationExecutionError(exception);
                    onOperationCompletion();
                    return;
                }

                logExecutionStatus("Async operation", status, exception, _proxy, _request);
                _lastExecutionStatus = status;
                _postponedAsyncOperationsQueue.enqueue(this);
            } catch (Exception e) {
                if (_logger.isLoggable(Level.SEVERE))
                    _logger.log(Level.SEVERE, "Unexpected exception during processing of async operation result. AsyncResult=" + asyncResult.toString() + ", request=" + _request.toString(), e);
                _request.setRemoteOperationExecutionError(e);
                onOperationCompletion();
            }
        }

        private void onOperationCompletion() {
            _futureListener.onOperationCompletion(_request, _proxy);
        }
    }
}
