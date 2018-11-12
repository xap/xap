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

package com.gigaspaces.internal.remoting.routing.embedded;

import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.internal.remoting.RemoteOperationFutureListener;
import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.remoting.RemoteOperationResult;
import com.gigaspaces.internal.remoting.RemoteOperationsExecutor;
import com.gigaspaces.internal.remoting.routing.AbstractRemoteOperationRouter;
import com.gigaspaces.internal.remoting.routing.clustered.RemoteOperationsExecutorProxy;
import com.gigaspaces.internal.utils.concurrent.ContextClassLoaderRunnable;

import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.logging.Level;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class EmbeddedRemoteOperationRouter extends AbstractRemoteOperationRouter {
    protected final RemoteOperationsExecutorProxy _memberProxy;
    private final int _partitionId;
    private final String _partitionDesc;
    private final String _name;
    protected final RemoteOperationsExecutor _executor;
    private final Executor _asyncExecutor;

    public EmbeddedRemoteOperationRouter(RemoteOperationsExecutorProxy memberProxy, int partitionId, String name, Executor asyncExecutor) {
        super(name);
        this._name = name;
        this._memberProxy = memberProxy;
        this._partitionId = partitionId;
        this._partitionDesc = _partitionId == -1 ? "" : " for partition #" + (_partitionId + 1);
        this._executor = memberProxy.getExecutor();
        this._asyncExecutor = asyncExecutor;

        if (_logger.isLoggable(Level.CONFIG))
            _logger.log(Level.CONFIG, "Initialized embedded router" + getPartitionDesc() + " - member=[" + memberProxy.getName() + "]");
    }

    public RemoteOperationsExecutor getExecutor() {
        return _executor;
    }

    @Override
    public <T extends RemoteOperationResult> void execute(RemoteOperationRequest<T> request)
            throws InterruptedException {
        executeImpl(request, false);
    }

    private <T extends RemoteOperationResult> void executeImpl(RemoteOperationRequest<T> request, boolean oneway) {
        if (!beforeOperationExecution(request))
            return;

        try {
            T result = null;
            logBeforeExecute(_memberProxy, request, oneway);
            if (oneway)
                _executor.executeOperationOneway(request);
            else
                result = _executor.executeOperation(request);
            logAfterExecute(_memberProxy, request, result, oneway);
            request.setRemoteOperationResult(result);
            afterOperationExecution(request);
        } catch (RemoteException e) {
            logExecutionFailure(_memberProxy, request, e, oneway);
            request.setRemoteOperationExecutionError(e);
        }
    }

    protected boolean beforeOperationExecution(RemoteOperationRequest<?> request) {
        return true;
    }

    protected void afterOperationExecution(RemoteOperationRequest<?> request) {
    }

    @Override
    public <T extends RemoteOperationResult> RemoteOperationFutureListener<T> createFutureListener(RemoteOperationRequest<T> request, AsyncFutureListener<Object> listener) {
        return new RemoteOperationFutureListener<T>(_logger, listener);
    }

    @Override
    public <T extends RemoteOperationResult> void executeAsync(RemoteOperationRequest<T> request, RemoteOperationFutureListener<T> futureListener) {
        if (!beforeOperationExecution(request)) {
            futureListener.onOperationCompletion(request, _memberProxy);
            return;
        }

        logBeforeExecuteAsync(_memberProxy, request);
        _asyncExecutor.execute(new AsyncOperationExecutor<T>(request, futureListener));
    }

    @Override
    public void executeOneway(RemoteOperationRequest<?> request)
            throws InterruptedException {
        executeImpl(request, true);
    }

    @Override
    public RemoteOperationsExecutorProxy getCachedMember() {
        return _memberProxy;
    }

    @Override
    public RemoteOperationsExecutorProxy getAnyAvailableMember() {
        return _memberProxy;
    }

    @Override
    public RemoteOperationsExecutorProxy getAnyActiveMember() {
        try {
            return _memberProxy.isActiveQuiesceTokenAware() ? _memberProxy : null;
        } catch (RemoteException e) {
            return null;
        }
    }

    @Override
    public void getAllAvailableMembers(List<RemoteOperationsExecutorProxy> availableMembers) {
        availableMembers.add(_memberProxy);
    }

    @Override
    public void close() {
    }

    private String getPartitionDesc() {
        return _partitionDesc;
    }

    public int getPartitionId() {
        return _partitionId;
    }

    public String getName() {
        return _name;
    }

    private class AsyncOperationExecutor<T extends RemoteOperationResult> extends ContextClassLoaderRunnable {
        private final RemoteOperationRequest<T> _request;
        private final RemoteOperationFutureListener<T> _futureListener;

        public AsyncOperationExecutor(RemoteOperationRequest<T> request, RemoteOperationFutureListener<T> listener) {
            this._request = request;
            this._futureListener = listener;
        }

        @Override
        protected void execute() {
            try {
                T result = _executor.executeOperation(_request);
                logAfterExecuteAsync(_memberProxy, _request, result);
                _request.setRemoteOperationResult(result);
                afterOperationExecution(_request);
            } catch (RemoteException e) {
                logAsyncExecutionFailure(_memberProxy, _request, e);
                _request.setRemoteOperationExecutionError(e);
            }

            if (_futureListener != null)
                _futureListener.onOperationCompletion(_request, _memberProxy);
        }
    }
}
