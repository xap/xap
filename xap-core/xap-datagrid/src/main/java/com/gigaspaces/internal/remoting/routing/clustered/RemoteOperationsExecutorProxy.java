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
import com.gigaspaces.internal.quiesce.QuiesceTokenProvider;
import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.remoting.RemoteOperationResult;
import com.gigaspaces.internal.remoting.RemoteOperationsExecutor;
import com.gigaspaces.lrmi.ILRMIProxy;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.lrmi.nio.async.FutureContext;

import java.rmi.RemoteException;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class RemoteOperationsExecutorProxy {
    private final String _name;
    private final RemoteOperationsExecutor _executor;
    private final QuiesceTokenProvider _quiesceTokenProvider;

    public RemoteOperationsExecutorProxy(String name, RemoteOperationsExecutor executor, QuiesceTokenProvider quiesceTokenProvider) {
        this._name = name;
        this._executor = executor;
        this._quiesceTokenProvider = quiesceTokenProvider;
    }

    public String getName() {
        return _name;
    }

    public RemoteOperationsExecutor getExecutor() {
        return _executor;
    }

    public <T extends RemoteOperationResult> T execute(RemoteOperationRequest<T> request)
            throws RemoteException {
        if (request.isBlockingOperation())
            LRMIInvocationContext.enableCallbackModeForNextInvocation();
        if (request.isDedicatedPoolRequired())
            LRMIInvocationContext.enableCustomPriorityForNextInvocation();
        if (request.isDirectExecutionEnabled())
            LRMIInvocationContext.enableDirectPriorityForNextInvocation();
        return _executor.executeOperation(request);
    }

    public <T extends RemoteOperationResult> void executeAsync(RemoteOperationRequest<T> request, AsyncFutureListener<T> listener)
            throws RemoteException {
        try {
            if (request.isBlockingOperation())
                LRMIInvocationContext.enableCallbackModeForNextInvocation();
            if (request.isDedicatedPoolRequired())
                LRMIInvocationContext.enableCustomPriorityForNextInvocation();
            if (request.isDirectExecutionEnabled())
                LRMIInvocationContext.enableDirectPriorityForNextInvocation();
            FutureContext.setFutureListener(listener);
            _executor.executeOperationAsync(request);
        } finally {
            FutureContext.clear();
        }
    }

    public void executeOneway(RemoteOperationRequest<?> request) throws RemoteException {
        _executor.executeOperationOneway(request);
    }

    public boolean isActive() throws RemoteException {
        return _executor.isActive();
    }

    public boolean isActiveQuiesceTokenAware() throws RemoteException {
        if (_quiesceTokenProvider.getToken() != null) {
            return _executor.isActive();
        } else {
            return _executor.isActiveAndNotSuspended();
        }
    }

    public String toLogMessage(RemoteOperationRequest<?> request) {
        return _name + "=>" + request.toString();
    }

    public void close() {
        if (_executor instanceof ILRMIProxy)
            ((ILRMIProxy) _executor).closeProxy();
    }

    public static boolean isAvailable(RemoteOperationsExecutorProxy proxy, boolean activeOnly) {
        if (proxy == null)
            return false;

        try {
            boolean isActive = proxy.isActiveQuiesceTokenAware();
            return (isActive || !activeOnly);
        } catch (RemoteException e) {
            return false;
        }
    }
}
