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

package com.gigaspaces.internal.client.spaceproxy.executors;

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.space.actions.GetTypeDescriptorActionInfo;
import com.gigaspaces.internal.space.requests.AddTypeIndexesRequestInfo;
import com.gigaspaces.internal.space.requests.RegisterTypeDescriptorRequestInfo;
import com.gigaspaces.internal.space.responses.AddTypeIndexesResponseInfo;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.gigaspaces.metadata.index.AddTypeIndexesResult;

import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceProxyImplTypeDescriptorActionsExecutor extends TypeDescriptorActionsProxyExecutor<SpaceProxyImpl> {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_SPACE_TYPEMANAGER);

    @Override
    public ITypeDesc getTypeDescriptor(SpaceProxyImpl spaceProxy, GetTypeDescriptorActionInfo actionInfo) throws RemoteException {
        return spaceProxy.getTypeManager().getTypeDescByNameIfExists(actionInfo.typeName);
    }

    @Override
    public void registerTypeDescriptor(SpaceProxyImpl spaceProxy, RegisterTypeDescriptorRequestInfo requestInfo)
            throws RemoteException {
        spaceProxy.getTypeManager().registerTypeDesc(requestInfo.typeDescriptor);
    }

    @Override
    public ITypeDesc registerTypeDescriptor(SpaceProxyImpl spaceProxy, Class<?> type)
            throws RemoteException {
        // This is explictly by name - achieves the same goal as by type.
        return spaceProxy.getTypeManager().getTypeDescByName(type.getName());
    }

    @Override
    public AsyncFuture<AddTypeIndexesResult> asyncAddIndexes(SpaceProxyImpl spaceProxy, AddTypeIndexesRequestInfo requestInfo) throws RemoteException {
        // Validate type is registered:
        ITypeDesc typeDesc = spaceProxy.getTypeDescriptor(requestInfo.getTypeName());
        if (typeDesc == null)
            throw new SpaceMetadataException("Cannot add indexes  - type is not registered [" + requestInfo.getTypeName() + "].");

        AsyncFuture<AddTypeIndexesResult> result;
        try {
            result = spaceProxy.execute(new AddTypeIndexesTask(requestInfo), null, null, null);
            // Set listener:
            result.setListener(new AsyncAddIndexesListener(spaceProxy, requestInfo));
            // Return result:
            return result;
        } catch (TransactionException e) {
            throw new SpaceMetadataException("Error adding new index to type [" + requestInfo.getTypeName() + "]", e);
        }
    }

    private static class AsyncAddIndexesListener implements AsyncFutureListener<AddTypeIndexesResult> {
        private final SpaceProxyImpl _spaceProxy;
        private final AddTypeIndexesRequestInfo _requestInfo;

        public AsyncAddIndexesListener(SpaceProxyImpl spaceProxy, AddTypeIndexesRequestInfo requestInfo) {
            this._spaceProxy = spaceProxy;
            this._requestInfo = requestInfo;
        }

        public void onResult(AsyncResult<AddTypeIndexesResult> result) {
            try {
                // If execution finished without a problem, process results:
                if (result.getException() == null) {
                    AddTypeIndexesResponseInfo responseInfo = (AddTypeIndexesResponseInfo) result.getResult();
                    if (responseInfo.getMetadataException() == null) {
                        synchronized (_spaceProxy.getTypeManager().getLockObject()) {
                            // Register updated type descriptor in proxy:
                            for (ITypeDesc updatedTypeDesc : responseInfo.getUpdatedTypeDescriptors())
                                _spaceProxy.getTypeManager().registerTypeDesc(updatedTypeDesc);
                            // Clean Query manager cache so updated type descriptors can be used:
                            _spaceProxy.getQueryManager().clean();
                        }
                    }
                }
            } catch (RuntimeException e) {
                if (_logger.isErrorEnabled()) {
                    _logger.error("Failed to add index.", e);
                }
                throw e;
            } finally {
                // If user supplied a listener, invoke it:
                if (_requestInfo.getListener() != null)
                    _requestInfo.getListener().onResult(result);
            }
        }
    }
}
