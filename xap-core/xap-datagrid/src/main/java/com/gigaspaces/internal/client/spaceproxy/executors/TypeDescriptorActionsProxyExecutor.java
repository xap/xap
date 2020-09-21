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
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.space.actions.GetTypeDescriptorActionInfo;
import com.gigaspaces.internal.space.requests.AddTypeIndexesRequestInfo;
import com.gigaspaces.internal.space.requests.RegisterTypeDescriptorRequestInfo;
import com.gigaspaces.internal.space.requests.UnregisterTypeDescriptorRequestInfo;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.gigaspaces.metadata.index.AddTypeIndexesResult;

import java.rmi.RemoteException;

/**
 * @author Niv Ingberg
 * @since 8.0
 */
public abstract class TypeDescriptorActionsProxyExecutor<TSpaceProxy extends ISpaceProxy> {
    public abstract ITypeDesc getTypeDescriptor(TSpaceProxy spaceProxy, GetTypeDescriptorActionInfo actionInfo)
            throws RemoteException;

    public abstract void registerTypeDescriptor(TSpaceProxy spaceProxy, RegisterTypeDescriptorRequestInfo actionInfo)
            throws RemoteException;

    public abstract ITypeDesc registerTypeDescriptor(TSpaceProxy spaceProxy, Class<?> type)
            throws RemoteException;

    public abstract AsyncFuture<AddTypeIndexesResult> asyncAddIndexes(TSpaceProxy spaceProxy, AddTypeIndexesRequestInfo requestInfo)
            throws RemoteException;

    public abstract void unregisterTypeDescriptor(TSpaceProxy spaceProxy, UnregisterTypeDescriptorRequestInfo actionInfo)
            throws RemoteException;

    protected static void assertEquivalent(ITypeDesc typeDesc1, ITypeDesc typeDesc2) {
        // TODO: Implement this method properly, with more tests and better descriptions.
        if (typeDesc1.getChecksum() != typeDesc2.getChecksum())
            throw new SpaceMetadataException("Type descriptors are not equivalent.");
    }
}
