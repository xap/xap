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

import com.gigaspaces.annotation.lrmi.AsyncRemoteCall;
import com.gigaspaces.annotation.lrmi.CustomTracking;
import com.gigaspaces.annotation.lrmi.OneWayRemoteCall;
import com.gigaspaces.internal.cluster.PartitionToChunksMap;

import java.rmi.RemoteException;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
public interface RemoteOperationsExecutor {
    boolean isActive() throws RemoteException;

    boolean isActiveAndNotSuspended() throws RemoteException;

    PartitionToChunksMap checkChunkMapGeneration(int clientGeneration);

    /***
     * @since 9.0.1
     */
    @AsyncRemoteCall
    Boolean isActiveAsync() throws RemoteException;

    @CustomTracking
    <T extends RemoteOperationResult> T executeOperation(RemoteOperationRequest<T> request)
            throws RemoteException;

    @CustomTracking
    @AsyncRemoteCall
    <T extends RemoteOperationResult> T executeOperationAsync(RemoteOperationRequest<T> request)
            throws RemoteException;

    /**
     * @since 9.1
     */
    @CustomTracking
    @OneWayRemoteCall
    void executeOperationOneway(RemoteOperationRequest<?> request)
            throws RemoteException;
}
