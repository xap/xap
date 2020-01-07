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

package com.gigaspaces.internal.client.spaceproxy.actions;

import com.gigaspaces.internal.client.SpaceIteratorBatchResult;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.GetBatchForIteratorProxyActionInfo;
import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;

/**
 * @author GigaSpaces
 */
public abstract class GetBatchForIteratorProxyAction<TSpaceProxy extends ISpaceProxy> {
    public abstract SpaceIteratorBatchResult getNextBatch(TSpaceProxy spaceProxy, GetBatchForIteratorProxyActionInfo actionInfo)
            throws RemoteException, TransactionException, UnusableEntryException;

    /**
     * Rethrow Exception as typed exception.
     */
    protected void rethrowException(Throwable exception)
            throws UnusableEntryException, TransactionException, RemoteException {
        if (exception instanceof UnusableEntryException)
            throw (UnusableEntryException) exception;

        if (exception instanceof TransactionException)
            throw (TransactionException) exception;

        if (exception instanceof RemoteException)
            throw (RemoteException) exception;

        if (exception instanceof RuntimeException)
            throw (RuntimeException) exception;

        throw new RuntimeException(exception);
    }
}
