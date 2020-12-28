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
package com.gigaspaces.internal.server.space.broadcast_table;

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.executors.BroadcastTableSpaceTask;
import com.gigaspaces.internal.space.requests.BroadcastTableSpaceRequestInfo;
import com.gigaspaces.internal.space.requests.PushBroadcastTableEntriesSpaceRequestInfo;
import com.gigaspaces.internal.space.requests.PushBroadcastTableEntrySpaceRequestInfo;
import com.gigaspaces.internal.space.responses.BroadcastTableSpaceResponseInfo;
import com.gigaspaces.internal.transport.IEntryPacket;
import net.jini.core.transaction.TransactionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.concurrent.ExecutionException;

public class BroadcastTableHandler {
    private Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_BROADCAST_TABLE);
    private final IDirectSpaceProxy _proxy;

    public BroadcastTableHandler(IDirectSpaceProxy proxy) {
        _proxy = proxy;
    }

    public void pushEntry(IEntryPacket entryPacket, long lease, boolean isUpdate, long timeout, int modifiers) throws RemoteException {
        executeTask(new PushBroadcastTableEntrySpaceRequestInfo(entryPacket, lease, isUpdate, timeout, modifiers));
    }

    public void pushEntries(IEntryPacket[] entryPackets, long lease, long[] leases, long timeout, int modifiers) throws RemoteException {
        executeTask(new PushBroadcastTableEntriesSpaceRequestInfo(entryPackets, lease, leases, timeout, modifiers));
    }

    private void executeTask(BroadcastTableSpaceRequestInfo broadcastTableSpaceRequestInfo) throws RemoteException {
        try {
            final AsyncFuture future = _proxy.execute(new BroadcastTableSpaceTask(broadcastTableSpaceRequestInfo), null, null, null);
            BroadcastTableSpaceResponseInfo result = (BroadcastTableSpaceResponseInfo) future.get();
            if(result.finishedSuccessfully())
                return;
            result.getExceptionMap().forEach((key, value) -> {
                if (_logger.isWarnEnabled()) {
                    _logger.warn("Broadcast table operation " + broadcastTableSpaceRequestInfo.getAction() + " failed in partition " + key + ". " +
                            "Notice that broadcast table data is not up to date in this partition", value);
                }
            });
        } catch (InterruptedException e) {
            //log
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new RemoteException(e.getMessage(), e);
        } catch (TransactionException e) {
            // can never happen, transactions are blocked
        } catch (RemoteException e) {
            throw e;
        }
        return;
    }
}
