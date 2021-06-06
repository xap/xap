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
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.space.requests.*;
import com.gigaspaces.internal.space.responses.BroadcastTableSpaceResponseInfo;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.UnknownTypesException;
import com.j_spaces.core.client.Modifiers;
import net.jini.core.lease.Lease;
import net.jini.core.transaction.TransactionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.concurrent.ExecutionException;
/**
 * @author alon shoham
 * @since 15.8.0
 */
@com.gigaspaces.api.InternalApi
public class BroadcastTableHandler {
    private Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_BROADCAST_TABLE);
    private final SpaceImpl space;
    private final IDirectSpaceProxy _proxy;

    public BroadcastTableHandler(SpaceImpl space) throws RemoteException {
        this.space = space;
        _proxy = space.getSpaceProxy().getDirectProxy();
    }

    public void pushEntry(IEntryPacket entryPacket, long lease, boolean isUpdate, long timeout, int modifiers) {
        executeTask(new PushBroadcastTableEntrySpaceRequestInfo(entryPacket, lease, isUpdate, timeout, modifiers), null);
    }

    public void pushEntries(IEntryPacket[] entryPackets, long lease, long[] leases, long timeout, int modifiers) {
        executeTask(new PushBroadcastTableEntriesSpaceRequestInfo(entryPackets, lease, leases, timeout, modifiers), null);
    }

    public boolean pullEntries(String typeName, Integer targetPartitionId) {
        BroadcastTableSpaceResponseInfo result = executeTask(new PullBroadcastTableEntriesSpaceRequestInfo(typeName), targetPartitionId);
        if(result != null && result.getEntries() != null) {
            try {
                space.write(result.getEntries(), null, Lease.FOREVER, null, new SpaceContext(), 0, Modifiers.BACKUP_ONLY, true);
                return true;
            } catch (RemoteException | TransactionException | UnknownTypesException e) {
                handleException(e, BroadcastTableSpaceRequestInfo.Action.PULL_ENTRIES);
            }
        }
        return false;
    }

    public void clearEntries(ITemplatePacket template, int modifiers) {
        executeTask(new ClearBroadcastTableEntriesSpaceRequestInfo(template, modifiers), null);
    }

    private BroadcastTableSpaceResponseInfo executeTask(BroadcastTableSpaceRequestInfo broadcastTableSpaceRequestInfo, Object routing) {
        try {
            final AsyncFuture<BroadcastTableSpaceResponseInfo> future = _proxy.execute(new BroadcastTableSpaceTask(broadcastTableSpaceRequestInfo), routing, null, null);
            BroadcastTableSpaceResponseInfo result = future.get();
            if(result.finishedSuccessfully()) {
                return result;
            }
            result.getExceptionMap().forEach((key, value) -> {
                if (_logger.isErrorEnabled()) {
                    _logger.error("Broadcast table operation " + broadcastTableSpaceRequestInfo.getAction() + " failed in partition " + key + ". " +
                            "Notice that broadcast table data is not up to date in this partition", value);
                }
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            handleException(e, broadcastTableSpaceRequestInfo.getAction());
        } catch (RemoteException | ExecutionException | TransactionException e) {
            handleException(e, broadcastTableSpaceRequestInfo.getAction());
        }
        return null;
    }

    private void handleException(Exception e, BroadcastTableSpaceRequestInfo.Action action){
        if (_logger.isErrorEnabled()) {
            _logger.error("Broadcast table operation " + action + " failed. " +
                    "Notice that broadcast table data is not up to date in this partition", e);
        }
    }
}
