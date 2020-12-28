package com.gigaspaces.internal.server.space.broadcast_table;

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.executors.BroadcastTableSpaceTask;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.space.requests.BroadcastTableSpaceRequestInfo;
import com.gigaspaces.internal.space.requests.PullBroadcastTableEntriesSpaceRequestInfo;
import com.gigaspaces.internal.space.requests.PushBroadcastTableEntriesSpaceRequestInfo;
import com.gigaspaces.internal.space.requests.PushBroadcastTableEntrySpaceRequestInfo;
import com.gigaspaces.internal.space.responses.BroadcastTableSpaceResponseInfo;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.UnknownTypesException;
import com.j_spaces.core.client.Modifiers;
import net.jini.core.lease.Lease;
import net.jini.core.transaction.TransactionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.concurrent.ExecutionException;

public class BroadcastTableHandler {
    private Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_BROADCAST_TABLE);
    private final SpaceImpl space;
    private final IDirectSpaceProxy _proxy;

    public BroadcastTableHandler(SpaceImpl space) throws RemoteException {
        this.space = space;
        _proxy = space.getSpaceProxy().getDirectProxy();
    }

    public void pushEntry(IEntryPacket entryPacket, long lease, boolean isUpdate, long timeout, int modifiers) throws RemoteException {
        executeTask(new PushBroadcastTableEntrySpaceRequestInfo(entryPacket, lease, isUpdate, timeout, modifiers), null);
    }

    public void pushEntries(IEntryPacket[] entryPackets, long lease, long[] leases, long timeout, int modifiers) throws RemoteException {
        executeTask(new PushBroadcastTableEntriesSpaceRequestInfo(entryPackets, lease, leases, timeout, modifiers), null);
    }

    public boolean pullEntries(String typeName, Integer targetPartitionId) {
        try {
            BroadcastTableSpaceResponseInfo result = executeTask(new PullBroadcastTableEntriesSpaceRequestInfo(typeName), targetPartitionId);
            if(result!= null && result.getEntries() != null)
                try {
                    space.write(result.getEntries(), null, Lease.FOREVER, null, null, 0, Modifiers.BACKUP_ONLY, true);
                    return true;
                } catch (TransactionException | UnknownTypesException e) {
                    return false;
                }
        } catch (RemoteException e) {
            e.printStackTrace();
        }
        return false;
    }

    private BroadcastTableSpaceResponseInfo executeTask(BroadcastTableSpaceRequestInfo broadcastTableSpaceRequestInfo, Object routing) throws RemoteException {
        try {
            final AsyncFuture<BroadcastTableSpaceResponseInfo> future = _proxy.execute(new BroadcastTableSpaceTask(broadcastTableSpaceRequestInfo), routing, null, null);
            BroadcastTableSpaceResponseInfo result = future.get();
            if(result.finishedSuccessfully()) {
                return result;
            }
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
        return null;
    }
}
