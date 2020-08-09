package com.gigaspaces.client.iterator.cursor;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.iterator.ServerIteratorRequestInfo;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.security.authorities.SpaceAuthority;
import com.j_spaces.core.ServerIteratorAnswerHolder;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.UnknownTypeException;
import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;
import java.util.UUID;

@com.gigaspaces.api.InternalApi
public class EmbeddedSpaceIteratorBatchResultsManager implements SpaceIteratorBatchProvider {
    private final UUID iteratorId;
    private final int batchSize;
    private final SpaceImpl spaceImpl;
    private final ITemplatePacket queryPacket;
    private final int readModifiers;
    private final long maxInactiveDuration;
    private final SpaceContext spaceContext;
    private int batchNumber;

    public EmbeddedSpaceIteratorBatchResultsManager(ISpaceProxy spaceProxy, int batchSize, int readModifiers, ITemplatePacket queryPacket, long maxInactiveDuration) {
        this.batchSize = batchSize;
        this.iteratorId = UUID.randomUUID();
        this.spaceImpl = spaceProxy.getDirectProxy().getSpaceImplIfEmbedded();
        this.queryPacket = queryPacket;
        this.readModifiers = readModifiers;
        this.maxInactiveDuration = maxInactiveDuration;
        try {
            this.spaceContext = spaceProxy.getDirectProxy().getSecurityManager().acquireContext(spaceImpl);
            // Since getNextBatch directly triggers an internal API, we explicitly call beforeTypeOperation to assert security, quiesce, etc.
            spaceImpl.beforeTypeOperation(true, spaceContext, SpaceAuthority.SpacePrivilege.READ, queryPacket.getTypeName());
        } catch (RemoteException e) {
            throw new IllegalStateException("Failed to acquire space context for iterator", e);
        }
    }

    @Override
    public Object[] getNextBatch() throws InterruptedException {
        ServerIteratorAnswerHolder answer;
        try {
            answer = spaceImpl.getNextBatchFromServerIterator(queryPacket, spaceContext, readModifiers,
                        new ServerIteratorRequestInfo(iteratorId, batchSize, batchNumber, maxInactiveDuration));
        } catch (TransactionException | UnusableEntryException | UnknownTypeException | RemoteException e) {
            throw new IllegalStateException("Failed to get next batch", e);
        }
        batchNumber++;
        IEntryPacket[] entryPackets = answer.getEntryPackets();
        // TODO: is this required?
        //if (entryPackets.length < batchSize)
        //    close();
        return entryPackets.length != 0 ? entryPackets : null;
    }

    @Override
    public void close() {
        spaceImpl.closeServerIterator(iteratorId);
    }
}
