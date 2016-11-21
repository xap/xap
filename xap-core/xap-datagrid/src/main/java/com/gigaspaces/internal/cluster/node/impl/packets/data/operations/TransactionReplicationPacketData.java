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

package com.gigaspaces.internal.cluster.node.impl.packets.data.operations;

import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.handlers.ITransactionInContext;
import com.gigaspaces.internal.cluster.node.impl.ReplicationMultipleOperationType;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IExecutableReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationTransactionalPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketDataMediator;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.transaction.TransactionUniqueId;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.cluster.IReplicationFilterEntry;

import net.jini.core.transaction.server.ServerTransaction;
import net.jini.core.transaction.server.TransactionParticipantDataImpl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.LinkedList;


/**
 * @deprecated since 9.0.0 - see {@link TransactionOnePhaseReplicationPacketData}
 */
@Deprecated
@com.gigaspaces.api.InternalApi
public class TransactionReplicationPacketData
        extends LinkedList<IReplicationTransactionalPacketEntryData>
        implements
        IExecutableReplicationPacketData<IReplicationTransactionalPacketEntryData>,
        ITransactionInContext {

    private static final long serialVersionUID = 1L;

    private TransactionParticipantDataImpl _metaData;

    private transient int _weight;

    public TransactionReplicationPacketData() {
    }

    @Override
    public OperationID getOperationID() {
        return null;
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _metaData = IOUtils.readObject(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            IReplicationTransactionalPacketEntryData entryData = IOUtils.readObject(in);
            add(entryData);
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, _metaData);
        out.writeInt(size());
        for (IReplicationTransactionalPacketEntryData entryData : this)
            IOUtils.writeObject(out, entryData);
    }

    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _metaData = IOUtils.readNullableSwapExternalizableObject(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            IReplicationTransactionalPacketEntryData entryData = IOUtils.readSwapExternalizableObject(in);
            add(entryData);
        }
    }

    public void writeToSwap(ObjectOutput out) throws IOException {
        IOUtils.writeNullableSwapExternalizableObject(out, _metaData);
        out.writeInt(size());
        for (IReplicationTransactionalPacketEntryData entryData : this)
            IOUtils.writeSwapExternalizableObject(out, entryData);
    }


    @Override
    public TransactionReplicationPacketData clone() {
        TransactionReplicationPacketData clone = createEmptyMultipleEntryData();
        for (IReplicationTransactionalPacketEntryData entryData : this)
            clone.add(entryData.clone());

        return clone;
    }

    public boolean isSingleEntryData() {
        return false;
    }

    public IReplicationPacketEntryData getSingleEntryData() {
        throw new UnsupportedOperationException();
    }

    public void setMetaData(TransactionParticipantDataImpl metaData) {
        _metaData = metaData;

    }

    public TransactionParticipantDataImpl getMetaData() {
        return _metaData;
    }

    @Override
    public ServerTransaction getTransaction() {
        return null;
    }

    @Override
    public ReplicationMultipleOperationType getMultipleOperationType() {
        return ReplicationMultipleOperationType.TRANSACTION_ONE_PHASE;
    }

    public void execute(IReplicationInContext context,
                        IReplicationInFacade inReplicationHandler, ReplicationPacketDataMediator dataMediator) throws Exception {
        inReplicationHandler.inTransaction(context, this);
    }

    public boolean beforeDelayedReplication() {
        return true;
    }

    public boolean supportsReplicationFilter() {
        return true;
    }

    public Iterable<IReplicationFilterEntry> toFilterEntries(
            SpaceTypeManager spaceTypeManager) {
        // Returns a very specific iterable that can only be used once
        // and iterated in a regular way without keeping the values it return
        // rather
        // all operations on the iterated object should be done during the
        // foreach loop
        // while iterating over that object only.
        // This is done to reduce garbage collection
        return new FilterIterable(iterator(), spaceTypeManager);
    }

    @Override
    public TransactionReplicationPacketData createEmptyMultipleEntryData() {
        TransactionReplicationPacketData emptyData = new TransactionReplicationPacketData();
        emptyData.setMetaData(_metaData);
        return emptyData;
    }

    @Override
    public boolean isPartOfBlobstoreBulk() {
        return false;
    }

    @Override
    public String toString() {
        TransactionUniqueId transactionId = getMetaData() != null ? getMetaData().getTransactionUniqueId() : null;
        String transactionIdAddition = transactionId != null ? "ID=" + transactionId + ", " : "";
        return "TRANSACTION [" + transactionIdAddition + getMultipleOperationType() + "]: " + super.toString();
    }

    public static class FilterIterable
            implements Iterable<IReplicationFilterEntry>,
            Iterator<IReplicationFilterEntry> {

        private final Iterator<IReplicationTransactionalPacketEntryData> _iterator;
        private final SpaceTypeManager _spaceTypeManager;
        private IReplicationFilterEntry _lastFilterEntry;

        public FilterIterable(
                Iterator<IReplicationTransactionalPacketEntryData> iterator,
                SpaceTypeManager spaceTypeManager) {
            _iterator = iterator;
            _spaceTypeManager = spaceTypeManager;
        }

        public Iterator<IReplicationFilterEntry> iterator() {
            return this;
        }

        public boolean hasNext() {
            removePreviousDiscardedIfNeeded();
            return _iterator.hasNext();
        }

        public IReplicationFilterEntry next() {
            // (protect iteration done using next only)
            removePreviousDiscardedIfNeeded();
            IReplicationTransactionalPacketEntryData next = _iterator.next();
            // We want to reduce garbage memory, so we keep reference to the
            // last
            // filtered entry and upon next hasNext call if it is discarded we
            // will
            // remove it from the transaction packet
            _lastFilterEntry = next.toFilterEntry(_spaceTypeManager);
            return _lastFilterEntry;
        }

        private void removePreviousDiscardedIfNeeded() {
            // If past filtered entry is discarded, we remove it from the packet
            if (_lastFilterEntry != null && _lastFilterEntry.isDiscarded()) {
                _iterator.remove();
                _lastFilterEntry = null;
            }
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    @Override
    public boolean isMultiParticipantData() {
        return _metaData != null && _metaData.getTransactionParticipantsCount() > 1;
    }

    @Override
    public boolean requiresRecoveryFiltering() {
        return false;
    }

    @Override
    public Object getRecoveryFilteringId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isFromGateway() {
        return false;
    }

    @Override
    public void setWeight(int weight) {
        _weight = weight;
    }

    @Override
    public int getWeight() {
        return _weight;
    }

}