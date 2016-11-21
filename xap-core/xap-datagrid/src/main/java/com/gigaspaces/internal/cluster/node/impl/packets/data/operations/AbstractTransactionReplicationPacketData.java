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

import com.gigaspaces.internal.cluster.node.handlers.ITransactionInContext;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IExecutableReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationTransactionalPacketEntryData;
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
 * Base class for transaction replication packet data
 *
 * @author eitany
 * @since 9.0
 */
public abstract class AbstractTransactionReplicationPacketData
        extends LinkedList<IReplicationTransactionalPacketEntryData>
        implements
        IExecutableReplicationPacketData<IReplicationTransactionalPacketEntryData>,
        ITransactionInContext {

    private static final long serialVersionUID = 1L;

    private TransactionParticipantDataImpl _metaData;
    private ServerTransaction _transaction;

    private boolean _fromGateway;

    private static final int FLAGS_GATEWAY = 1 << 0;

    private transient int _weight;

    public AbstractTransactionReplicationPacketData() {
    }

    public AbstractTransactionReplicationPacketData(ServerTransaction transaction, boolean fromGateway) {
        this(fromGateway);
        _transaction = transaction;
        _metaData = _transaction.getMetaData();
    }

    public AbstractTransactionReplicationPacketData(boolean fromGateway) {
        this._fromGateway = fromGateway;
    }

    @Override
    public OperationID getOperationID() {
        return null;
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        short flags = in.readShort();
        _fromGateway = (flags & FLAGS_GATEWAY) != 0;
        _metaData = IOUtils.readObject(in);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeShort(buildFlags());
        IOUtils.writeObject(out, _metaData);
    }

    private short buildFlags() {
        short flags = 0;
        if (_fromGateway)
            flags |= FLAGS_GATEWAY;
        return flags;
    }

    protected void writeTransaction(ObjectOutput out) throws IOException {
        IOUtils.writeWithCachedStubs(out, _transaction);
    }

    protected void readTransaction(ObjectInput in) throws IOException, ClassNotFoundException {
        _transaction = IOUtils.readWithCachedStubs(in);
    }

    protected void writeTransactionData(ObjectOutput out) throws IOException {
        out.writeInt(size());
        for (IReplicationTransactionalPacketEntryData entryData : this)
            IOUtils.writeObject(out, entryData);
    }

    protected void readTransactionData(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            IReplicationTransactionalPacketEntryData entryData = IOUtils.readObject(in);
            add(entryData);
        }
    }

    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        short flags = in.readShort();
        _fromGateway = (flags & FLAGS_GATEWAY) != 0;
        _metaData = IOUtils.readNullableSwapExternalizableObject(in);
        _transaction = IOUtils.readNullableSwapExternalizableObject(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            IReplicationTransactionalPacketEntryData entryData = IOUtils.readSwapExternalizableObject(in);
            add(entryData);
        }
    }

    public void writeToSwap(ObjectOutput out) throws IOException {
        out.writeShort(buildFlags());
        IOUtils.writeNullableSwapExternalizableObject(out, _metaData);
        IOUtils.writeNullableSwapExternalizableObject(out, _transaction);
        out.writeInt(size());
        for (IReplicationTransactionalPacketEntryData entryData : this)
            IOUtils.writeSwapExternalizableObject(out, entryData);
    }

    @Override
    public IExecutableReplicationPacketData<IReplicationTransactionalPacketEntryData> clone() {
        IExecutableReplicationPacketData<IReplicationTransactionalPacketEntryData> clone = createEmptyMultipleEntryData();
        for (IReplicationTransactionalPacketEntryData entryData : this)
            clone.add(entryData.clone());

        return clone;
    }

    public boolean isSingleEntryData() {
        return false;
    }

    @Override
    public boolean isFromGateway() {
        return _fromGateway;
    }

    public IReplicationPacketEntryData getSingleEntryData() {
        throw new UnsupportedOperationException();
    }

    public void setMetaData(TransactionParticipantDataImpl metaData) {
        _metaData = metaData;
    }

    @Override
    public TransactionParticipantDataImpl getMetaData() {
        return _metaData;
    }

    @Override
    public ServerTransaction getTransaction() {
        return _transaction;
    }

    public boolean beforeDelayedReplication() {
        for (Iterator<IReplicationTransactionalPacketEntryData> iterator = iterator(); iterator.hasNext(); ) {
            IReplicationTransactionalPacketEntryData type = iterator.next();
            final boolean stillRelevant = type.beforeDelayedReplication();
            if (!stillRelevant)
                iterator.remove();
        }
        return !isEmpty();
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
    public String toString() {
        TransactionUniqueId transactionId = getMetaData() != null ? getMetaData().getTransactionUniqueId() : null;
        String transactionIdAddition = transactionId != null ? "ID=" + transactionId + ", " : "";
        return "TRANSACTION [" + transactionIdAddition + getCustomToString() + getMultipleOperationType() + "]: " + super.toString();
    }

    protected String getCustomToString() {
        return "";
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
    public Object getRecoveryFilteringId() {
        return _transaction;
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
