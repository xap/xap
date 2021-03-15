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

package com.gigaspaces.internal.client.spaceproxy.operations;

import com.gigaspaces.cluster.loadbalance.PartitionResultsMetadata;
import com.gigaspaces.internal.client.ReadTakeEntriesUidsResult;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.query.IPartitionResultMetadata;
import com.gigaspaces.internal.query.PartitionResultMetadata;
import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterExecutionType;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterRemoteOperationRouter;
import com.gigaspaces.internal.server.space.operations.SpaceOperationsCodes;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.utils.Textualizer;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.client.ReadModifiers;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.RemoteException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author idan
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class ReadTakeEntriesUidsSpaceOperationRequest extends SpaceOperationRequest<ReadTakeEntriesUidsSpaceOperationResult> {
    private static final long serialVersionUID = 1L;

    private static final short FLAG_TRANSACTION = 1 << 0;
    private static final short FLAG_ENTRIES_LIMIT = 1 << 1;
    private static final short FLAG_MODIFIERS = 1 << 2;

    private static final int DEFAULT_ENTRIES_LIMIT = Integer.MAX_VALUE;
    private static final int DEFAULT_MODIFIERS = ReadModifiers.REPEATABLE_READ;

    private ITemplatePacket _template;
    private Transaction _txn;
    private int _maxResults;
    private int _modifiers;

    private transient List<String[]> _results;
    private transient List<IPartitionResultMetadata> _metadata;
    private transient Exception _exception;
    private transient int _resultsCount;

    /**
     * Externalizable
     */
    public ReadTakeEntriesUidsSpaceOperationRequest() {
    }

    public ReadTakeEntriesUidsSpaceOperationRequest(ITemplatePacket template, Transaction transaction, int entriesLimit,
                                                    int modifiers) {
        this._template = template;
        this._txn = transaction;
        this._maxResults = entriesLimit;
        this._modifiers = modifiers;
        setFifoIfNeeded();

        if (entriesLimit != Integer.MAX_VALUE)
            throw new IllegalArgumentException("entriesLimit other than Integer.MAX_VALUE is not supported");
    }

    private void setFifoIfNeeded() {
        if (_template.getTypeDescriptor() != null && _template.getTypeDescriptor().isFifoDefault())
            _modifiers = Modifiers.add(_modifiers, Modifiers.FIFO);
    }

    @Override
    public int getOperationCode() {
        return SpaceOperationsCodes.READ_TAKE_ENTRIES_UIDS;
    }

    @Override
    public ReadTakeEntriesUidsSpaceOperationResult createRemoteOperationResult() {
        return new ReadTakeEntriesUidsSpaceOperationResult();
    }

    @Override
    public PartitionedClusterExecutionType getPartitionedClusterExecutionType() {
        if( _template.getRoutingFieldValue() != null || _template.isBroadcast())
            return PartitionedClusterExecutionType.SINGLE;
        return PartitionedClusterExecutionType.BROADCAST_CONCURRENT;
    }

    @Override
    public Object getPartitionedClusterRoutingValue(PartitionedClusterRemoteOperationRouter router) {
        return _template.getTemplateRoutingValue();
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("template", _template);
        textualizer.append("txn", _txn);
        textualizer.append("maxResults", _maxResults);
        textualizer.append("modifiers", _modifiers);
    }

    @Override
    public String getLRMIMethodTrackingId() {
        return "readMultipleUids";
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        short flags = buildFlags();
        out.writeShort(flags);
        IOUtils.writeObject(out, _template);
        if ((flags & FLAG_TRANSACTION) != 0)
            IOUtils.writeWithCachedStubs(out, _txn);
        if ((flags & FLAG_MODIFIERS) != 0)
            out.writeInt(_modifiers);
        if ((flags & FLAG_ENTRIES_LIMIT) != 0)
            out.writeInt(_maxResults);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        short flags = in.readShort();
        _template = IOUtils.readObject(in);
        if ((flags & FLAG_TRANSACTION) != 0)
            _txn = IOUtils.readWithCachedStubs(in);
        _modifiers = (flags & FLAG_MODIFIERS) != 0 ? in.readInt() : DEFAULT_MODIFIERS;
        _maxResults = (flags & FLAG_ENTRIES_LIMIT) != 0 ? in.readInt() : DEFAULT_ENTRIES_LIMIT;
    }

    private short buildFlags() {
        short flags = 0;
        if (_txn != null)
            flags |= FLAG_TRANSACTION;
        if (_maxResults != DEFAULT_ENTRIES_LIMIT)
            flags |= FLAG_ENTRIES_LIMIT;
        if (_modifiers != DEFAULT_MODIFIERS)
            flags |= FLAG_MODIFIERS;
        return flags;
    }

    public ITemplatePacket getTemplate() {
        return _template;
    }

    public void setTemplate(ITemplatePacket template) {
        _template = template;
    }

    @Override
    public Transaction getTransaction() {
        return _txn;
    }

    public int getEntriesLimit() {
        return _maxResults;
    }

    public int getModifiers() {
        return _modifiers;
    }

    @Override
    public boolean isDirectExecutionEnabled() {
        return Modifiers.contains(_modifiers, Modifiers.RUN_ON_IO_THREAD);
    }

    @Override
    public boolean processUnknownTypeException(List<Integer> positions) {
        if (_template.isSerializeTypeDesc())
            return false;
        _template.setSerializeTypeDesc(true);
        return true;
    }

    @Override
    public boolean processPartitionResult(ReadTakeEntriesUidsSpaceOperationResult remoteOperationResult,
                                          List<ReadTakeEntriesUidsSpaceOperationResult> previousResults, int numOfPartitions) {
        if (remoteOperationResult.hasException()) {
            _exception = remoteOperationResult.getExecutionException();
            return false;
        }
        if (_results == null) {
            _results = new LinkedList<String[]>();
            _metadata = new LinkedList<IPartitionResultMetadata>();
        }
        _results.add(remoteOperationResult.getUids());
        _metadata.add(new PartitionResultMetadata(remoteOperationResult.getPartitionId(),
                remoteOperationResult.getUids().length));
        _resultsCount += remoteOperationResult.getUids().length;
        return true;
    }

    @Override
    public boolean hasLockedResources() {
        return getRemoteOperationResult().getUids() != null && getRemoteOperationResult().getUids().length > 0;
    }

    @Override
    public void afterOperationExecution(int partitionId) {
        if (getRemoteOperationResult() != null)
            getRemoteOperationResult().setPartitionId(partitionId != -1 ? partitionId : null);
    }

    @Override
    public RemoteOperationRequest<ReadTakeEntriesUidsSpaceOperationResult> createCopy(int targetPartitionId) {
        final ReadTakeEntriesUidsSpaceOperationRequest copy = (ReadTakeEntriesUidsSpaceOperationRequest) super.createCopy(targetPartitionId);
        copy._template = _template.clone();
        return copy;
    }

    public ReadTakeEntriesUidsResult getFinalResult() throws RemoteException, TransactionException, UnusableEntryException {
        if (_exception != null) {
            setRemoteOperationExecutionError(_exception);
        } else if (_results != null) {
            final String[] finalResultUids = new String[_resultsCount];
            int index = 0;
            for (String[] uids : _results) {
                for (int i = 0; i < uids.length; i++)
                    finalResultUids[index++] = uids[i];
            }
            return new ReadTakeEntriesUidsResult(finalResultUids, _metadata);
        }
        final ReadTakeEntriesUidsSpaceOperationResult result = getRemoteOperationResult();
        result.processExecutionException();
        return new ReadTakeEntriesUidsResult(result.getUids(), new PartitionResultsMetadata(result.getPartitionId(),
                result.getUids().length));
    }

}
