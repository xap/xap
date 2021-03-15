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

import com.gigaspaces.client.WriteMultipleException;
import com.gigaspaces.cluster.replication.WriteConsistencyLevelCompromisedException;
import com.gigaspaces.internal.client.spaceproxy.metadata.ISpaceProxyTypeManager;
import com.gigaspaces.internal.client.spaceproxy.metadata.SpaceProxyTypeManager;
import com.gigaspaces.internal.exceptions.WriteResultImpl;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterExecutionType;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterRemoteOperationRouter;
import com.gigaspaces.internal.remoting.routing.partitioned.ScatterGatherOperationFutureListener;
import com.gigaspaces.internal.remoting.routing.partitioned.ScatterGatherPartitionInfo;
import com.gigaspaces.internal.remoting.routing.partitioned.ScatterGatherRemoteOperationRequest;
import com.gigaspaces.internal.server.space.operations.SpaceOperationsCodes;
import com.gigaspaces.internal.server.space.operations.WriteEntriesResult;
import com.gigaspaces.internal.server.space.operations.WriteEntryResult;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.utils.Textualizer;
import com.j_spaces.core.LeaseContext;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.client.UpdateModifiers;

import net.jini.core.lease.Lease;
import net.jini.core.transaction.Transaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class WriteEntriesSpaceOperationRequest extends SpaceScatterGatherOperationRequest<WriteEntriesSpaceOperationResult> {
    private static final long serialVersionUID = 1L;

    private IEntryPacket[] _entriesPackets;
    private Transaction _txn;
    private long _lease;
    private long[] _leases;
    private int _modifiers;
    private long _timeout;

    private transient ISpaceProxyTypeManager _typeManager;
    private transient Object[] _entries;
    private transient LeaseContext<?>[] _resultLeases;
    private transient WriteMultipleException.IWriteResult[] _partialFailureResults;
    private transient boolean _broadcast;

    /**
     * Required for Externalizable
     */
    public WriteEntriesSpaceOperationRequest() {
    }

    public WriteEntriesSpaceOperationRequest(ISpaceProxyTypeManager typeManager, Object[] entries, IEntryPacket[] entriesPackets,
                                             Transaction txn, long lease, long[] leases, long timeout, int modifiers, boolean broadcast) {
        this._typeManager = typeManager;
        this._entries = entries;
        this._entriesPackets = entriesPackets;
        this._txn = txn;
        this._lease = lease;
        this._leases = leases;
        this._modifiers = modifiers;
        this._timeout = timeout;
        this._broadcast = broadcast;
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);

        if (_entriesPackets.length < 10) {
            textualizer.append("entriesPackets", _entriesPackets);
        } else {
            IEntryPacket[] dest = new IEntryPacket[10];
            System.arraycopy(_entriesPackets, 0, dest, 0, dest.length);
            textualizer.append("entriesPackets", dest);
            textualizer.appendString("... truncated [length=" + _entriesPackets.length + "]");
        }
        textualizer.append("txn", _txn);
        textualizer.append("lease", _lease);
        textualizer.append("leases", _leases);
        textualizer.append("modifiers", _modifiers);
        textualizer.append("timeout", _timeout);
    }

    @Override
    public int getOperationCode() {
        return SpaceOperationsCodes.WRITE_ENTRIES;
    }

    @Override
    public WriteEntriesSpaceOperationResult createRemoteOperationResult() {
        return new WriteEntriesSpaceOperationResult();
    }

    @Override
    public PartitionedClusterExecutionType getPartitionedClusterExecutionType() {
        return _broadcast ? PartitionedClusterExecutionType.SINGLE : PartitionedClusterExecutionType.SCATTER_CONCURRENT;
    }

    @Override
    public Object getPartitionedClusterRoutingValue(PartitionedClusterRemoteOperationRouter router) {
        if(_broadcast)
            return 0;
        throw new IllegalStateException();
    }

    @Override
    public void scatterIndexesToPartitions(ScatterGatherOperationFutureListener<WriteEntriesSpaceOperationResult> listener) {
        for (int i = 0; i < _entriesPackets.length; i++) {
            Object routingValue = _entriesPackets[i].getRoutingFieldValue();
            int partitionId;
            if (routingValue != null)
                partitionId = listener.getPartitionIdByHashcode(routingValue);
            else if (_entriesPackets[i].getTypeDescriptor().isAutoGenerateRouting())
                partitionId = listener.getNextDistributionPartitionId();
            else
                throw new IllegalArgumentException("Routing value of entry #" + i + " is null");
            listener.mapIndexToPartition(i, partitionId, this);
        }
    }

    @Override
    public void loadPartitionData(ScatterGatherRemoteOperationRequest<WriteEntriesSpaceOperationResult> mainRequest) {
        WriteEntriesSpaceOperationRequest main = (WriteEntriesSpaceOperationRequest) mainRequest;
        this._entriesPackets = scatter(main._entriesPackets, new IEntryPacket[_partitionInfo.size()]);
        this._leases = scatter(main._leases);
    }

    @Override
    public boolean processPartitionResult(ScatterGatherRemoteOperationRequest<WriteEntriesSpaceOperationResult> partitionRequest,
                                          List<ScatterGatherRemoteOperationRequest<WriteEntriesSpaceOperationResult>> previousRequests) {
        if (partitionRequest.getRemoteOperationResult().getExecutionException() != null ||
                partitionRequest.getRemoteOperationResult().getResult().getErrors() != null) {
            if (_partialFailureResults == null) {
                // Init partial failures result:
                _partialFailureResults = new WriteMultipleException.IWriteResult[_entriesPackets.length];
                // Convert previous results to partial failure results:
                if (_resultLeases != null)
                    for (ScatterGatherRemoteOperationRequest<WriteEntriesSpaceOperationResult> prevRequest : previousRequests)
                        processPartitionResult(prevRequest);
            }
        }

        // Process current partition result:
        processPartitionResult(partitionRequest);

        return true;
    }

    private void processPartitionResult(ScatterGatherRemoteOperationRequest<WriteEntriesSpaceOperationResult> partitionRequest) {

        ScatterGatherPartitionInfo partitionInfo = partitionRequest.getPartitionInfo();
        Exception partitionException = partitionRequest.getRemoteOperationResult().getExecutionException();
        boolean consistencyCompromised = isSyncReplicationConsistencyCompromised(partitionRequest);
        if (consistencyCompromised && _partialFailureResults == null) {
            _partialFailureResults = new WriteMultipleException.IWriteResult[_entriesPackets.length];
        }
        if (partitionException != null) {
            int size = partitionInfo != null ? partitionInfo.size() : _entriesPackets.length;
            for (int i = 0; i < size; i++) {
                int masterPosition = partitionInfo != null ? partitionInfo.getQuick(i) : i;
                _partialFailureResults[masterPosition] = WriteResultImpl.createErrorResult(partitionException);
            }
        } else if (_partialFailureResults != null) {
            WriteEntriesResult partitionResult = partitionRequest.getRemoteOperationResult().getResult();
            for (int i = 0; i < partitionResult.getResults().length; i++) {
                int masterPosition = partitionInfo != null ? partitionInfo.getQuick(i) : i;
                if (partitionResult.isError(i))
                    _partialFailureResults[masterPosition] = WriteResultImpl.createErrorResult(partitionResult.getErrors()[i]);
                else {
                    try {
                        LeaseContext<?> lease = createLease(partitionResult.getResults()[i], masterPosition);
                        _partialFailureResults[masterPosition] = WriteResultImpl.createLeaseResult(lease);
                    } catch (WriteConsistencyLevelCompromisedException e) {
                        _partialFailureResults[masterPosition] = WriteResultImpl.createErrorResult(e);
                    }
                }
            }
        } else {
            if (_resultLeases == null)
                _resultLeases = new LeaseContext<?>[_entriesPackets.length];
            WriteEntriesResult partitionResult = partitionRequest.getRemoteOperationResult().getResult();
            for (int i = 0; i < partitionResult.getResults().length; i++) {
                int masterPosition = partitionInfo != null ? partitionInfo.getQuick(i) : i;
                _resultLeases[masterPosition] = createLease(partitionResult.getResults()[i], masterPosition);
            }
        }
    }

    private boolean isSyncReplicationConsistencyCompromised(ScatterGatherRemoteOperationRequest<WriteEntriesSpaceOperationResult> partitionRequest) {
        WriteEntriesResult partitionResult = partitionRequest.getRemoteOperationResult().getResult();
        if (partitionResult == null || partitionResult.getResults() == null) {
            return false;
        }
        for (int i = 0; i < partitionResult.getResults().length; i++) {
            if (!partitionResult.isError(i)) {
                if (syncReplicationConsistencyCompromised(partitionResult.getResults()[i])) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean syncReplicationConsistencyCompromised(WriteEntryResult writeEntryResult) {
        return writeEntryResult.getSyncReplicationLevel() + 1 < SpaceProxyTypeManager.requiredConsistencyLevel();
    }

    private LeaseContext<?> createLease(WriteEntryResult writeResult, int masterPosition) {
        return _typeManager.processWriteResult(
                writeResult,
                _entries[masterPosition],
                _entriesPackets[masterPosition]);
    }

    public LeaseContext<?>[] getFinalResult() {
        if (getRemoteOperationResult() != null)
            processPartitionResult(this, null);

        if (_partialFailureResults != null)
            throw new WriteMultipleException(_partialFailureResults);

        return _resultLeases;
    }

    public IEntryPacket[] getEntriesPackets() {
        return _entriesPackets;
    }

    @Override
    public Transaction getTransaction() {
        return _txn;
    }

    public long getLease() {
        return _lease;
    }

    public long[] getLeases() {
        return _leases;
    }

    public int getModifiers() {
        return _modifiers;
    }

    @Override
    public boolean isDirectExecutionEnabled() {
        return Modifiers.contains(_modifiers, Modifiers.RUN_ON_IO_THREAD);
    }

    public long getTimeOut() {
        return _timeout;
    }

    @Override
    public boolean processUnknownTypeException(List<Integer> positions) {
        if (positions == null)
            return false;
        boolean retry = false;
        for (Integer position : positions) {
            if (!_entriesPackets[position].isSerializeTypeDesc()) {
                _entriesPackets[position].setSerializeTypeDesc(true);
                retry = true;
            }
        }
        return retry;
    }

    @Override
    public String getLRMIMethodTrackingId() {
        return "writeMultiple";
    }

    private static final short FLAG_TRANSACTION = 1 << 0;
    private static final short FLAG_LEASE = 1 << 1;
    private static final short FLAG_LEASES = 1 << 2;
    private static final short FLAG_MODIFIERS = 1 << 3;
    private static final short FLAG_TIMEOUT = 1 << 4;

    private static final long DEFAULT_LEASE = Lease.FOREVER;
    private static final int DEFAULT_MODIFIERS = UpdateModifiers.UPDATE_OR_WRITE;
    private static long DEFAULT_TIMEOUT = 0L;

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        final short flags = buildFlags();
        out.writeShort(flags);
        IOUtils.writeObjectArray(out, _entriesPackets);
        if (flags != 0) {
            if (_txn != null)
                IOUtils.writeWithCachedStubs(out, _txn);
            if (_lease != DEFAULT_LEASE)
                out.writeLong(_lease);
            if (_leases != null)
                IOUtils.writeLongArray(out, _leases);
            if (_modifiers != DEFAULT_MODIFIERS)
                out.writeInt(_modifiers);
            if (_timeout != DEFAULT_TIMEOUT)
                out.writeLong(_timeout);

        }
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);

        final short flags = in.readShort();
        this._entriesPackets = IOUtils.readEntryPacketArray(in);
        if (flags != 0) {
            if ((flags & FLAG_TRANSACTION) != 0)
                this._txn = IOUtils.readWithCachedStubs(in);
            this._lease = (flags & FLAG_LEASE) != 0 ? in.readLong() : DEFAULT_LEASE;
            if ((flags & FLAG_LEASES) != 0)
                this._leases = IOUtils.readLongArray(in);
            this._modifiers = (flags & FLAG_MODIFIERS) != 0 ? in.readInt() : DEFAULT_MODIFIERS;
            this._timeout = (flags & FLAG_TIMEOUT) != 0 ? in.readLong() : DEFAULT_TIMEOUT;
        } else {
            this._lease = DEFAULT_LEASE;
            this._modifiers = DEFAULT_MODIFIERS;
            this._timeout = DEFAULT_TIMEOUT;
        }
    }

    private short buildFlags() {
        short flags = 0;

        if (_txn != null)
            flags |= FLAG_TRANSACTION;
        if (_lease != DEFAULT_LEASE)
            flags |= FLAG_LEASE;
        if (_leases != null)
            flags |= FLAG_LEASES;
        if (_modifiers != DEFAULT_MODIFIERS)
            flags |= FLAG_MODIFIERS;
        if (_timeout != DEFAULT_TIMEOUT)
            flags |= FLAG_TIMEOUT;
        return flags;
    }

    @Override
    public boolean requiresPartitionedPreciseDistribution() {
        return true;
    }

    @Override
    public int getPreciseDistributionGroupingCode() {
        return SpacePreciseDistributionGroupingCodes.WRITE;
    }
}
