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

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.query.RawEntryConverter;
import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterExecutionType;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterRemoteOperationRouter;
import com.gigaspaces.internal.server.space.operations.SpaceOperationsCodes;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.query.aggregators.AggregationInternalUtils;
import com.gigaspaces.query.aggregators.AggregationResult;
import com.gigaspaces.query.aggregators.SpaceEntriesAggregator;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.exception.internal.InterruptedSpaceException;

import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 10.0
 */
@com.gigaspaces.api.InternalApi
public class AggregateEntriesSpaceOperationRequest extends SpaceOperationRequest<AggregateEntriesSpaceOperationResult> {
    private static final long serialVersionUID = 1L;

    private ITemplatePacket queryPacket;
    private List<SpaceEntriesAggregator> aggregators;
    private Transaction txn;
    private int readModifiers;

    private transient Exception _exception;

    /**
     * Required for Externalizable
     */
    public AggregateEntriesSpaceOperationRequest() {
    }

    public AggregateEntriesSpaceOperationRequest(ITemplatePacket queryPacket, Transaction txn, int modifiers, List<SpaceEntriesAggregator> aggregators) {
        this.queryPacket = queryPacket;
        this.aggregators = aggregators;
        this.readModifiers = modifiers;
        // Scanner currently does not use transactions.
        //this.txn = txn;
    }

    @Override
    public int getOperationCode() {
        return SpaceOperationsCodes.AGGREGATE_ENTRIES;
    }


    @Override
    public AggregateEntriesSpaceOperationResult createRemoteOperationResult() {
        return new AggregateEntriesSpaceOperationResult();
    }

    @Override
    public RemoteOperationRequest<AggregateEntriesSpaceOperationResult> createCopy(int targetPartitionId) {
        AggregateEntriesSpaceOperationRequest copy = (AggregateEntriesSpaceOperationRequest) super.createCopy(targetPartitionId);
        copy.queryPacket = this.queryPacket.clone();
        copy.aggregators = new ArrayList<SpaceEntriesAggregator>(this.aggregators.size());
        for (SpaceEntriesAggregator aggregator : this.aggregators)
            copy.aggregators.add(aggregator.clone());
        return copy;
    }

    @Override
    public PartitionedClusterExecutionType getPartitionedClusterExecutionType() {
        if (queryPacket.getRoutingFieldValue() != null || isReplicatedTableOperation())
            return PartitionedClusterExecutionType.SINGLE;

        return PartitionedClusterExecutionType.BROADCAST_CONCURRENT;
    }

    @Override
    public Object getPartitionedClusterRoutingValue(PartitionedClusterRemoteOperationRouter router) {
        return queryPacket.getTemplateRoutingValue();
    }

    @Override
    public boolean processPartitionResult(AggregateEntriesSpaceOperationResult remoteOperationResult,
                                          List<AggregateEntriesSpaceOperationResult> previousResults,
                                          int numOfPartitions) {
        if (remoteOperationResult.hasException())
            _exception = remoteOperationResult.getExecutionException();
        else
            aggregate(remoteOperationResult);
        return _exception == null;
    }

    public boolean isReplicatedTableOperation() {
        return queryPacket.getTypeDescriptor() != null && !queryPacket.getTypeDescriptor().isPartitioned();
    }

    private void aggregate(AggregateEntriesSpaceOperationResult result) {
        Object[] newResults = result.getIntermediateResults();
        for (int i = 0; i < aggregators.size(); i++) {
            Serializable partitionResult = (Serializable) newResults[i];
            if (partitionResult != null)
                aggregators.get(i).aggregateIntermediateResult(partitionResult);
        }
    }

    public AggregationResult getFinalResult(IJSpace spaceProxy, ITemplatePacket queryPacket, boolean returnEntryPacket)
            throws TransactionException, InterruptedException, RemoteException {
        AggregateEntriesSpaceOperationResult remoteOperationResult = getRemoteOperationResult();
        if (remoteOperationResult != null) {
            if (remoteOperationResult.hasException())
                _exception = remoteOperationResult.getExecutionException();
            else if (!remoteOperationResult.isEmbedded())
                aggregate(remoteOperationResult);
        }

        if (_exception != null)
            processExecutionException(_exception);

        return AggregationInternalUtils.getFinalResult(aggregators, new RawEntryConverter(spaceProxy.getDirectProxy(),
                queryPacket, returnEntryPacket));
    }

    private void processExecutionException(Exception executionException)
            throws RemoteException, TransactionException, InterruptedException {
        if (executionException instanceof TransactionException)
            throw (TransactionException) executionException;
        if (executionException instanceof RemoteException)
            throw (RemoteException) executionException;
        if (executionException instanceof InterruptedSpaceException && executionException.getCause() instanceof InterruptedException)
            throw (InterruptedException) executionException.getCause();
        if (executionException instanceof InterruptedException)
            throw (InterruptedException) executionException;

        SpaceOperationResult.onUnexpectedException(executionException);
    }

    @Override
    public String getLRMIMethodTrackingId() {
        return "ScanEntries";
    }

    public ITemplatePacket getQueryPacket() {
        return queryPacket;
    }

    public void setQueryPacket(ITemplatePacket queryPacket) {
        this.queryPacket = queryPacket;
    }

    public List<SpaceEntriesAggregator> getAggregators() {
        return aggregators;
    }

    public int getReadModifiers() {
        return readModifiers;
    }

    private static final short FLAG_TRANSACTION = 1 << 0;
    private static final short FLAG_MODIFIERS = 1 << 1;

    private static final int DEFAULT_MODIFIERS = 0;

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        final short flags = buildFlags();
        out.writeShort(flags);
        IOUtils.writeObject(out, queryPacket);
        PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        if (version.greaterOrEquals(PlatformLogicalVersion.v12_1_0)) {
            IOUtils.serializeSupportCodeChangeCollection(out, aggregators);
        }
        else {
            IOUtils.writeObject(out, aggregators);
        }

        if (flags != 0) {
            if (txn != null)
                IOUtils.writeWithCachedStubs(out, txn);
            if (readModifiers != DEFAULT_MODIFIERS)
                out.writeInt(readModifiers);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        short flags = in.readShort();
        this.queryPacket = IOUtils.readObject(in);
        PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        if (version.greaterOrEquals(PlatformLogicalVersion.v12_1_0)) {
            this.aggregators = new ArrayList<SpaceEntriesAggregator>();
            IOUtils.deserializeSupportCodeChangeCollection(in, aggregators);
        }
        else {
            this.aggregators = IOUtils.readObject(in);
        }

        if (flags != 0) {
            if ((flags & FLAG_TRANSACTION) != 0)
                this.txn = IOUtils.readWithCachedStubs(in);
            this.readModifiers = (flags & FLAG_MODIFIERS) != 0 ? in.readInt() : DEFAULT_MODIFIERS;
        } else {
            this.readModifiers = DEFAULT_MODIFIERS;
        }
    }

    private short buildFlags() {
        short flags = 0;

        if (txn != null)
            flags |= FLAG_TRANSACTION;
        if (readModifiers != DEFAULT_MODIFIERS)
            flags |= FLAG_MODIFIERS;

        return flags;
    }
}
