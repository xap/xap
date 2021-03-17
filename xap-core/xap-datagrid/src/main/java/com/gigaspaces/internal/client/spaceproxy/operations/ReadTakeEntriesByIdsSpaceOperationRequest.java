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

import com.gigaspaces.client.ReadTakeByIdsException;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterExecutionType;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterRemoteOperationRouter;
import com.gigaspaces.internal.remoting.routing.partitioned.ScatterGatherOperationFutureListener;
import com.gigaspaces.internal.remoting.routing.partitioned.ScatterGatherPartitionInfo;
import com.gigaspaces.internal.remoting.routing.partitioned.ScatterGatherRemoteOperationRequest;
import com.gigaspaces.internal.server.space.operations.SpaceOperationsCodes;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.utils.Textualizer;
import com.j_spaces.core.AbstractIdsQueryPacket;
import com.j_spaces.core.IdsMultiRoutingQueryPacket;
import com.j_spaces.core.IdsQueryPacket;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.client.ReadModifiers;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.RemoteException;
import java.util.List;

/**
 * @author idan
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class ReadTakeEntriesByIdsSpaceOperationRequest extends SpaceScatterGatherOperationRequest<ReadTakeEntriesByIdsSpaceOperationResult> {
    private static final long serialVersionUID = 1L;

    private static final int DEFAULT_MODIFIERS = ReadModifiers.REPEATABLE_READ;

    private static final short FLAG_TRANSACTION = 1 << 0;
    private static final short FLAG_MODIFIERS = 1 << 1;
    private static final short FLAG_TAKE = 1 << 2;

    private AbstractIdsQueryPacket _template;
    private boolean _take;
    private int _modifiers;
    private Transaction _transaction;

    private transient IEntryPacket[] _results;
    private transient Throwable[] _exceptions;

    /**
     * Externalizable.
     */
    public ReadTakeEntriesByIdsSpaceOperationRequest() {
    }

    public ReadTakeEntriesByIdsSpaceOperationRequest(AbstractIdsQueryPacket queryPacket, boolean isTake, int modifiers,
                                                     Transaction transaction) {
        _template = queryPacket;
        _take = isTake;
        _modifiers = modifiers;
        _transaction = transaction;
    }

    @Override
    public boolean processPartitionResult(
            ScatterGatherRemoteOperationRequest<ReadTakeEntriesByIdsSpaceOperationResult> partitionRequest,
            List<ScatterGatherRemoteOperationRequest<ReadTakeEntriesByIdsSpaceOperationResult>> previousRequests) {
        if (_results == null)
            _results = new IEntryPacket[_template.getIds().length];
        final ReadTakeEntriesByIdsSpaceOperationResult result = partitionRequest.getRemoteOperationResult();
        final ScatterGatherPartitionInfo partitionInfo = partitionRequest.getPartitionInfo();
        if (result.hasException()) {
            processScatterResultException(result, partitionInfo);
        } else {
            final IEntryPacket[] entries = result.getEntryPackets();
            for (int i = 0; i < partitionInfo.size(); i++)
                _results[partitionInfo.getQuick(i)] = entries[i];
        }
        return true;
    }

    private void processScatterResultException(final ReadTakeEntriesByIdsSpaceOperationResult result,
                                               final ScatterGatherPartitionInfo partitionInfo) {
        if (_exceptions == null)
            _exceptions = new Throwable[_template.getIds().length];
        if (result.getExecutionException() instanceof ReadTakeByIdsException) {
            final ReadTakeByIdsException exception = (ReadTakeByIdsException) result.getExecutionException();
            for (int i = 0; i < partitionInfo.size(); i++) {
                int index = partitionInfo.getQuick(i);
                if (_results[index] == null) {
                    if (exception.getResults()[i].isError())
                        _exceptions[index] = exception.getResults()[i].getError();
                    else
                        _results[index] = (IEntryPacket) exception.getResults()[i].getObject();
                }
            }
        } else {
            for (int i = 0; i < partitionInfo.size(); i++)
                _exceptions[partitionInfo.getQuick(i)] = result.getExecutionException();
        }
    }

    @Override
    public void loadPartitionData(ScatterGatherRemoteOperationRequest<ReadTakeEntriesByIdsSpaceOperationResult> mainRequest) {
        final ReadTakeEntriesByIdsSpaceOperationRequest request = (ReadTakeEntriesByIdsSpaceOperationRequest) mainRequest;
        final IdsMultiRoutingQueryPacket mainTemplate = (IdsMultiRoutingQueryPacket) request.getTemplate();
        final Object[] ids = new Object[_partitionInfo.size()];
        final Object[] routings = new Object[_partitionInfo.size()];
        for (int i = 0; i < _partitionInfo.size(); i++) {
            ids[i] = mainTemplate.getIds()[_partitionInfo.getQuick(i)];
            routings[i] = mainTemplate.getRoutings()[_partitionInfo.getQuick(i)];
        }
        _template = new IdsMultiRoutingQueryPacket(ids, routings, 0, mainTemplate.getTypeDescriptor(),
                mainTemplate.getQueryResultType(), mainTemplate.getProjectionTemplate());
    }

    @Override
    public int getOperationCode() {
        return SpaceOperationsCodes.READ_TAKE_ENTRIES_BY_IDS;
    }

    @Override
    public ReadTakeEntriesByIdsSpaceOperationResult createRemoteOperationResult() {
        return new ReadTakeEntriesByIdsSpaceOperationResult();
    }

    @Override
    public PartitionedClusterExecutionType getPartitionedClusterExecutionType() {
        if(_template.isBroadcast())
            return PartitionedClusterExecutionType.SINGLE;
        if (_template instanceof IdsQueryPacket) {
            final IdsQueryPacket queryPacket = (IdsQueryPacket) _template;
            return queryPacket.getRoutingFieldValue() != null ? PartitionedClusterExecutionType.SINGLE : PartitionedClusterExecutionType.BROADCAST_CONCURRENT;
        }
        return PartitionedClusterExecutionType.SCATTER_CONCURRENT;
    }

    @Override
    public Object getPartitionedClusterRoutingValue(
            PartitionedClusterRemoteOperationRouter router) {
        return _template.getRouting(0);
    }

    public ReadTakeEntriesByIdsSpaceOperationResult getFinalResult() throws RemoteException, TransactionException, UnusableEntryException {
        ReadTakeEntriesByIdsSpaceOperationResult result;
        if (_results != null) {
            result = new ReadTakeEntriesByIdsSpaceOperationResult();
            if (_exceptions == null) {
                result.setEntryPackets(_results);
            } else {
                for (int i = 0; i < _results.length; i++)
                    if (_results[i] != null)
                        _exceptions[i] = null;
                result.setExecutionException(ReadTakeByIdsException.newException(_template.getIds(), _results, _exceptions, _take));
            }
        } else {
            result = getRemoteOperationResult();
            if (result.hasException() && !(result.getExecutionException() instanceof ReadTakeByIdsException)) {
                result.setExecutionException(ReadTakeByIdsException.newException(_template.getIds(), result.getExecutionException(), _take));
            }
        }
        result.processExecutionException();
        return result;
    }

    @Override
    public void scatterIndexesToPartitions(
            ScatterGatherOperationFutureListener<ReadTakeEntriesByIdsSpaceOperationResult> scatterGatherCoordinator) {
        final IdsMultiRoutingQueryPacket multiRoutingTemplate = (IdsMultiRoutingQueryPacket) _template;
        scatterGatherCoordinator.mapValuesByHashCode(multiRoutingTemplate.getRoutings(), this);
    }

    @Override
    public boolean processPartitionResult(
            ReadTakeEntriesByIdsSpaceOperationResult remoteOperationResult,
            List<ReadTakeEntriesByIdsSpaceOperationResult> previousResults,
            int numOfPartitions) {
        if (_results == null)
            _results = new IEntryPacket[_template.getIds().length];
        if (remoteOperationResult.hasException()) {
            processBroadcastResultException(remoteOperationResult);
        } else {
            final IEntryPacket[] entries = remoteOperationResult.getEntryPackets();
            for (int i = 0; i < entries.length; i++) {
                if (entries[i] != null)
                    _results[i] = entries[i];
            }
        }
        return true;
    }

    private void processBroadcastResultException(ReadTakeEntriesByIdsSpaceOperationResult remoteOperationResult) {
        boolean internalServerError = remoteOperationResult.getExecutionException() instanceof ReadTakeByIdsException;
        if (_exceptions == null) {
            _exceptions = new Throwable[_template.getIds().length];
            if (!internalServerError) {
                for (int i = 0; i < _exceptions.length; i++)
                    _exceptions[i] = remoteOperationResult.getExecutionException();
            }
        }
        if (internalServerError) {
            final ReadTakeByIdsException exception = (ReadTakeByIdsException) remoteOperationResult.getExecutionException();
            for (int i = 0; i < exception.getResults().length; i++) {
                if (_results[i] == null) {
                    if (exception.getResults()[i].isError())
                        _exceptions[i] = exception.getResults()[i].getError();
                    else
                        _results[i] = (IEntryPacket) exception.getResults()[i].getObject();
                }
            }
        }
    }

    @Override
    public String getLRMIMethodTrackingId() {
        return isTake() ? "takeByIds" : "readByIds";
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        short flags = buildFlags();
        out.writeShort(flags);
        out.writeObject(_template);
        if ((flags & FLAG_TRANSACTION) != 0)
            IOUtils.writeWithCachedStubs(out, _transaction);
        if ((flags & FLAG_MODIFIERS) != 0)
            out.writeInt(_modifiers);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        short flags = in.readShort();
        _template = (AbstractIdsQueryPacket) in.readObject();
        if ((flags & FLAG_TRANSACTION) != 0)
            _transaction = IOUtils.readWithCachedStubs(in);
        _modifiers = (flags & FLAG_MODIFIERS) != 0 ? in.readInt()
                : DEFAULT_MODIFIERS;
        _take = (flags & FLAG_TAKE) != 0;
    }

    @Override
    public boolean processUnknownTypeException(List<Integer> positions) {
        if (_template.isSerializeTypeDesc())
            return false;
        _template.setSerializeTypeDesc(true);
        return true;
    }

    @Override
    public SpaceScatterGatherOperationRequest<ReadTakeEntriesByIdsSpaceOperationResult> createCopy(int targetPartitionId) {
        ReadTakeEntriesByIdsSpaceOperationRequest copy = (ReadTakeEntriesByIdsSpaceOperationRequest) super.createCopy(targetPartitionId);
        // Clone template on broadcast
        if (_template instanceof IdsQueryPacket)
            copy._template = (AbstractIdsQueryPacket) this._template.clone();
        return copy;
    }

    private short buildFlags() {
        short flags = 0;
        if (_transaction != null)
            flags |= FLAG_TRANSACTION;
        if (_modifiers != DEFAULT_MODIFIERS)
            flags |= FLAG_MODIFIERS;
        if (_take)
            flags |= FLAG_TAKE;
        return flags;
    }

    @Override
    public Transaction getTransaction() {
        return _transaction;
    }

    public boolean isTake() {
        return _take;
    }

    public int getModifiers() {
        return _modifiers;
    }

    @Override
    public boolean isDirectExecutionEnabled() {
        return Modifiers.contains(_modifiers, Modifiers.RUN_ON_IO_THREAD);
    }

    public AbstractIdsQueryPacket getTemplate() {
        return _template;
    }

    @Override
    public boolean hasLockedResources() {
        final IEntryPacket[] entries = getRemoteOperationResult().getEntryPackets();
        if (entries != null && entries.length > 0) {
            for (IEntryPacket entry : entries) {
                if (entry != null)
                    return true;
            }
        }
        return false;
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("isTake", _take);
        textualizer.append("template", _template);
        textualizer.append("txn", _transaction);
        textualizer.append("modifiers", _modifiers);
    }
}
