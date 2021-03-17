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
import com.gigaspaces.internal.query.explainplan.ExplainPlanImpl;
import com.gigaspaces.query.explainplan.ExplainPlan;
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
import java.util.List;

/**
 * @author anna
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class CountClearEntriesSpaceOperationRequest extends SpaceOperationRequest<CountClearEntriesSpaceOperationResult> {
    private static final long serialVersionUID = 1L;

    private ITemplatePacket _templatePacket;
    private Transaction _txn;
    private boolean _isClear;
    private int _modifiers;

    private transient CountClearEntriesSpaceOperationResult _finalResult;
    private transient ExplainPlanImpl explainPlan;


    /**
     * Required for Externalizable
     */
    public CountClearEntriesSpaceOperationRequest() {
    }

    public CountClearEntriesSpaceOperationRequest(ITemplatePacket templatePacket, Transaction txn, boolean isClear, int modifiers) {
        _templatePacket = templatePacket;
        _txn = txn;
        _isClear = isClear;
        _modifiers = modifiers;
        this.explainPlan = ExplainPlanImpl.fromQueryPacket(templatePacket);
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("isClear", _isClear);
        textualizer.append("template", _templatePacket);
        textualizer.append("txn", _txn);
        textualizer.append("modifiers", _modifiers);
        textualizer.append("typeName", _templatePacket.getTypeName());
    }

    @Override
    public int getOperationCode() {
        return SpaceOperationsCodes.COUNT_CLEAR_ENTRIES;
    }

    @Override
    public CountClearEntriesSpaceOperationResult createRemoteOperationResult() {
        return new CountClearEntriesSpaceOperationResult();
    }

    @Override
    public PartitionedClusterExecutionType getPartitionedClusterExecutionType() {
        if (_templatePacket.getRoutingFieldValue() != null || _templatePacket.isBroadcast())
            return PartitionedClusterExecutionType.SINGLE;

        return PartitionedClusterExecutionType.BROADCAST_CONCURRENT;
    }

    @Override
    public Object getPartitionedClusterRoutingValue(PartitionedClusterRemoteOperationRouter router) {
        return _templatePacket.getTemplateRoutingValue();
    }

    public ITemplatePacket getTemplatePacket() {
        return _templatePacket;
    }

    public int getModifiers() {
        return _modifiers;
    }

    @Override
    public boolean isDirectExecutionEnabled() {
        return Modifiers.contains(_modifiers, Modifiers.RUN_ON_IO_THREAD);
    }

    @Override
    public Transaction getTransaction() {
        return _txn;
    }

    @Override
    public boolean isBlockingOperation() {
        return false;
    }

    public boolean isClear() {
        return _isClear;
    }

    @Override
    public String getLRMIMethodTrackingId() {
        return isClear() ? "clear" : "count";
    }

    @Override
    public boolean processUnknownTypeException(List<Integer> positions) {
        if (_templatePacket.isSerializeTypeDesc())
            return false;
        _templatePacket.setSerializeTypeDesc(true);
        return true;
    }

    @Override
    public boolean processPartitionResult(
            CountClearEntriesSpaceOperationResult remoteOperationResult,
            List<CountClearEntriesSpaceOperationResult> previousResults,
            int numOfPartitions) {
        if (remoteOperationResult.getExecutionException() != null) {
            // TODO: accumulate exceptions and return more detailed ClearException with count
            _finalResult = remoteOperationResult;

            //in case of clear continue to other partitions even if one fails
            return _isClear;
        }
        processExplainPlan(remoteOperationResult);
        if (_finalResult == null)
            _finalResult = remoteOperationResult;
        else
            _finalResult.add(remoteOperationResult.getCount());
        return true;
    }

    public int getFinalResult() throws RemoteException, TransactionException, UnusableEntryException {
        CountClearEntriesSpaceOperationResult result = _finalResult == null ? getRemoteOperationResult() : _finalResult;
        result.processExecutionException();
        return result.getCount();
    }

    private static final short FLAG_TRANSACTION = 1 << 0;
    private static final short FLAG_MODIFIERS = 1 << 1;
    private static final short FLAG_IS_CLEAR = 1 << 2;

    private static final int DEFAULT_MODIFIERS = ReadModifiers.REPEATABLE_READ;

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        final short flags = buildFlags();
        out.writeShort(flags);
        IOUtils.writeObject(out, _templatePacket);
        if (flags != 0) {
            if (_txn != null)
                IOUtils.writeWithCachedStubs(out, _txn);
            if (_modifiers != DEFAULT_MODIFIERS)
                out.writeInt(_modifiers);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);

        final short flags = in.readShort();
        this._templatePacket = IOUtils.readObject(in);
        if (flags != 0) {
            if ((flags & FLAG_TRANSACTION) != 0)
                this._txn = IOUtils.readWithCachedStubs(in);
            this._modifiers = (flags & FLAG_MODIFIERS) != 0 ? in.readInt() : DEFAULT_MODIFIERS;
            this._isClear = (flags & FLAG_IS_CLEAR) != 0;
        } else {
            this._modifiers = DEFAULT_MODIFIERS;
        }
    }

    @Override
    public RemoteOperationRequest<CountClearEntriesSpaceOperationResult> createCopy(int targetPartitionId) {
        CountClearEntriesSpaceOperationRequest copy = (CountClearEntriesSpaceOperationRequest) super.createCopy(targetPartitionId);
        copy._templatePacket = _templatePacket.clone();
        return copy;
    }

    private short buildFlags() {
        short flags = 0;

        if (_txn != null)
            flags |= FLAG_TRANSACTION;
        if (_modifiers != DEFAULT_MODIFIERS)
            flags |= FLAG_MODIFIERS;
        if (_isClear)
            flags |= FLAG_IS_CLEAR;

        return flags;
    }

    @Override
    public boolean hasLockedResources() {
        return _isClear && getRemoteOperationResult().getCount() > 0;
    }


    public void afterOperationExecution(int partitionId){
        processExplainPlan(getRemoteOperationResult());
    }

    private void processExplainPlan(SpaceOperationResult result) {
        if(result != null && result.getExplainPlan() != null){
            explainPlan.aggregate(result.getExplainPlan());
        }
    }

    public ExplainPlan getExplainPlan() {
        return explainPlan;
    }
}
