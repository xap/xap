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

import com.gigaspaces.client.ReadMultipleException;
import com.gigaspaces.internal.client.SpaceIteratorBatchResult;
import com.gigaspaces.internal.exceptions.BatchQueryException;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.query.QueryUtils;
import com.gigaspaces.internal.query.explainplan.ExplainPlanImpl;
import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterExecutionType;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterRemoteOperationRouter;
import com.gigaspaces.internal.server.space.operations.SpaceOperationsCodes;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.client.ReadModifiers;
import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.TransactionException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.RemoteException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Alon Shoham
 * @since 15.2.0
 */
@com.gigaspaces.api.InternalApi
public class GetBatchForIteratorSpaceOperationRequest extends SpaceOperationRequest<GetBatchForIteratorSpaceOperationResult> {
    private static final long serialVersionUID = 1L;

    private static final Logger _devLogger = Logger.getLogger(Constants.LOGGER_DEV);

    private ITemplatePacket _templatePacket;
    private UUID _iteratorId;
    private boolean _firstTime; //use it with client disconnection + expiration
    private int _modifiers;
    private int _batchSize;
    private transient int _totalNumberOfMatchesEntries;
    private transient Object _query;
    private transient ExplainPlanImpl explainPlan;

    /**
     * Required for Externalizable.
     */
    public GetBatchForIteratorSpaceOperationRequest() {
    }

    public GetBatchForIteratorSpaceOperationRequest(
            ITemplatePacket templatePacket, int modifiers, int maxResults, Object query, UUID iteratorId, boolean firstTime) {
        this._templatePacket = templatePacket;
        this._modifiers = modifiers;
        this._batchSize = maxResults;
        this._query = query;
        this.explainPlan = ExplainPlanImpl.fromQueryPacket(query);
        this._iteratorId = iteratorId;
        this._firstTime = firstTime;
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("template", _templatePacket);
        textualizer.append("batchSize", _batchSize);
        textualizer.append("modifiers", _modifiers); //TODO fail unsupported read modifiers
        textualizer.append("iteratorId", _iteratorId);
        textualizer.append("firstTime", _firstTime);
    }

    @Override
    public int getOperationCode() {
        return SpaceOperationsCodes.GET_BATCH_FOR_ITERATOR;
    }

    @Override
    public GetBatchForIteratorSpaceOperationResult createRemoteOperationResult() {
        return new GetBatchForIteratorSpaceOperationResult();
    }

    @Override
    public PartitionedClusterExecutionType getPartitionedClusterExecutionType() {
        return PartitionedClusterExecutionType.SINGLE;
    }

    public SpaceIteratorBatchResult getFinalResult()
            throws RemoteException, TransactionException, UnusableEntryException {
        GetBatchForIteratorSpaceOperationResult result = getRemoteOperationResult();
        result.processExecutionException();
        _totalNumberOfMatchesEntries = result.getNumOfEntriesMatched();
        if (_totalNumberOfMatchesEntries > 0
                && _devLogger.isLoggable(Level.FINEST)) {
            _devLogger.finest(_totalNumberOfMatchesEntries
                    + " entries were scanned in the space in order to return the result for the "
                    + " get next batch from server iterator operation of query "
                    + QueryUtils.getQueryDescription(_query));
        }
        return new SpaceIteratorBatchResult(result.getEntryPackets(), result.getPartitionId(), result.getExecutionException(), _firstTime, _iteratorId);
    }

    @Override
    public Object getPartitionedClusterRoutingValue(PartitionedClusterRemoteOperationRouter router) {
        return _templatePacket.getRoutingFieldValue();
    }

    @Override
    public boolean processUnknownTypeException(List<Integer> positions) {
        if (_templatePacket.isSerializeTypeDesc())
            return false;
        _templatePacket.setSerializeTypeDesc(true);
        return true;
    }

    public ITemplatePacket getTemplatePacket() {
        return _templatePacket;
    }

    public int getBatchSize() {
        return _batchSize;
    }

    public int getModifiers() {
        return _modifiers;
    }

    public UUID getIteratorId() {
        return _iteratorId;
    }

    @Override
    public String getLRMIMethodTrackingId() {
        return "getBatchForSpaceIterator";
    }

    private static final short FLAG_MODIFIERS = 1;

    private static final int DEFAULT_MODIFIERS = ReadModifiers.REPEATABLE_READ;

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);
        final short flags = buildFlags();
        out.writeShort(flags);
        IOUtils.writeObject(out, _templatePacket);
        IOUtils.writeUUID(out, _iteratorId);
        out.writeBoolean(_firstTime);
        out.writeInt(_batchSize);
        if (flags != 0) {
            if (_modifiers != DEFAULT_MODIFIERS)
                out.writeInt(_modifiers);
        }
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);
        final short flags = in.readShort();
        _templatePacket = IOUtils.readObject(in);
        _iteratorId = IOUtils.readUUID(in);
        _firstTime = in.readBoolean();
        _batchSize = in.readInt();
        if (flags != 0) {
            _modifiers = (flags & FLAG_MODIFIERS) != 0 ? in.readInt() : DEFAULT_MODIFIERS;
        } else {
            _modifiers = DEFAULT_MODIFIERS;
        }
    }

    private short buildFlags() {
        short flags = 0;
        if (_modifiers != DEFAULT_MODIFIERS)
            flags |= FLAG_MODIFIERS;
        return flags;
    }

    @Override
    public boolean hasLockedResources() {
        return getRemoteOperationResult().getEntryPackets() != null
                && getRemoteOperationResult().getEntryPackets().length > 0;
    }

    @Override
    public boolean requiresPartitionedPreciseDistribution() {
        return false;
    }

    @Override
    public int getPreciseDistributionGroupingCode() {
        throw new UnsupportedOperationException();
    }


    public void afterOperationExecution(int partitionId) {
        GetBatchForIteratorSpaceOperationResult getBatchForIteratorSpaceOperationResult = getRemoteOperationResult();
        if (getBatchForIteratorSpaceOperationResult != null) {
            getBatchForIteratorSpaceOperationResult.setPartitionId(partitionId != -1 ? partitionId : null);
            processExplainPlan(getBatchForIteratorSpaceOperationResult);
        }
    }

    private void processExplainPlan(SpaceOperationResult result) {
        if (result.getExplainPlan() != null) {
            explainPlan.aggregate(result.getExplainPlan());
        }
    }

    public long getLease() {
        //TODO add lease field to flow
        return 1000;
    }

    public boolean isFirstTime() {
        return _firstTime;
    }
}
