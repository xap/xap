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
import com.gigaspaces.client.TakeMultipleException;
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
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.client.ReadModifiers;
import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.RemoteException;
import java.util.*;

/**
 * @author idan
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class ReadTakeEntriesSpaceOperationRequest extends SpaceOperationRequest<ReadTakeEntriesSpaceOperationResult> {
    private static final long serialVersionUID = 1L;

    private static final Logger _devLogger = LoggerFactory.getLogger(Constants.LOGGER_DEV);

    private ITemplatePacket _templatePacket;
    private Transaction _txn;
    private boolean _isTake;
    private int _modifiers;
    private int _maxResults;
    private int _minResultsToWaitFor;
    private boolean _ifExist;
    private long _timeout;

    private transient int _resultEntriesCount = 0;
    private transient int _finalResultMaxEntries;
    private transient List<IEntryPacket[]> _entries;
    private transient List<Throwable> _exceptions;
    private transient int _totalNumberOfMatchesEntries;
    private transient Object _query;
    private transient Map<IEntryPacket[], Integer> replicationLevels;
    private transient List<ReplicationLevel> levels = null;
    private transient ExplainPlanImpl explainPlan;

    /**
     * Required for Externalizable.
     */
    public ReadTakeEntriesSpaceOperationRequest() {
    }

    public ReadTakeEntriesSpaceOperationRequest(
            ITemplatePacket templatePacket, Transaction transaction,
            boolean isTake, int modifiers, int maxResults, int minResultsToWaitFor, long timeout, boolean ifExist, Object query) {
        this._templatePacket = templatePacket;
        this._txn = transaction;
        this._isTake = isTake;
        this._modifiers = modifiers;
        this._maxResults = maxResults;
        this._minResultsToWaitFor = minResultsToWaitFor;
        this._timeout = timeout;
        this._ifExist = ifExist;
        this._finalResultMaxEntries = maxResults;
        this._query = query;
        this.explainPlan = ExplainPlanImpl.fromQueryPacket(query);
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("isTake", _isTake);
        textualizer.append("template", _templatePacket);
        textualizer.append("txn", _txn);
        textualizer.append("maxResults", _maxResults);
        textualizer.append("modifiers", _modifiers);
        textualizer.append("timeout", _timeout);
        textualizer.append("minResultsToWaitFor", _minResultsToWaitFor);
        textualizer.append("ifExist", _ifExist);
    }

    @Override
    public int getOperationCode() {
        return SpaceOperationsCodes.READ_TAKE_ENTRIES;
    }

    @Override
    public ReadTakeEntriesSpaceOperationResult createRemoteOperationResult() {
        return new ReadTakeEntriesSpaceOperationResult();
    }

    @Override
    public PartitionedClusterExecutionType getPartitionedClusterExecutionType() {
        if (_templatePacket.getRoutingFieldValue() != null || _templatePacket.isBroadcast())
            return PartitionedClusterExecutionType.SINGLE;

        if (_timeout != 0)
            throw new IllegalArgumentException("Broadcast is not supported for Read/Take multiple Operations with timeout greater than 0. (Type='"
                    + _templatePacket.getTypeName()
                    + "' ,routing property='"
                    + _templatePacket.getTypeDescriptor()
                    .getRoutingPropertyName() + "')");

        if (_maxResults != Integer.MAX_VALUE && (_isTake || _txn != null))
            return PartitionedClusterExecutionType.BROADCAST_SEQUENTIAL;

        return PartitionedClusterExecutionType.BROADCAST_CONCURRENT;
    }

    @Override
    public boolean processPartitionResult(
            ReadTakeEntriesSpaceOperationResult remoteOperationResult,
            List<ReadTakeEntriesSpaceOperationResult> previousResults,
            int numberOfPartitions) {
        _totalNumberOfMatchesEntries += remoteOperationResult.getNumOfEntriesMatched();
        if (remoteOperationResult.hasException()) {
            if (_exceptions == null)
                _exceptions = new LinkedList<Throwable>();
            _exceptions.add(remoteOperationResult.getExecutionException());
        } else {
            if (remoteOperationResult.getEntryPackets() != null) {
                if (_entries == null) {
                    _entries = new LinkedList<IEntryPacket[]>();
                    replicationLevels = new HashMap<IEntryPacket[], Integer>();
                }
                _entries.add(remoteOperationResult.getEntryPackets());
                replicationLevels.put(remoteOperationResult.getEntryPackets(), remoteOperationResult.getSyncReplicationLevel());
                _resultEntriesCount += remoteOperationResult.getEntryPackets().length;
                if (previousResults.size() + 1 == numberOfPartitions) {
                    levels = new ArrayList<ReplicationLevel>(_entries.size());
                    int index = 0;
                    for (IEntryPacket[] entry : _entries) {
                        levels.add(new ReplicationLevel(index, entry.length, replicationLevels.get(entry)));
                        index += entry.length;
                    }
                    replicationLevels.clear();
                }
            }

            if (_maxResults != DEFAULT_MAX_ENTRIES) {
                _maxResults -= remoteOperationResult.getEntryPackets().length;
                _minResultsToWaitFor = Math.min(_minResultsToWaitFor, _maxResults);
            }
            processExplainPlan(remoteOperationResult);
        }

        return _maxResults > 0;
    }

    @SuppressWarnings("deprecation")
    public IEntryPacket[] getFinalResult()
            throws RemoteException, TransactionException, UnusableEntryException {
        ReadTakeEntriesSpaceOperationResult result;


        if (_entries == null && _exceptions == null) {
            result = getRemoteOperationResult();
            _totalNumberOfMatchesEntries = getRemoteOperationResult().getNumOfEntriesMatched();
        } else {
            final int resultsCount = Math.min(_resultEntriesCount, _finalResultMaxEntries);
            final IEntryPacket[] entries = new IEntryPacket[resultsCount];
            if (_entries != null) {
                int index = 0;
                for (IEntryPacket[] entry : _entries)
                    index = accumulateResult(entry, entries, index);
            }
            result = new ReadTakeEntriesSpaceOperationResult(entries);
            if (_exceptions != null && entries.length < _finalResultMaxEntries && ReadModifiers.isThrowPartialFailure(_modifiers))
                result.setExecutionException(createPartialExecutionException(entries));
        }

        result.processExecutionException();

        if (_totalNumberOfMatchesEntries > 0
                && _devLogger.isTraceEnabled()) {
            _devLogger.trace(_totalNumberOfMatchesEntries
                    + " entries were scanned in the space in order to return the result for the "
                    + (_isTake ? "take" : "read") + " multiple operation of query "
                    + QueryUtils.getQueryDescription(_query));
        }

        IEntryPacket[] entryPackets = result.getEntryPackets();
        return entryPackets;
    }

    private BatchQueryException createPartialExecutionException(IEntryPacket[] entries) {
        return _isTake ? new TakeMultipleException(entries, _exceptions)
                : new ReadMultipleException(entries, _exceptions);
    }

    private int accumulateResult(IEntryPacket[] entryPackets,
                                 final IEntryPacket[] entries, int index) {
        for (int i = 0; i < entryPackets.length && index < entries.length; i++)
            entries[index++] = entryPackets[i];
        return index;
    }


    @Override
    public Object getPartitionedClusterRoutingValue(PartitionedClusterRemoteOperationRouter router) {
        return _templatePacket.getTemplateRoutingValue();
    }

    @Override
    public boolean processUnknownTypeException(List<Integer> positions) {
        if (_templatePacket.isSerializeTypeDesc())
            return false;
        _templatePacket.setSerializeTypeDesc(true);
        return true;
    }

    @Override
    public Transaction getTransaction() {
        return _txn;
    }

    public ITemplatePacket getTemplatePacket() {
        return _templatePacket;
    }

    public boolean isTake() {
        return _isTake;
    }

    public int getMaxResults() {
        return _maxResults;
    }

    public int getModifiers() {
        return _modifiers;
    }

    @Override
    public boolean isDirectExecutionEnabled() {
        return Modifiers.contains(_modifiers, Modifiers.RUN_ON_IO_THREAD);
    }

    public int getMinResultsToWaitFor() {
        return _minResultsToWaitFor;
    }

    public long getTimeOut() {
        return _timeout;
    }

    public boolean isIfExist() {
        return _ifExist;
    }

    @Override
    public String getLRMIMethodTrackingId() {
        return isTake() ? "takeMultiple" : "readMultiple";
    }

    private static final short FLAG_TRANSACTION = 1 << 0;
    private static final short FLAG_MAX_ENTRIES = 1 << 1;
    private static final short FLAG_MODIFIERS = 1 << 2;
    private static final short FLAG_IS_TAKE = 1 << 3;
    private static final short FLAG_TIMEOUT = 1 << 4;
    private static final short FLAG_MIN_ENTRIES = 1 << 5;
    private static final short FLAG_IS_IFEXIST = 1 << 6;

    private static final int DEFAULT_MODIFIERS = ReadModifiers.REPEATABLE_READ;
    private static final int DEFAULT_MAX_ENTRIES = Integer.MAX_VALUE;

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);

        final short flags = buildFlags();
        out.writeShort(flags);

        IOUtils.writeObject(out, _templatePacket);
        if (flags != 0) {
            if (_txn != null)
                IOUtils.writeWithCachedStubs(out, _txn);
            if (_maxResults != DEFAULT_MAX_ENTRIES)
                out.writeInt(_maxResults);
            if (_modifiers != DEFAULT_MODIFIERS)
                out.writeInt(_modifiers);
            if (_timeout != 0)
                out.writeLong(_timeout);
            if (_maxResults != _minResultsToWaitFor)
                out.writeInt(_minResultsToWaitFor);

        }
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);

        final short flags = in.readShort();
        _templatePacket = IOUtils.readObject(in);
        if (flags != 0) {
            if ((flags & FLAG_TRANSACTION) != 0)
                _txn = IOUtils.readWithCachedStubs(in);
            _maxResults = (flags & FLAG_MAX_ENTRIES) != 0 ? in.readInt() : DEFAULT_MAX_ENTRIES;
            _modifiers = (flags & FLAG_MODIFIERS) != 0 ? in.readInt() : DEFAULT_MODIFIERS;
            _isTake = (flags & FLAG_IS_TAKE) != 0;
            _timeout = (flags & FLAG_TIMEOUT) != 0 ? in.readLong() : 0;
            _minResultsToWaitFor = (flags & FLAG_MIN_ENTRIES) != 0 ? in.readInt() : _maxResults;
            _ifExist = (flags & FLAG_IS_IFEXIST) != 0;
        } else {
            _modifiers = DEFAULT_MODIFIERS;
            _maxResults = DEFAULT_MAX_ENTRIES;
            _minResultsToWaitFor = _maxResults;
        }
    }

    @Override
    public RemoteOperationRequest<ReadTakeEntriesSpaceOperationResult> createCopy(int targetPartitionId) {
        ReadTakeEntriesSpaceOperationRequest copy = (ReadTakeEntriesSpaceOperationRequest) super.createCopy(targetPartitionId);
        copy._templatePacket = _templatePacket.clone();
        return copy;
    }

    private short buildFlags() {
        short flags = 0;
        if (_txn != null)
            flags |= FLAG_TRANSACTION;
        if (_maxResults != DEFAULT_MAX_ENTRIES)
            flags |= FLAG_MAX_ENTRIES;
        if (_modifiers != DEFAULT_MODIFIERS)
            flags |= FLAG_MODIFIERS;
        if (_isTake)
            flags |= FLAG_IS_TAKE;
        if (_timeout != 0)
            flags |= FLAG_TIMEOUT;
        if (_minResultsToWaitFor != _maxResults)
            flags |= FLAG_MIN_ENTRIES;
        if (_ifExist)
            flags |= FLAG_IS_IFEXIST;


        return flags;
    }

    @Override
    public boolean hasLockedResources() {
        return getRemoteOperationResult().getEntryPackets() != null
                && getRemoteOperationResult().getEntryPackets().length > 0;
    }

    @Override
    public boolean requiresPartitionedPreciseDistribution() {
        return _isTake;
    }

    @Override
    public int getPreciseDistributionGroupingCode() {
        if (_isTake)
            return SpacePreciseDistributionGroupingCodes.TAKE;

        throw new UnsupportedOperationException();
    }

    public List<ReplicationLevel> getLevels() {
        return levels;
    }


    public void afterOperationExecution(int partitionId) {
        processExplainPlan(getRemoteOperationResult());
    }

    private void processExplainPlan(SpaceOperationResult result) {
        if (result != null && result.getExplainPlan() != null && explainPlan != null) {
            explainPlan.aggregate(result.getExplainPlan());
        }
    }
}
