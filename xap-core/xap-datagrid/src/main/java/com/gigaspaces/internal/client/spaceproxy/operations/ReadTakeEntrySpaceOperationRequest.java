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

import com.gigaspaces.internal.client.spaceproxy.metadata.ISpaceProxyTypeManager;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.query.QueryUtils;
import com.gigaspaces.internal.query.explainplan.ExplainPlanImpl;
import com.gigaspaces.query.explainplan.ExplainPlan;
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
import com.j_spaces.core.exception.internal.InterruptedSpaceException;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.RemoteException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author anna
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class ReadTakeEntrySpaceOperationRequest extends SpaceOperationRequest<ReadTakeEntrySpaceOperationResult> {
    private static final long serialVersionUID = 1L;

    private static final Logger _devLogger = LoggerFactory.getLogger(Constants.LOGGER_DEV);

    private ITemplatePacket _templatePacket;
    private Transaction _txn;
    private boolean _isTake;
    private boolean _ifExists;
    private long _timeout;
    private int _modifiers;
    private boolean _returnOnlyUid;

    private transient Exception _executionException;
    private transient IEntryPacket _resultEntry;
    private transient ISpaceProxyTypeManager _typeManager;
    private transient boolean _returnPacket;
    private transient int _totalNumberOfMatchesEntries;
    private transient Object _query;
    private transient ExplainPlanImpl explainPlan;

    /**
     * Required for Externalizable
     */
    public ReadTakeEntrySpaceOperationRequest() {
    }

    public ReadTakeEntrySpaceOperationRequest(ITemplatePacket templatePacket, Transaction txn, boolean isTake,
                                              boolean ifExists, long timeout, int modifiers, boolean returnOnlyUid, Object query) {
        if (timeout < 0)
            throw new IllegalArgumentException("Timeout should be greater or equals 0");
        _templatePacket = templatePacket;
        _txn = txn;
        _isTake = isTake;
        _ifExists = ifExists;
        _timeout = timeout;
        _modifiers = modifiers;
        _returnOnlyUid = returnOnlyUid;
        _query = query;
        this.explainPlan = ExplainPlanImpl.fromQueryPacket(query);
    }

    public ReadTakeEntrySpaceOperationRequest(ITemplatePacket templatePacket, Transaction txn, boolean isTake,
                                              boolean ifExists, long timeout, int modifiers, boolean returnOnlyUid,
                                              ISpaceProxyTypeManager typeManager, boolean returnPacket, Object query) {
        this(templatePacket, txn, isTake, ifExists, timeout, modifiers, returnOnlyUid, query);
        this._typeManager = typeManager;
        this._returnPacket = returnPacket;
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("isTake", _isTake);
        textualizer.append("txn", _txn);
        textualizer.append("modifiers", _modifiers);
        textualizer.append("timeout", _timeout);
        textualizer.append("ifExists", _ifExists);
        textualizer.append("returnOnlyUid", _returnOnlyUid);
        textualizer.append("template", _templatePacket);
    }

    @Override
    public int getOperationCode() {
        return SpaceOperationsCodes.READ_TAKE_ENTRY;
    }

    @Override
    public ReadTakeEntrySpaceOperationResult createRemoteOperationResult() {
        return new ReadTakeEntrySpaceOperationResult();
    }

    @Override
    public PartitionedClusterExecutionType getPartitionedClusterExecutionType() {
        if (_templatePacket.getRoutingFieldValue() != null || _templatePacket.isBroadcast())
            return PartitionedClusterExecutionType.SINGLE;

        if (_timeout != 0)
            throw new IllegalArgumentException("Broadcast is not supported for Read/Take Operations with timeout greater than 0. (Type='"
                    + _templatePacket.getTypeName()
                    + "' ,routing property='"
                    + _templatePacket.getTypeDescriptor()
                    .getRoutingPropertyName() + "')");

        if (_isTake || _txn != null)
            return PartitionedClusterExecutionType.BROADCAST_SEQUENTIAL;

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

    public long getTimeout() {
        return _timeout;
    }

    @Override
    public Transaction getTransaction() {
        return _txn;
    }

    @Override
    public boolean isBlockingOperation() {
        return _timeout != 0;
    }

    public boolean isTake() {
        return _isTake;
    }

    public boolean isIfExists() {
        return _ifExists;
    }

    public boolean isReturnOnlyUid() {
        return _returnOnlyUid;
    }

    @Override
    public boolean processUnknownTypeException(List<Integer> positions) {
        if (_templatePacket.isSerializeTypeDesc())
            return false;
        _templatePacket.setSerializeTypeDesc(true);
        return true;
    }

    @Override
    public boolean processPartitionResult(ReadTakeEntrySpaceOperationResult partitionResult,
                                          List<ReadTakeEntrySpaceOperationResult> previousResults,
                                          int numOfPartitions) {
        _totalNumberOfMatchesEntries += partitionResult.getNumOfEntriesMatched();
        if (partitionResult.getExecutionException() != null) {
            _executionException = partitionResult.getExecutionException();
            return false;
        }

        if (_resultEntry == null)
            _resultEntry = partitionResult.getEntryPacket();

        processExplainPlan(partitionResult);
        return _resultEntry == null;
    }

    public IEntryPacket getFinalResult()
            throws RemoteException, TransactionException, InterruptedException, UnusableEntryException {
        if (getRemoteOperationResult() != null) {
            _executionException = getRemoteOperationResult().getExecutionException();
            _resultEntry = getRemoteOperationResult().getEntryPacket();
            _totalNumberOfMatchesEntries = getRemoteOperationResult().getNumOfEntriesMatched();
        }

        if (_executionException != null)
            processExecutionException(_executionException);

        if (_totalNumberOfMatchesEntries > 0
                && _devLogger.isTraceEnabled())
            _devLogger.trace(_totalNumberOfMatchesEntries
                    + " entries were scanned in the space in order to return the result for the "
                    + (_isTake ? "take" : "read") + " operation of query "
                    + QueryUtils.getQueryDescription(_query));

        return _resultEntry;
    }

    private void processExecutionException(Exception executionException)
            throws TransactionException, RemoteException, InterruptedException, UnusableEntryException {
        if (executionException instanceof TransactionException)
            throw (TransactionException) executionException;
        if (executionException instanceof RemoteException)
            throw (RemoteException) executionException;
        if (executionException instanceof InterruptedSpaceException && executionException.getCause() instanceof InterruptedException)
            throw (InterruptedException) executionException.getCause();
        if (executionException instanceof InterruptedException)
            throw (InterruptedException) executionException;
        if (executionException instanceof UnusableEntryException)
            throw (UnusableEntryException) executionException;

        SpaceOperationResult.onUnexpectedException(executionException);
    }

    @Override
    public Object getAsyncFinalResult() throws Exception {
        IEntryPacket resultPacket = getFinalResult();
        return _typeManager.convertQueryResult(resultPacket, _templatePacket, _returnPacket);
    }

    @Override
    public String getLRMIMethodTrackingId() {
        return isTake() ? "take" : "read";
    }

    private static final short FLAG_TRANSACTION = 1 << 0;
    private static final short FLAG_TIMEOUT = 1 << 1;
    private static final short FLAG_MODIFIERS = 1 << 2;
    private static final short FLAG_IS_TAKE = 1 << 3;
    private static final short FLAG_IF_EXISTS = 1 << 4;
    private static final short FLAG_RETURN_ONLY_UID = 1 << 5;

    private static final int DEFAULT_MODIFIERS = ReadModifiers.REPEATABLE_READ;
    private static final long DEFAULT_TIMEOUT = 0;

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        final short flags = buildFlags();
        out.writeShort(flags);
        IOUtils.writeObject(out, _templatePacket);
        if (flags != 0) {
            if (_txn != null)
                IOUtils.writeWithCachedStubs(out, _txn);
            if (_timeout != DEFAULT_TIMEOUT)
                out.writeLong(_timeout);
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
            this._timeout = (flags & FLAG_TIMEOUT) != 0 ? in.readLong() : DEFAULT_TIMEOUT;
            this._modifiers = (flags & FLAG_MODIFIERS) != 0 ? in.readInt() : DEFAULT_MODIFIERS;
            this._isTake = (flags & FLAG_IS_TAKE) != 0;
            this._ifExists = (flags & FLAG_IF_EXISTS) != 0;
            this._returnOnlyUid = (flags & FLAG_RETURN_ONLY_UID) != 0;
        } else {
            this._timeout = DEFAULT_TIMEOUT;
            this._modifiers = DEFAULT_MODIFIERS;
        }
    }

    @Override
    public RemoteOperationRequest<ReadTakeEntrySpaceOperationResult> createCopy(int targetPartitionId) {
        ReadTakeEntrySpaceOperationRequest copy = (ReadTakeEntrySpaceOperationRequest) super.createCopy(targetPartitionId);
        copy._templatePacket = _templatePacket.clone();
        return copy;
    }

    private short buildFlags() {
        short flags = 0;

        if (_txn != null)
            flags |= FLAG_TRANSACTION;
        if (_timeout != DEFAULT_TIMEOUT)
            flags |= FLAG_TIMEOUT;
        if (_modifiers != DEFAULT_MODIFIERS)
            flags |= FLAG_MODIFIERS;
        if (_isTake)
            flags |= FLAG_IS_TAKE;
        if (_ifExists)
            flags |= FLAG_IF_EXISTS;
        if (_returnOnlyUid)
            flags |= FLAG_RETURN_ONLY_UID;

        return flags;
    }

    @Override
    public boolean hasLockedResources() {
        return getRemoteOperationResult().getEntryPacket() != null;
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

    public void afterOperationExecution(int partitionId) {
        processExplainPlan(getRemoteOperationResult());
    }

    private void processExplainPlan(SpaceOperationResult result) {
        if(result !=null && result.getExplainPlan() != null){
            explainPlan.aggregate(result.getExplainPlan());
        }
    }

    public ExplainPlan getExplainPlan() {
        return explainPlan;
    }

}
