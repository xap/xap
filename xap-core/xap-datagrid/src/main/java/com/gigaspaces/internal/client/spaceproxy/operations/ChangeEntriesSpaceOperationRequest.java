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

import com.gigaspaces.client.ChangeException;
import com.gigaspaces.client.ChangeResult;
import com.gigaspaces.client.ChangedEntryDetails;
import com.gigaspaces.client.FailedChangedEntryDetails;
import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.internal.client.ChangeDetailedResultImpl;
import com.gigaspaces.internal.client.ChangeEntryDetailsImpl;
import com.gigaspaces.internal.client.ChangeResultImpl;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.query.QueryUtils;
import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterExecutionType;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterRemoteOperationRouter;
import com.gigaspaces.internal.server.space.operations.SpaceOperationsCodes;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.j_spaces.core.client.Modifiers;

import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author eitany
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class ChangeEntriesSpaceOperationRequest extends SpaceOperationRequest<ChangeEntriesSpaceOperationResult> {
    private static final long serialVersionUID = 1L;

    private static final Logger _devLogger = LoggerFactory.getLogger(Constants.LOGGER_DEV);

    private ITemplatePacket _templatePacket;
    private Transaction _txn;
    private long _timeout;
    private long _lease;
    private int _modifiers;
    private Collection<SpaceEntryMutator> _mutators;

    private transient List<ChangeResult<?>> _updates;
    private transient List<Throwable> _exceptions;
    private transient List<ChangeException> _changeExceptions;
    private transient int _totalNumberOfMatchesEntries;
    private transient Object _query;


    /**
     * Required for Externalizable
     */
    public ChangeEntriesSpaceOperationRequest() {
    }

    public ChangeEntriesSpaceOperationRequest(ITemplatePacket templatePacket,
                                              Transaction txn, long timeout, long lease, int modifiers, Collection<SpaceEntryMutator> mutators, Object query) {
        _templatePacket = templatePacket;
        _txn = txn;
        _timeout = timeout;
        _lease = lease;
        _modifiers = modifiers;
        _mutators = mutators;
        _query = query;
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("templatePacket", _templatePacket);
        textualizer.append("txn", _txn);
        textualizer.append("timeout", _timeout);
        textualizer.append("lease", _lease);
        textualizer.append("modifiers", _modifiers);
        textualizer.append("mutators", _mutators);
    }

    @Override
    public int getOperationCode() {
        return SpaceOperationsCodes.CHANGE_ENTRIES;
    }

    @Override
    public ChangeEntriesSpaceOperationResult createRemoteOperationResult() {
        return new ChangeEntriesSpaceOperationResult();
    }

    @Override
    public PartitionedClusterExecutionType getPartitionedClusterExecutionType() {
        if (_templatePacket.getRoutingFieldValue() != null)
            return PartitionedClusterExecutionType.SINGLE;

        return PartitionedClusterExecutionType.BROADCAST_CONCURRENT;
    }

    @Override
    public boolean processPartitionResult(
            ChangeEntriesSpaceOperationResult remoteOperationResult,
            List<ChangeEntriesSpaceOperationResult> previousResults,
            int numOfPartitions) {
        _totalNumberOfMatchesEntries += remoteOperationResult.getNumOfEntriesMatched();
        if (getRemoteOperationResult() == null)
            setRemoteOperationResult(remoteOperationResult);

        if (numOfPartitions > 1 || (remoteOperationResult.getExecutionException() != null && !(remoteOperationResult.getExecutionException() instanceof ChangeException))) {
            if (remoteOperationResult.getExecutionException() != null) {
                Exception ex = remoteOperationResult.getExecutionException();
                if ((ex instanceof ChangeException)) {
                    if (_changeExceptions == null)
                        _changeExceptions = new ArrayList<ChangeException>(numOfPartitions);
                    _changeExceptions.add((ChangeException) ex);
                } else {
                    if (_exceptions == null)
                        _exceptions = new ArrayList<Throwable>(numOfPartitions);
                    _exceptions.add(ex);
                }
            }
            if (remoteOperationResult.getChangeResult() != null) {
                if (_updates == null)
                    _updates = new ArrayList<ChangeResult<?>>(numOfPartitions);
                _updates.add(remoteOperationResult.getChangeResult());
            }

        }

        return numOfPartitions > 1;
    }

    @Override
    public Object getPartitionedClusterRoutingValue(
            PartitionedClusterRemoteOperationRouter router) {
        return _templatePacket.getRoutingFieldValue();
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

    public long getLease() {
        return _lease;
    }

    @Override
    public Transaction getTransaction() {
        return _txn;
    }

    @Override
    public boolean isBlockingOperation() {
        return _timeout != 0;
    }

    public Collection<SpaceEntryMutator> getMutators() {
        return _mutators;
    }

    @Override
    public boolean processUnknownTypeException(List<Integer> positions) {
        if (_templatePacket.isSerializeTypeDesc())
            return false;
        _templatePacket.setSerializeTypeDesc(true);
        return true;
    }

    @Override
    public String getLRMIMethodTrackingId() {
        return "change";
    }

    public ChangeResult<?> getFinalResult() throws RemoteException, TransactionException, InterruptedException {
        final boolean isDetailedResult = Modifiers.contains(_modifiers, Modifiers.RETURN_DETAILED_CHANGE_RESULT);
        if (_updates == null && _changeExceptions == null && _exceptions == null) {//single space
            ChangeEntriesSpaceOperationResult result = getRemoteOperationResult();
            _totalNumberOfMatchesEntries = result.getNumOfEntriesMatched();
            if (_totalNumberOfMatchesEntries > 0
                    && _devLogger.isTraceEnabled())
                _devLogger.trace(_totalNumberOfMatchesEntries
                        + " entries were scanned in the space in order to return the result for the "
                        + "change operation of query "
                        + QueryUtils.getQueryDescription(_query));
            try {
                result.processExecutionException();
            } catch (ChangeException e) {
                if (isDetailedResult)
                    setMutators(e.getSuccesfullChanges());
                throw e;
            }
            ChangeResult<?> changeResult = result.getChangeResult();
            if (isDetailedResult)
                setMutators((Iterable) changeResult.getResults());

            return changeResult;
        }

        if (_totalNumberOfMatchesEntries > 0
                && _devLogger.isTraceEnabled())
            _devLogger.trace(_totalNumberOfMatchesEntries
                    + " entries were scanned in the space in order to return the result for the "
                    + "change operation of query "
                    + QueryUtils.getQueryDescription(_query));

        //create a unified response
        Collection<ChangedEntryDetails<?>> allChangedEntries = null;
        int numChanged = 0;
        if (_updates != null) {
            for (ChangeResult<?> update : _updates) {
                if (isDetailedResult && update.getNumberOfChangedEntries() > 0) {
                    if (allChangedEntries == null)
                        allChangedEntries = new LinkedList<ChangedEntryDetails<?>>();
                    Collection<?> results = update.getResults();
                    setMutators((Iterable) results);
                    allChangedEntries.addAll((Collection<ChangedEntryDetails<?>>) results);
                }
                numChanged += update.getNumberOfChangedEntries();
            }
        }
        if (_exceptions == null && _changeExceptions == null) {
            return isDetailedResult ? new ChangeDetailedResultImpl(allChangedEntries == null ? Collections.EMPTY_LIST : allChangedEntries) : new ChangeResultImpl(numChanged);
        }
        Collection<FailedChangedEntryDetails> rejectedEntries = null;
        if (_changeExceptions != null) {//derive from change-exceptions all the failed and changed info
            for (ChangeException ce : _changeExceptions) {
                if (ce.getErrors() != null && ce.getErrors().size() > 0) {
                    if (_exceptions == null)
                        _exceptions = new ArrayList<Throwable>();
                    _exceptions.addAll(ce.getErrors());
                }
                if (ce.getFailedChanges() != null && ce.getFailedChanges().size() > 0) {
                    if (rejectedEntries == null)
                        rejectedEntries = new ArrayList<FailedChangedEntryDetails>();
                    rejectedEntries.addAll(ce.getFailedChanges());
                }
                if (ce.getNumSuccesfullChanges() > 0) {
                    if (isDetailedResult) {
                        if (allChangedEntries == null)
                            allChangedEntries = new LinkedList<ChangedEntryDetails<?>>();
                        setMutators(ce.getSuccesfullChanges());
                        allChangedEntries.addAll(ce.getSuccesfullChanges());
                    }
                    numChanged += ce.getNumSuccesfullChanges();
                }
            }
        }

        //create a changeExecption and return it

        String msg = null;
        if (rejectedEntries != null && !rejectedEntries.isEmpty()) {
            msg = "Failed to change some entries in space";
        } else {
            msg = "error in change operation";
            if (_exceptions != null && !_exceptions.isEmpty()) {
                if (_exceptions.get(0).getCause() != null)
                    msg = msg + (_exceptions.get(0).getCause().getMessage() != null ? _exceptions.get(0).getCause().getMessage() : _exceptions.get(0).getCause());
            }
        }
        ChangeException cex = null;
        if (isDetailedResult)
            cex = new ChangeException(msg, allChangedEntries != null ? allChangedEntries : Collections.EMPTY_LIST, rejectedEntries != null ? rejectedEntries : Collections.EMPTY_LIST,
                    _exceptions != null ? _exceptions : Collections.EMPTY_LIST);
        else
            cex = new ChangeException(msg, numChanged, rejectedEntries != null ? rejectedEntries : Collections.EMPTY_LIST,
                    _exceptions != null ? _exceptions : Collections.EMPTY_LIST);
        throw cex;

    }

    protected void setMutators(Iterable<ChangedEntryDetails<?>> entriesDetails) {
        for (ChangedEntryDetails<?> entryDetails : entriesDetails) {
            if (entryDetails instanceof ChangeEntryDetailsImpl<?>)
                ((ChangeEntryDetailsImpl) entryDetails).setMutators(_mutators);
        }
    }

    @Override
    public RemoteOperationRequest<ChangeEntriesSpaceOperationResult> createCopy(int targetPartitionId) {
        ChangeEntriesSpaceOperationRequest copy = (ChangeEntriesSpaceOperationRequest) super.createCopy(targetPartitionId);
        copy._templatePacket = _templatePacket.clone();
        return copy;
    }


    @Override
    public Object getAsyncFinalResult() throws Exception {
        return getFinalResult();
    }

    private static final short FLAG_TRANSACTION = 1 << 0;
    private static final short FLAG_TIMEOUT = 1 << 1;
    private static final short FLAG_LEASE = 1 << 2;
    private static final short FLAG_MODIFIERS = 1 << 3;

    private static final int DEFAULT_MODIFIERS = 0;
    private static final long DEFAULT_TIMEOUT = 0;
    private static final long DEFAULT_LEASE = 0;

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        final short flags = buildFlags();
        out.writeShort(flags);
        IOUtils.writeObject(out, _templatePacket);
        PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        if (version.greaterOrEquals(PlatformLogicalVersion.v12_1_0)) {
            IOUtils.serializeSupportCodeChangeCollection(out, _mutators);
        }
        else {
            IOUtils.writeObject(out, _mutators);
        }
        if (flags != 0) {
            if (_txn != null)
                IOUtils.writeWithCachedStubs(out, _txn);
            if (_timeout != DEFAULT_TIMEOUT)
                out.writeLong(_timeout);
            if (_lease != DEFAULT_LEASE)
                out.writeLong(_lease);
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
        PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        if (version.greaterOrEquals(PlatformLogicalVersion.v12_1_0)) {
            this._mutators = new LinkedList<SpaceEntryMutator>();
            IOUtils.deserializeSupportCodeChangeCollection(in, _mutators);
        }
        else {
            this._mutators = IOUtils.readObject(in);
        }
        if (flags != 0) {
            if ((flags & FLAG_TRANSACTION) != 0)
                this._txn = IOUtils.readWithCachedStubs(in);
            this._timeout = (flags & FLAG_TIMEOUT) != 0 ? in.readLong() : DEFAULT_TIMEOUT;
            this._lease = (flags & FLAG_LEASE) != 0 ? in.readLong() : DEFAULT_LEASE;
            this._modifiers = (flags & FLAG_MODIFIERS) != 0 ? in.readInt() : DEFAULT_MODIFIERS;
        } else {
            this._timeout = DEFAULT_TIMEOUT;
            this._lease = DEFAULT_LEASE;
            this._modifiers = DEFAULT_MODIFIERS;
        }
    }


    private short buildFlags() {
        short flags = 0;

        if (_txn != null)
            flags |= FLAG_TRANSACTION;
        if (_timeout != DEFAULT_TIMEOUT)
            flags |= FLAG_TIMEOUT;
        if (_lease != DEFAULT_LEASE)
            flags |= FLAG_LEASE;
        if (_modifiers != DEFAULT_MODIFIERS)
            flags |= FLAG_MODIFIERS;

        return flags;
    }


}
