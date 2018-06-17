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

import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.internal.cluster.node.IReplicationInBatchContext;
import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.ReplicationInContentContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationTransactionalPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ITransactionalBatchExecutionCallback;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ITransactionalExecutionCallback;
import com.gigaspaces.internal.cluster.node.impl.packets.data.filters.ChangeReplicationFilterUidDataWrapper;
import com.gigaspaces.internal.cluster.node.impl.packets.data.filters.ITimeToLiveUpdateCallback;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.ObjectTypes;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.cluster.IReplicationFilterEntry;
import com.j_spaces.core.cluster.ReplicationOperationType;
import net.jini.core.transaction.Transaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.LinkedList;
import java.util.logging.Logger;


@com.gigaspaces.api.InternalApi
public class ChangeReplicationPacketData
        extends SingleUidReplicationPacketData
        implements IReplicationTransactionalPacketEntryData, ITimeToLiveUpdateCallback {

    private static final long serialVersionUID = 1L;

    private static final String ChangeReplicationPacketData = null;

    private static final Logger _logger = Logger.getLogger("com.gigaspaces.internal.cluster.node.impl.packets.data.operations.ChangeReplicationPacketData");

    private transient long _expirationTime;
    private transient IEntryData _previousEntryData;

    private int _routingHash;
    private Collection<SpaceEntryMutator> _spaceEntryMutators;
    private long _timeToLive;
    private String _typeName;
    private Object _id;
    private int _version;
    private int _previousVersion;

    public static final ThreadLocal<Boolean> forRecovery = new ThreadLocal<Boolean>() {
        @Override
        public Boolean initialValue() {
            return false;
        }
    };


    public ChangeReplicationPacketData() {
    }

    public ChangeReplicationPacketData(String typeName, String uid,
                                       Object id, int version, int previousVersion,
                                       boolean isTransient, IEntryData entryData,
                                       OperationID operationID,
                                       int routingHash, Collection<SpaceEntryMutator> spaceEntryMutators, IEntryData previousEntryData,
                                       long timeToLive, long expirationTime, boolean fromGateway) {
        super(uid, operationID, isTransient, fromGateway, entryData);
        _typeName = typeName;
        _id = id;
        _version = version;
        _previousVersion = previousVersion;
        _routingHash = routingHash;
        _spaceEntryMutators = spaceEntryMutators;
        _previousEntryData = previousEntryData;
        _timeToLive = timeToLive;
        _expirationTime = expirationTime;
    }

    @Override
    public ReplicationSingleOperationType getOperationType() {
        return ReplicationSingleOperationType.CHANGE;
    }

    @Override
    public boolean filterIfNotPresentInReplicaState() {
        return true;
    }

    @Override
    public void executeTransactional(IReplicationInContext context,
                                     ITransactionalExecutionCallback transactionExecutionCallback,
                                     Transaction transaction, boolean twoPhaseCommit) throws Exception {
        try {
            transactionExecutionCallback.changeEntry(context,
                    transaction,
                    twoPhaseCommit,
                    getTypeName(),
                    getUid(),
                    _id,
                    _version,
                    _previousVersion,
                    _timeToLive,
                    _routingHash,
                    _spaceEntryMutators,
                    isTransient(),
                    getOperationId(),
                    _previousEntryData);
        } finally {
            ReplicationInContentContext contentContext = context.getContentContext();
            if (contentContext != null) {
                if (contentContext.getMainEntryData() != null)
                    _entryData = contentContext.getMainEntryData();
                if (contentContext.getSecondaryEntryData() != null)
                    _previousEntryData = contentContext.getSecondaryEntryData();

                contentContext.clear();
            }
        }
    }

    @Override
    public void batchExecuteTransactional(IReplicationInBatchContext context,
                                          ITransactionalBatchExecutionCallback transactionExecutionCallback)
            throws Exception {
        transactionExecutionCallback.changeEntry(context,
                getTypeName(),
                getUid(),
                _id,
                _version,
                _previousVersion,
                _timeToLive,
                _routingHash,
                _spaceEntryMutators,
                isTransient(),
                getOperationId());
    }

    @Override
    public boolean beforeDelayedReplication() {
        _timeToLive = updateTimeToLiveIfNeeded(_expirationTime, _timeToLive);
        return true;
    }

    @Override
    protected void executeImpl(IReplicationInContext context,
                               IReplicationInFacade inReplicationHandler) throws Exception {
        try {
            inReplicationHandler.inChangeEntry(context,
                    getTypeName(),
                    getUid(),
                    _id,
                    _version,
                    _previousVersion,
                    _routingHash,
                    _timeToLive,
                    _spaceEntryMutators,
                    isTransient(),
                    getOperationId());
        } finally {
            ReplicationInContentContext contentContext = context.getContentContext();
            if (contentContext != null
                    && contentContext.getSecondaryEntryData() != null) {
                _previousEntryData = contentContext.getSecondaryEntryData();
                // The upper layer will fill in the main entry data and clear
                // the context
            }
        }
    }

    @Override
    public String getTypeName() {
        return _typeName;
    }

    @Override
    protected int getFilterObjectType() {
        return ObjectTypes.ENTRY;
    }

    @Override
    public boolean supportsReplicationFilter() {
        return true;
    }

    @Override
    public IReplicationFilterEntry toFilterEntry(
            SpaceTypeManager spaceTypeManager) {
        IServerTypeDesc serverTypeDesc = getTypeDescriptor(spaceTypeManager);
        ITypeDesc typeDesc = serverTypeDesc == null ? null
                : serverTypeDesc.getTypeDesc();

        return new ChangeReplicationFilterUidDataWrapper(this,
                getUid(),
                typeDesc,
                getFilterOldReplicationOpType(),
                getFilterObjectType(),
                getTimeToLiveUpdateCallback(),
                _version,
                _spaceEntryMutators,
                _id);
    }

    @Override
    protected ReplicationOperationType getFilterOldReplicationOpType() {
        return ReplicationOperationType.CHANGE;
    }

    @Override
    protected ITimeToLiveUpdateCallback getTimeToLiveUpdateCallback() {
        return this;
    }

    @Override
    public void updateTimeToLive(long newTimeToLive) {
        _timeToLive = newTimeToLive;

    }

    @Override
    public long getTimeToLive() {
        return _timeToLive;
    }


    @Override
    public IEntryData getSecondaryEntryData() {
        return _previousEntryData;
    }

    @Override
    public boolean containsFullEntryData() {
        return true;
    }

    @Override
    public Collection<SpaceEntryMutator> getCustomContent() {
        return _spaceEntryMutators;
    }

    @Override
    public ChangeReplicationPacketData clone() {
        ChangeReplicationPacketData clone = (ChangeReplicationPacketData) super.clone();
        LinkedList<SpaceEntryMutator> clonedMutators = new LinkedList<SpaceEntryMutator>();
        for (SpaceEntryMutator spaceEntryMutator : _spaceEntryMutators)
            clonedMutators.add(spaceEntryMutator);

        clone._spaceEntryMutators = clonedMutators;
        return clone;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        writeExternalImpl(out, LRMIInvocationContext.getEndpointLogicalVersion());
        if (forRecovery.get()) {
            _logger.fine("Serializing packet data "+this+" with full entryData due to recovery process");
            serializeEntryData(_entryData, out);
        }
    }

    private static final byte FLAGS_TIME_TO_LIVE = 1 << 0;
    private static final byte FLAGS_PREVIOUS_VERSION = 1 << 1;
    private static final byte FLAGS_ID = 1 << 2;

    private static final long DEFAULT_TIME_TO_LIVE = Long.MAX_VALUE;

    private byte buildFlags() {
        byte flags = 0;
        if (_timeToLive != DEFAULT_TIME_TO_LIVE)
            flags |= FLAGS_TIME_TO_LIVE;
        if (_previousVersion != 0)
            flags |= FLAGS_PREVIOUS_VERSION;
        if (!getUid().equals(_id))
            flags |= FLAGS_ID;
        return flags;
    }

    private final void writeExternalImpl(ObjectOutput out, PlatformLogicalVersion version) throws IOException {
        if (version.greaterOrEquals(PlatformLogicalVersion.v9_5_0)) {
            byte flags = buildFlags();
            out.writeByte(flags);
            IOUtils.writeRepetitiveString(out, _typeName);
            out.writeInt(_version);
            out.writeInt(_routingHash);
            IOUtils.writeObject(out, _spaceEntryMutators);

            if ((flags & FLAGS_TIME_TO_LIVE) != 0)
                out.writeLong(_timeToLive);
            if ((flags & FLAGS_PREVIOUS_VERSION) != 0)
                out.writeInt(_previousVersion);
            if ((flags & FLAGS_ID) != 0)
                IOUtils.writeObject(out, _id);
        } else {
            IOUtils.writeRepetitiveString(out, _typeName);
            out.writeInt(_version);
            out.writeInt(_routingHash);
            out.writeLong(_timeToLive);
            IOUtils.writeObject(out, _spaceEntryMutators);
        }

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        readExternalImpl(in, LRMIInvocationContext.getEndpointLogicalVersion());
        _expirationTime = _timeToLive != Long.MAX_VALUE ? _timeToLive + SystemTime.timeMillis() : Long.MAX_VALUE;
        if (forRecovery.get()) {
            _entryData = deserializeEntryData(in);
        }
    }

    private void readExternalImpl(ObjectInput in, PlatformLogicalVersion version) throws IOException,
            ClassNotFoundException {
        if (version.greaterOrEquals(PlatformLogicalVersion.v9_5_0)) {
            byte flags = in.readByte();
            _typeName = IOUtils.readRepetitiveString(in);
            _version = in.readInt();
            _routingHash = in.readInt();
            _spaceEntryMutators = IOUtils.readObject(in);

            if ((flags & FLAGS_TIME_TO_LIVE) != 0)
                _timeToLive = in.readLong();
            else
                _timeToLive = DEFAULT_TIME_TO_LIVE;

            if ((flags & FLAGS_PREVIOUS_VERSION) != 0)
                _previousVersion = in.readInt();
            else
                _previousVersion = 0;

            if ((flags & FLAGS_ID) != 0)
                _id = IOUtils.readObject(in);
            else
                _id = getUid();
        } else {
            _typeName = IOUtils.readRepetitiveString(in);
            _version = in.readInt();
            _routingHash = in.readInt();
            _timeToLive = in.readLong();
            _spaceEntryMutators = IOUtils.readObject(in);
        }
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);
        writeExternalImpl(out, PlatformLogicalVersion.getLogicalVersion());
        out.writeLong(_expirationTime);
        serializeEntryData(_previousEntryData, out);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);
        readExternalImpl(in, PlatformLogicalVersion.getLogicalVersion());
        _expirationTime = in.readLong();
        _previousEntryData = deserializeEntryData(in);
    }

    public long getExpirationTime() {
        return _expirationTime;
    }

    @Override
    public String toString() {
        return "CHANGE: (typeName=" + getTypeName() + " uid=" + getUid()
                + " operationID=" + getOperationId() + " version=" + _version
                + " mutators=" + _spaceEntryMutators + ")";
    }

}
