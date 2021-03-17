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
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterExecutionType;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterRemoteOperationRouter;
import com.gigaspaces.internal.server.space.operations.SpaceOperationsCodes;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.utils.Textualizer;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.client.UpdateModifiers;

import net.jini.core.lease.Lease;
import net.jini.core.transaction.Transaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author idan
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class WriteEntrySpaceOperationRequest extends SpaceOperationRequest<WriteEntrySpaceOperationResult> {
    private static final long serialVersionUID = 1L;

    private IEntryPacket _entryPacket;
    private Transaction _txn;
    private long _lease;
    private long _timeout;
    private int _modifiers;
    private boolean _isUpdate;

    /**
     * Required for Externalizable
     */
    public WriteEntrySpaceOperationRequest() {
    }

    public WriteEntrySpaceOperationRequest(IEntryPacket entryPacket, Transaction txn, long lease, long timeout, int modifiers, boolean isUpdate) {
        _entryPacket = entryPacket;
        _txn = txn;
        _lease = lease;
        _timeout = timeout;
        _modifiers = modifiers;
        _isUpdate = isUpdate;
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("entryPacket", _entryPacket);
        textualizer.append("txn", _txn);
        textualizer.append("lease", _lease);
        textualizer.append("timeout", _timeout);
        textualizer.append("modifiers", _modifiers);
        textualizer.append("isUpdate", _isUpdate);
    }

    @Override
    public WriteEntrySpaceOperationResult createRemoteOperationResult() {
        return new WriteEntrySpaceOperationResult();
    }

    @Override
    public PartitionedClusterExecutionType getPartitionedClusterExecutionType() {
        return PartitionedClusterExecutionType.SINGLE;
    }

    @Override
    public Object getPartitionedClusterRoutingValue(PartitionedClusterRemoteOperationRouter router) {
        Object routingValue =  _entryPacket.getTypeDescriptor().isBroadcast() ? 0 : _entryPacket.getRoutingFieldValue();
        if (routingValue != null)
            return routingValue;
        if (_entryPacket.getTypeDescriptor().isAutoGenerateRouting())
            return router.getNextPreciseDistributionPartitionId(getPreciseDistributionGroupingCode());

        return null;
    }

    @Override
    public int getOperationCode() {
        return SpaceOperationsCodes.WRITE_ENTRY;
    }

    public IEntryPacket getEntryPacket() {
        return _entryPacket;
    }

    public long getLease() {
        return _lease;
    }

    public int getModifiers() {
        return _modifiers;
    }

    @Override
    public boolean isDirectExecutionEnabled() {
        return Modifiers.contains(_modifiers, Modifiers.RUN_ON_IO_THREAD);
    }

    public boolean isUpdate() {
        return _isUpdate;
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

    @Override
    public boolean processUnknownTypeException(List<Integer> positions) {
        if (_entryPacket.isSerializeTypeDesc())
            return false;
        _entryPacket.setSerializeTypeDesc(true);
        return true;
    }

    @Override
    public String getLRMIMethodTrackingId() {
        return "write";
    }

    private static final short FLAG_TRANSACTION = 1 << 0;
    private static final short FLAG_LEASE = 1 << 1;
    private static final short FLAG_TIMEOUT = 1 << 2;
    private static final short FLAG_MODIFIERS = 1 << 3;
    private static final short FLAG_UPDATE = 1 << 4;

    private static final long DEFAULT_LEASE = Lease.FOREVER;
    private static final int DEFAULT_MODIFIERS = UpdateModifiers.UPDATE_OR_WRITE;
    private static final long DEFAULT_TIMEOUT = 0;

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        final short flags = buildFlags();
        out.writeShort(flags);
        IOUtils.writeObject(out, _entryPacket);
        if (flags != 0) {
            if (_txn != null)
                IOUtils.writeWithCachedStubs(out, _txn);
            if (_lease != DEFAULT_LEASE)
                out.writeLong(_lease);
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
        this._entryPacket = IOUtils.readObject(in);
        if (flags != 0) {
            if ((flags & FLAG_TRANSACTION) != 0)
                this._txn = IOUtils.readWithCachedStubs(in);
            this._lease = (flags & FLAG_LEASE) != 0 ? in.readLong() : DEFAULT_LEASE;
            this._timeout = (flags & FLAG_TIMEOUT) != 0 ? in.readLong() : DEFAULT_TIMEOUT;
            this._modifiers = (flags & FLAG_MODIFIERS) != 0 ? in.readInt() : DEFAULT_MODIFIERS;
            this._isUpdate = (flags & FLAG_UPDATE) != 0;
        } else {
            this._lease = DEFAULT_LEASE;
            this._timeout = DEFAULT_TIMEOUT;
            this._modifiers = DEFAULT_MODIFIERS;
        }
    }

    private short buildFlags() {
        short flags = 0;

        if (_txn != null)
            flags |= FLAG_TRANSACTION;
        if (_lease != DEFAULT_LEASE)
            flags |= FLAG_LEASE;
        if (_timeout != DEFAULT_TIMEOUT)
            flags |= FLAG_TIMEOUT;
        if (_modifiers != DEFAULT_MODIFIERS)
            flags |= FLAG_MODIFIERS;
        if (_isUpdate)
            flags |= FLAG_UPDATE;

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
