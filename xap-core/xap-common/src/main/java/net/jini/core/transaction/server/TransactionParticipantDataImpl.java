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
/**
 *
 */
package net.jini.core.transaction.server;

import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationParticipantsMetadata;
import com.gigaspaces.internal.transaction.DefaultTransactionUniqueId;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.serialization.SmartExternalizable;
import com.gigaspaces.transaction.ConsolidatedDistributedTransactionMetaData;
import com.gigaspaces.transaction.TransactionParticipantMetaData;
import com.gigaspaces.transaction.TransactionUniqueId;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Contains ServerTransaction meta data for distributed transactions that have more than 1
 * participant.
 *
 * @author anna
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class TransactionParticipantDataImpl
        implements TransactionParticipantData, TransactionParticipantMetaData, ConsolidatedDistributedTransactionMetaData, SmartExternalizable, IReplicationParticipantsMetadata {

    static final long serialVersionUID = 1L;

    private int _participantId;
    private int _participantsCount;
    private TransactionUniqueId _transactionUniqueId;

    /**
     *
     */
    public TransactionParticipantDataImpl() {
        super();

    }

    /**
     *
     * @param transactionUniqueId
     * @param partitionIdOneBased
     * @param numberOfParticipants
     */
    public TransactionParticipantDataImpl(TransactionUniqueId transactionUniqueId, int partitionIdOneBased,
                                          int numberOfParticipants) {
        this._transactionUniqueId = transactionUniqueId;
        _participantId = partitionIdOneBased;
        _participantsCount = numberOfParticipants;
    }

    /**
     * @see net.jini.core.transaction.server.ServerTransaction#readExternal(java.io.ObjectInput)
     */
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        deserialize(in, LRMIInvocationContext.getEndpointLogicalVersion());
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        deserialize(in, PlatformLogicalVersion.getLogicalVersion());
    }

    private void deserialize(ObjectInput in, PlatformLogicalVersion version) throws IOException,
            ClassNotFoundException {
        _transactionUniqueId = (TransactionUniqueId) in.readObject();
        _participantId = in.readInt();
        _participantsCount = in.readInt();
    }

    /**
     * @see net.jini.core.transaction.server.ServerTransaction#writeExternal(java.io.ObjectOutput)
     */
    public void writeExternal(ObjectOutput out) throws IOException {
        serialize(out, LRMIInvocationContext.getEndpointLogicalVersion());
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        serialize(out, PlatformLogicalVersion.getLogicalVersion());
    }

    private final void serialize(ObjectOutput out, PlatformLogicalVersion version) throws IOException {
        out.writeObject(_transactionUniqueId);
        out.writeInt(_participantId);
        out.writeInt(_participantsCount);
    }

    /**
     * @return the transactionId
     */
    public long getTransactionId() {
        if (_transactionUniqueId instanceof DefaultTransactionUniqueId)
            return ((DefaultTransactionUniqueId) _transactionUniqueId).getTransactionId().longValue();
        return -1;
    }

    @Override
    public TransactionUniqueId getTransactionUniqueId() {
        return _transactionUniqueId;
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.server.ITransactionMetaData#getParticipantId()
     */
    public int getParticipantId() {
        return _participantId;
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.server.ITransactionMetaData#getParticpantsCount()
     */
    public int getTransactionParticipantsCount() {
        return _participantsCount;
    }

    public boolean isUnconsoliated() {
        return _participantsCount > 1 && _participantId != -1;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "DistributedTransactionMetaData [getTransactionUniqueId()="
                + getContextId() + ", getParticipantId()="
                + getParticipantId() + ", getParticipantsCount()="
                + getTransactionParticipantsCount() + "]";
    }

    @Override
    public Object getContextId() {
        return getTransactionUniqueId();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + _participantId;
        result = prime * result + _participantsCount;
        result = prime * result + ((_transactionUniqueId == null) ? 0 : _transactionUniqueId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TransactionParticipantDataImpl other = (TransactionParticipantDataImpl) obj;
        if (_participantId != other._participantId)
            return false;
        if (_participantsCount != other._participantsCount)
            return false;
        if (_transactionUniqueId == null) {
            if (other._transactionUniqueId != null)
                return false;
        } else if (!_transactionUniqueId.equals(other._transactionUniqueId))
            return false;
        return true;
    }


}
