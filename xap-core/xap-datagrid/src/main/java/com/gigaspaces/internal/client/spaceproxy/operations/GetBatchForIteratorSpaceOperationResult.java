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

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.TransactionException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.RemoteException;

/**
 * @author Alon Shoham
 * @since 15.2.0
 */
@com.gigaspaces.api.InternalApi
public class GetBatchForIteratorSpaceOperationResult extends SpaceOperationResult {
    private static final long serialVersionUID = 1L;

    private IEntryPacket[] _entryPackets;
    private int _numOfEntriesMatched;
    private int syncReplicationLevel;
    private transient Integer _partitionId;

    /**
     * Required for Externalizable
     */
    public GetBatchForIteratorSpaceOperationResult() {
    }

    public GetBatchForIteratorSpaceOperationResult(IEntryPacket[] entries) {
        this._entryPackets = entries;
    }

    public GetBatchForIteratorSpaceOperationResult(IEntryPacket[] entries, Exception ex) {
        this._entryPackets = entries;
        super.setExecutionException(ex);
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("entryPackets", _entryPackets);
    }

    public void processExecutionException() throws TransactionException, RemoteException, UnusableEntryException {
        final Exception executionException = getExecutionException();
        if (executionException == null)
            return;
        if (executionException instanceof TransactionException)
            throw (TransactionException) executionException;
        if (executionException instanceof RemoteException)
            throw (RemoteException) executionException;
        if (executionException instanceof UnusableEntryException)
            throw (UnusableEntryException) executionException;

        onUnexpectedException(executionException);
    }

    public int getSyncReplicationLevel() {
        return syncReplicationLevel;
    }

    public void setSyncReplicationLevel(int syncReplicationLevel) {
        this.syncReplicationLevel = syncReplicationLevel;
    }

    public IEntryPacket[] getEntryPackets() {
        return _entryPackets;
    }

    public void setEntryPackets(IEntryPacket[] entryPackets) {
        _entryPackets = entryPackets;
    }

    public void setNumOfEntriesMatched(int numOfEntriesMatched) {
        _numOfEntriesMatched = numOfEntriesMatched;
    }

    public int getNumOfEntriesMatched() {
        return _numOfEntriesMatched;
    }

    public Integer getPartitionId() {
        return _partitionId;
    }

    public void setPartitionId(Integer partitionId) {
        this._partitionId = partitionId;
    }

    private static final short FLAG_ENTRY_PACKETS = 1 << 0;
    private static final short FLAG_NUM_OF_ENTRIES_MATCHED = 1 << 1;
    private static final short FLAG_SYNC_REPLICATION_LEVEL = 1 << 2;


    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);
        final short flags = buildFlags();
        out.writeShort(flags);
        if (flags != 0) {
            if (_entryPackets != null) {
                out.writeObject(_entryPackets);
            }
            if (_numOfEntriesMatched > 0) {
                out.writeInt(_numOfEntriesMatched);
            }
            if (0 < syncReplicationLevel) {
                out.writeInt(syncReplicationLevel);
            }
        }
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);
        final short flags = in.readShort();
        if (flags != 0) {
            if ((flags & FLAG_ENTRY_PACKETS) != 0) {
                this._entryPackets = (IEntryPacket[]) in.readObject();
            }
            if ((flags & FLAG_NUM_OF_ENTRIES_MATCHED) != 0) {
                this._numOfEntriesMatched = in.readInt();
            }
            this.syncReplicationLevel = ((flags & FLAG_SYNC_REPLICATION_LEVEL) != 0) ? in.readInt() : 0;
        }
    }


    private short buildFlags() {
        short flags = 0;

        if (_entryPackets != null) {
            flags |= FLAG_ENTRY_PACKETS;
        }
        if (_numOfEntriesMatched > 0) {
            flags |= FLAG_NUM_OF_ENTRIES_MATCHED;
        }
        if (0 < syncReplicationLevel) {
            flags |= FLAG_SYNC_REPLICATION_LEVEL;
        }

        return flags;
    }

}
