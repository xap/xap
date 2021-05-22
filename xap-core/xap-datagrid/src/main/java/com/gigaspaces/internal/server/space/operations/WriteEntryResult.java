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

package com.gigaspaces.internal.server.space.operations;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.utils.Textualizable;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.LeaseContext;
import com.j_spaces.core.LeaseInitializer;
import com.j_spaces.core.LeaseProxy;
import com.j_spaces.core.ObjectTypes;
import com.j_spaces.core.client.UpdateModifiers;

import net.jini.core.lease.Lease;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class WriteEntryResult implements SmartExternalizable, Textualizable {
    private static final long serialVersionUID = 1L;

    private static final boolean ReturnPrevOnUpdate = Boolean.getBoolean("com.gigaspaces.client.returnPrevOnUpdate");

    private String _uid;
    private int _version;
    private long _expiration;
    private IEntryPacket _prevEntry;
    private int syncReplicationLevel;

    /**
     * Required for Externalizable
     */
    public WriteEntryResult() {
    }

    public WriteEntryResult(String uid, int version, long expiration) {
        this._uid = uid;
        this._version = version;
        this._expiration = expiration;
    }

    public WriteEntryResult(IEntryPacket entryPacket) {
        // TODO: This constructor is for backwards compatibility with old router only - remove it when possible.
        this._uid = entryPacket.getUID();
        this._version = entryPacket.getVersion();
        this._prevEntry = entryPacket;
    }

    @Override
    public String toString() {
        return Textualizer.toString(this);
    }

    public void setSyncReplicationLevel(int syncReplicationLevel) {
        this.syncReplicationLevel = syncReplicationLevel;
    }

    public int getSyncReplicationLevel() {
        return syncReplicationLevel;
    }

    @Override
    public void toText(Textualizer textualizer) {
        textualizer.append("uid", _uid);
        textualizer.append("version", _version);
        textualizer.append("expiration", _expiration);
    }

    public String getUid() {
        return _uid;
    }

    public void removeRedundantData(ITypeDesc typeDesc, int modifiers) {
        if (_version > 1 || (StringUtils.hasLength(typeDesc.getIdPropertyName()) && !typeDesc.isAutoGenerateId()))
            _uid = null;
        // TODO: previous entry packet construction can be skipped if this condition is moved to the correct location.
        if (ReturnPrevOnUpdate) {
            if (UpdateModifiers.isNoReturnValue(modifiers))
                _prevEntry = null;
        } else {
            if (!UpdateModifiers.isReturnPrevOnUpdate(modifiers))
                _prevEntry = null;
        }
    }

    public int getVersion() {
        return _version;
    }

    public long getExpiration() {
        return _expiration;
    }

    public IEntryPacket getPrevEntry() {
        return _prevEntry;
    }

    public void setPrevEntry(IEntryPacket prevEntry) {
        this._prevEntry = prevEntry;
    }

    public LeaseContext<?> createLease(String typeName, SpaceImpl spaceImpl, boolean noWriteLease) {
        if (_expiration == 0) {
            if (_prevEntry != null)
                return LeaseInitializer.createDummyLease(_uid, _version, _prevEntry);
            return LeaseInitializer.createDummyLease(_uid, _version);
        }
        if (noWriteLease)
            return LeaseInitializer.createDummyLease(_uid, _version, _prevEntry);
        LeaseProxy result = new LeaseProxy(_expiration, _uid, typeName, _version, ObjectTypes.ENTRY, spaceImpl);
        result.setObject(_prevEntry);
        return result;
    }

    private static final short FLAG_UID = 1;
    private static final short FLAG_VERSION = 1 << 1;
    private static final short FLAG_EXPIRATION = 1 << 2;
    private static final short FLAG_PREV_ENTRY = 1 << 3;
    private static final short FLAG_SYNC_REPLICATION_LEVEL = 1 << 4;
    private static final int DEFAULT_VERSION = 1;
    private static final long DEFAULT_EXPIRATION = Lease.FOREVER;

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        final short flags = buildFlags();
        out.writeShort(flags);
        if (flags != 0) {
            if (_uid != null)
                IOUtils.writeString(out, _uid);
            if (_version != DEFAULT_VERSION)
                out.writeInt(_version);
            if (_expiration != DEFAULT_EXPIRATION)
                out.writeLong(_expiration);
            if (_prevEntry != null)
                IOUtils.writeObject(out, _prevEntry);
            if (0 < syncReplicationLevel) {
                out.writeInt(syncReplicationLevel);
            }
        }
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        final short flags = in.readShort();
        if (flags != 0) {
            if ((flags & FLAG_UID) != 0)
                this._uid = IOUtils.readString(in);
            _version = ((flags & FLAG_VERSION) != 0 ? in.readInt() : DEFAULT_VERSION);
            _expiration = ((flags & FLAG_EXPIRATION) != 0 ? in.readLong() : DEFAULT_EXPIRATION);
            if ((flags & FLAG_PREV_ENTRY) != 0) {
                this._prevEntry = IOUtils.readObject(in);
            }
            this.syncReplicationLevel = ((flags & FLAG_SYNC_REPLICATION_LEVEL) != 0) ? in.readInt() : 0;
        } else {
            _version = DEFAULT_VERSION;
            _expiration = DEFAULT_EXPIRATION;
        }
    }


    private short buildFlags() {
        short flags = 0;

        if (_uid != null)
            flags |= FLAG_UID;
        if (_version != DEFAULT_VERSION)
            flags |= FLAG_VERSION;
        if (_expiration != DEFAULT_EXPIRATION)
            flags |= FLAG_EXPIRATION;
        if (_prevEntry != null)
            flags |= FLAG_PREV_ENTRY;
        if (0 < syncReplicationLevel) {
            flags |= FLAG_SYNC_REPLICATION_LEVEL;
        }
        return flags;
    }

}
