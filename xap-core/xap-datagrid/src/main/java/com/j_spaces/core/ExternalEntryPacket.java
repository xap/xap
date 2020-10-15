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

package com.j_spaces.core;

import com.gigaspaces.internal.client.StorageTypeDeserialization;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.converter.ConversionException;
import com.gigaspaces.internal.transport.EntryPacket;
import com.gigaspaces.internal.transport.TransportPacketType;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.j_spaces.core.client.ExternalEntry;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Represents an ExternalEntry as an IEntryPacket
 *
 * @author assafr
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class ExternalEntryPacket extends EntryPacket {
    private static final long serialVersionUID = 1L;

    // The name of the user class that extends ExternalEntry.
    protected String _implClassName;

    public ExternalEntryPacket() {
    }

    public ExternalEntryPacket(ITypeDesc typeDesc, Object[] values) {
        super(typeDesc, values);
    }

    public ExternalEntryPacket(ITypeDesc typeDesc, EntryType entryType, Object[] fixedProperties,
                               String uid, int version, long timeToLive, boolean isTransient) {
        super(typeDesc, entryType, fixedProperties, null, uid, version, timeToLive, isTransient);
    }

    public ExternalEntryPacket(ITypeDesc typeDesc, EntryType entryType, Object[] fixedProperties,
                               String uid, int version, long timeToLive, boolean isTransient, String implClassName, byte[] binaryFields) {
        super(typeDesc, entryType, fixedProperties, null, uid, version, timeToLive, isTransient, binaryFields);
        _implClassName = implClassName;
    }

    @Override
    public TransportPacketType getPacketType() {
        return TransportPacketType.ENTRY_PACKET;
    }

    @Override
    public String getExternalEntryImplClassName() {
        return _implClassName;
    }

    @Override
    public Object toObject(EntryType entryType, StorageTypeDeserialization storageTypeDeserialization) {
        try {
            if (_typeDesc != null)
                return super.toObject(entryType, storageTypeDeserialization);

            if (getMultipleUIDs() != null)
                return new ExternalEntry(getMultipleUIDs());

            return new ExternalEntry(getUID());
        } catch (Exception e) {
            throw new ConversionException(e);
        }
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);

        serializePacket(out);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);

        deserializePacket(in);
    }

    @Override
    protected void writeExternal(ObjectOutput out, PlatformLogicalVersion version)
            throws IOException {
        super.writeExternal(out, version);

        serializePacket(out);
    }

    private final void serializePacket(ObjectOutput out) throws IOException {
        final byte flags = buildFlags();
        out.writeByte(flags);
        if (_implClassName != null)
            IOUtils.writeString(out, _implClassName);
    }

    @Override
    protected void readExternal(ObjectInput in, PlatformLogicalVersion version)
            throws IOException, ClassNotFoundException {
        super.readExternal(in, version);

        deserializePacket(in);
    }

    private final void deserializePacket(ObjectInput in) throws IOException,
            ClassNotFoundException {
        final byte flags = in.readByte();

        if ((flags & BitMap.IMPL_CLASSNAME) != 0)
            _implClassName = IOUtils.readString(in);
    }

    /**
     * Bit map used by the serialization to avoid multi-boolean serialization
     */
    private interface BitMap {
        byte IMPL_CLASSNAME = 1 << 1;
    }

    private byte buildFlags() {
        byte flags = 0;

        if (_implClassName != null)
            flags |= BitMap.IMPL_CLASSNAME;

        return flags;
    }
}
