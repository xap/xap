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

/*
 * @(#)EntryPacket.java 1.0   24/10/2000  10:15AM
 */
package com.gigaspaces.internal.transport;

import com.gigaspaces.internal.io.IOArrayException;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.j_spaces.core.EntrySerializationException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

/**
 * This class represents a packet of information transmitted between a J-Space client and its
 * J-Space server (and vice versa). <p/> The information transmitted is composed of 2 types:
 * information on the entry's class, and information on the entry's data (the fields' values). Since
 * the protocol between J-Space client and server are designed for minimum data passing, most of the
 * fields in an EntryPacket are usually null (they are transmitted once for each entry class in a
 * client's life-time). <p/> All the fields in this class are public, in order to increase access
 * and modification speed (no need to call a method).
 *
 * @author Igor Goldenberg
 * @version 1.0
 */
@com.gigaspaces.api.InternalApi
public class EntryPacket extends AbstractEntryPacket {
    private static final long serialVersionUID = 1L;

    protected String _typeName;
    private Object[] _fixedProperties;
    private Map<String, Object> _dynamicProperties;
    private String _uid;
    private int _version;
    private long _timeToLive;
    private boolean _transient;

    private ICustomQuery _customQuery;

    //for read/take multiple - on output: UIDS of entries to read/take,
    //on input- the result UIDs of the operation if ReturnOnlyUids option is used
    // this field is valid only if the classname is null
    private String[] _multipleUIDs;
    protected boolean _returnOnlyUIDs;

    // Deprecated:
    private boolean _noWriteLease;
    private boolean _fifo;


    private byte[] binaryFields;

    /**
     * Default constructor required by {@link java.io.Externalizable}.
     */
    public EntryPacket() {
    }

    public EntryPacket(ITypeDesc typeDesc, EntryType entryType, Object[] fixedProperties, Map<String, Object> dynamicProperties,
                       String uid, int version, long timeToLive, boolean isTransient) {
        this(typeDesc,entryType,fixedProperties,dynamicProperties,uid,version,timeToLive,isTransient,null);
    }


    public EntryPacket(ITypeDesc typeDesc, EntryType entryType, Object[] fixedProperties, Map<String, Object> dynamicProperties,
                       String uid, int version, long timeToLive, boolean isTransient, byte[] binaryFields) {
        super(typeDesc, entryType);
        _typeName = typeDesc.getTypeName();
        _fixedProperties = fixedProperties;
        _dynamicProperties = dynamicProperties;
        _uid = uid;
        _version = version;
        _timeToLive = timeToLive;
        _transient = isTransient;
        _noWriteLease = false;
        _fifo = false;
        this.binaryFields = binaryFields;
    }

    protected EntryPacket(ITypeDesc typeDesc, Object[] values) {
      this(typeDesc,values,null);
    }


    protected EntryPacket(ITypeDesc typeDesc, Object[] values, byte[] binaryFields) {
        super(typeDesc, typeDesc.getObjectType());
        this._typeName = typeDesc.getTypeName();
        this._fixedProperties = values;
        this.binaryFields = binaryFields;
    }

    /**
     * Returns a shallow copy of this <tt>EntryPacket</tt> instance. And also only clone the values
     * array.
     *
     * @return a shallow clone of this <tt>EntryPacket</tt> instance.
     */
    @Override
    public IEntryPacket clone() {
        IEntryPacket packet = super.clone();
        if (_fixedProperties != null)
            packet.setFieldsValues(_fixedProperties.clone());
        if (binaryFields != null){
            packet.setBinaryFields(binaryFields.clone());
        }
        return packet;
    }

    public TransportPacketType getPacketType() {
        return TransportPacketType.ENTRY_PACKET;
    }

    public String getTypeName() {
        return _typeName;
    }

    public String getUID() {
        return _uid;
    }

    public void setUID(String uid) {
        _uid = uid;
    }

    public String[] getMultipleUIDs() {
        return _multipleUIDs;
    }

    public void setMultipleUIDs(String[] multipleUIDs) {
        this._multipleUIDs = multipleUIDs;
    }

    public boolean isReturnOnlyUids() {
        return _returnOnlyUIDs;
    }

    public void setReturnOnlyUIDs(boolean returnOnlyUIDs) {
        this._returnOnlyUIDs = returnOnlyUIDs;
    }

    public Map<String, Object> getDynamicProperties() {
        return _dynamicProperties;
    }

    public void setDynamicProperties(Map<String, Object> dynamicProperties) {
        _dynamicProperties = dynamicProperties;
    }

    public Object[] getFieldValues() {
        if (_fixedProperties == null && getTypeDescriptor() != null &&
                getTypeDescriptor().getClassBinaryStorageAdapter() != null && binaryFields != null) {
            try {
                _fixedProperties = getTypeDescriptor().getClassBinaryStorageAdapter().fromBinary(getTypeDescriptor(), binaryFields);
                binaryFields = null;
            } catch (IOException | ClassNotFoundException e) {
                throw new IllegalStateException("The field values array was not properly set", e);
            }
        }

        return _fixedProperties;
    }

    public void setFieldsValues(Object[] values) {
        this._fixedProperties = values;
    }

    public Object getFieldValue(int index) {
        try {
            return getFieldValues()[index];
        } catch (Exception e) {
            throw new IllegalStateException("The field values array was not properly set", e);
        }
    }

    public void setFieldValue(int index, Object value) {
        try {
            getFieldValues()[index] = value;
        } catch (Exception e) {
            throw new IllegalStateException("The field values array was not properly set", e);
        }
    }

    public boolean isTransient() {
        return _transient;
    }

    public boolean isFifo() {
        return _fifo;
    }

    public int getVersion() {
        return _version;
    }

    public void setVersion(int version) {
        this._version = version;
    }

    public long getTTL() {
        return _timeToLive;
    }

    public void setTTL(long ttl) {
        _timeToLive = ttl;
    }

    public boolean isNoWriteLease() {
        return _noWriteLease;
    }

    public ICustomQuery getCustomQuery() {
        return _customQuery;
    }

    public void setCustomQuery(ICustomQuery customQuery) {
        _customQuery = customQuery;
    }

    @Override
    public boolean isSerializeTypeDesc() {
        return binaryFields != null || super.isSerializeTypeDesc();
    }

    private static final short FLAG_CLASSNAME = 1 << 0;
    private static final short FLAG_UID = 1 << 1;
    private static final short FLAG_VERSION = 1 << 2;
    private static final short FLAG_TIME_TO_LIVE = 1 << 3;
    private static final short FLAG_MULTIPLE_UIDS = 1 << 4;
    private static final short FLAG_FIELDS_VALUES = 1 << 5;
    private static final short FLAG_FIFO = 1 << 6;
    private static final short FLAG_TRANSIENT = 1 << 7;
    private static final short FLAG_NO_WRITE_LEASE = 1 << 8;
    private static final short FLAG_RETURN_ONLY_UIDS = 1 << 9;
    private static final short FLAG_CUSTOM_QUERY = 1 << 10;
    private static final short FLAG_DYNAMIC_PROPERTIES = 1 << 11;
    private static final short FLAG_BINARY_FIELDS= 1 << 12;

    private short buildFlags() {
        short flags = 0;

        if (_typeName != null)
            flags |= FLAG_CLASSNAME;
        if (_uid != null)
            flags |= FLAG_UID;
        if (_version != 0)
            flags |= FLAG_VERSION;
        if (_timeToLive != 0)
            flags |= FLAG_TIME_TO_LIVE;
        if (_multipleUIDs != null)
            flags |= FLAG_MULTIPLE_UIDS;
        if (binaryFields != null) {
            flags |= FLAG_BINARY_FIELDS;
        } else if (_fixedProperties != null){
            flags |= FLAG_FIELDS_VALUES;
        }
        if (_fifo)
            flags |= FLAG_FIFO;
        if (_transient)
            flags |= FLAG_TRANSIENT;
        if (_noWriteLease)
            flags |= FLAG_NO_WRITE_LEASE;
        if (_returnOnlyUIDs)
            flags |= FLAG_RETURN_ONLY_UIDS;
        if (_customQuery != null)
            flags |= FLAG_CUSTOM_QUERY;
        if (_dynamicProperties != null)
            flags |= FLAG_DYNAMIC_PROPERTIES;

        return flags;
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);

        serializePacket(out, PlatformLogicalVersion.getLogicalVersion());
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);

        deserializePacket(in, PlatformLogicalVersion.getLogicalVersion());
    }

    @Override
    protected void writeExternal(ObjectOutput out, PlatformLogicalVersion version)
            throws IOException {
        super.writeExternal(out, version);

        serializePacket(out, version);
    }

    private final void serializePacket(ObjectOutput out,
                                       PlatformLogicalVersion version) {
        try {
            out.writeShort(buildFlags());

            if (_typeName != null)
                IOUtils.writeRepetitiveString(out, _typeName);
            if (_uid != null)
                IOUtils.writeString(out, _uid);
            if (_version != 0)
                out.writeInt(_version);
            if (_timeToLive != 0)
                out.writeLong(_timeToLive);
            if (_multipleUIDs != null)
                IOUtils.writeStringArray(out, _multipleUIDs);
            if(binaryFields != null) {
                IOUtils.writeByteArray(out, binaryFields);
            }else if (_fixedProperties != null) {
                try {
                    IOUtils.writeObjectArrayCompressed(out, _fixedProperties);
                } catch (IOArrayException e) {
                    throw createPropertySerializationException(e, true);
                }
            }
            if (_dynamicProperties != null)
                IOUtils.writeObject(out, _dynamicProperties);
            if (_customQuery != null)
                IOUtils.writeObject(out, _customQuery);
        } catch (EntrySerializationException e) {
            throw e;
        } catch (Exception e) {
            String className = _typeName == null ? "." : _typeName;
            throw new EntrySerializationException("Failed to serialize Entry " + className, e);
        }
    }

    @Override
    protected void readExternal(ObjectInput in, PlatformLogicalVersion version)
            throws IOException, ClassNotFoundException {
        super.readExternal(in, version);

        deserializePacket(in, version);
    }

    private final void deserializePacket(ObjectInput in,
                                         PlatformLogicalVersion version) {
        try {
            final short flags = in.readShort();

            this._fifo = (flags & FLAG_FIFO) != 0;
            this._transient = (flags & FLAG_TRANSIENT) != 0;
            this._noWriteLease = (flags & FLAG_NO_WRITE_LEASE) != 0;
            this._returnOnlyUIDs = (flags & FLAG_RETURN_ONLY_UIDS) != 0;

            if ((flags & FLAG_CLASSNAME) != 0)
                _typeName = IOUtils.readRepetitiveString(in);
            if ((flags & FLAG_UID) != 0)
                _uid = IOUtils.readString(in);
            if ((flags & FLAG_VERSION) != 0)
                _version = in.readInt();
            if ((flags & FLAG_TIME_TO_LIVE) != 0)
                _timeToLive = in.readLong();
            if ((flags & FLAG_MULTIPLE_UIDS) != 0)
                _multipleUIDs = IOUtils.readStringArray(in);
            if((flags & FLAG_BINARY_FIELDS) != 0 ){
                binaryFields = IOUtils.readByteArray(in);
            }
            if ((flags & FLAG_FIELDS_VALUES) != 0) {
                try {
                    _fixedProperties = IOUtils.readObjectArrayCompressed(in);
                } catch (IOArrayException e) {
                    throw createPropertySerializationException(e, false);
                }
            }
            if ((flags & FLAG_DYNAMIC_PROPERTIES) != 0)
                _dynamicProperties = IOUtils.readObject(in);
            if ((flags & FLAG_CUSTOM_QUERY) != 0)
                _customQuery = IOUtils.readObject(in);
        } catch (EntrySerializationException e) {
            throw e;
        } catch (Exception e) {
            String className = _typeName != null ? _typeName : ".";
            throw new EntrySerializationException("Failed to deserialize Entry " + className, e);
        }
    }

    private EntrySerializationException createPropertySerializationException(IOArrayException e, boolean isSerialize) {
        final int index = e.getIndex();
        String message = "Failed to " + (isSerialize ? "serialize" : "deserialize") +
                " property #" + index;

        ITypeDesc typeDesc = getTypeDescriptor();
        if (typeDesc != null) {
            int numOfProperties = typeDesc.getNumOfFixedProperties();
            if (index < 0 || index >= numOfProperties)
                message += " (index out of bound: number of properties=" + numOfProperties + ")";
            else {
                PropertyInfo property = typeDesc.getFixedProperty(index);
                message += " (Name=[" + property.getName() + "], Type=[" + property.getTypeName() + "])";
            }
        }

        message += " in entry of type [" + this.getTypeName() + "].";
        if (isSerialize)
            message += " Value=[" + String.valueOf(getFieldValue(index)) + "]";

        return new EntrySerializationException(message, e.getCause());
    }

    /**
     * true if the entry packet has an array of fixed properties
     */
    @Override
    public boolean hasFixedPropertiesArray() {
        return true;
    }

    @Override
    public void setBinaryFields(byte[] binaryFields) {
        this.binaryFields = binaryFields;
    }

    @Override
    public boolean allNullFieldValues() {
        return _fixedProperties == null && binaryFields == null;
    }
}
