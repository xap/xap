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

package com.gigaspaces.internal.transport;

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.StorageTypeDeserialization;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.ITypeIntrospector;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.serialization.AbstractExternalizable;
import com.gigaspaces.internal.server.space.SpaceUidFactory;
import com.gigaspaces.internal.utils.Textualizable;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.metadata.StorageType;
import com.j_spaces.core.OperationID;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

/**
 * @author kimchy
 */
public abstract class AbstractEntryPacket extends AbstractExternalizable implements IEntryPacket, Textualizable {
    private static final long serialVersionUID = 1L;

    protected ITypeDesc _typeDesc;
    protected int _typeDescChecksum;
    private OperationID _operationID;
    private boolean _serializeTypeDesc;
    protected EntryType _entryType;
    protected transient byte _entryTypeCode;
    /**
     * Previous entry version, 0 if no previous version was provided.
     */
    private int _previousVersion;


    /**
     * Default constructor required by {@link java.io.Externalizable}.
     */
    protected AbstractEntryPacket() {
    }

    protected AbstractEntryPacket(ITypeDesc typeDesc, EntryType entryType) {
        this._typeDesc = typeDesc;
        this._typeDescChecksum = typeDesc != null ? typeDesc.getChecksum() : 0;
        this._entryType = entryType;
    }

    @Override
    public IEntryPacket clone() {
        try {
            return (AbstractEntryPacket) super.clone();
        } catch (CloneNotSupportedException e) {
            // should not happen
            throw new InternalError();
        }
    }

    @Override
    public String toString() {
        return Textualizer.toString(this);
    }

    @Override
    public void toText(Textualizer textualizer) {
        textualizer.append("typeName", getTypeName());
        textualizer.append("uid", getUID());
        textualizer.append("version", getVersion());
        textualizer.append("operationId", getOperationID());
    }

    @Override
    public int hashCode() {
        return (getUID() != null) ? getUID().hashCode() : 10;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)    //check ref. first
            return true;

        if (!(obj instanceof IEntryPacket))
            return false;

        IEntryPacket other = (IEntryPacket) obj;

        //match uids
        if (this.getUID() == other.getUID())
            return true;

        if (this.getUID() == null || other.getUID() == null)
            return false;

        return this.getUID().equals(other.getUID());
    }

    public EntryType getEntryType() {
        return _entryType;
    }

    public String getExternalEntryImplClassName() {
        return null;
    }

    public OperationID getOperationID() {
        return _operationID;
    }

    public void setOperationID(OperationID operationID) {
        this._operationID = operationID;
    }

    public ITypeDesc getTypeDescriptor() {
        return _typeDesc;
    }

    public void setTypeDesc(ITypeDesc typeDesc, boolean serializeTypeDesc) {
        _typeDesc = typeDesc;
        _typeDescChecksum = _typeDesc != null ? _typeDesc.getChecksum() : 0;
        _serializeTypeDesc = serializeTypeDesc;
    }

    public int getTypeDescChecksum() {
        return _typeDescChecksum;
    }

    public boolean supportsTypeDescChecksum() {
        return true;
    }

    public boolean isSerializeTypeDesc() {
        return _serializeTypeDesc;
    }

    public void setSerializeTypeDesc(boolean serializeTypeDesc) {
        _serializeTypeDesc = serializeTypeDesc;
    }

    public String getCodebase() {
        return _typeDesc != null ? _typeDesc.getCodeBase() : null;
    }

    public Object getPropertyValue(String name) {
        final ITypeDesc typeDesc = getTypeDescriptor();
        int pos = typeDesc.getFixedPropertyPosition(name);

        if (pos != -1)
            return getFieldValue(pos);

        if (!typeDesc.supportsDynamicProperties())
            throw new IllegalArgumentException("Failed to get value of property '" + name + "' in type '" + typeDesc.getTypeName() +
                    "' - it does not exist and the type does not support dynamic properties.");

        final Map<String, Object> dynamicProperties = getDynamicProperties();
        return dynamicProperties != null ? dynamicProperties.get(name) : null;
    }

    public void setPropertyValue(String name, Object value) {
        final ITypeDesc typeDesc = getTypeDescriptor();
        int pos = typeDesc.getFixedPropertyPosition(name);

        if (pos != -1)
            setFieldValue(pos, value);
        else {
            if (getDynamicProperties() == null)
                throw new IllegalArgumentException("Failed to set value of property '" + name + "' in type '" + typeDesc.getTypeName() +
                        "' - it does not exist and the type does not support dynamic properties.");

            getDynamicProperties().put(name, value);
        }
    }

    public Object getID() {
        final ITypeDesc typeDesc = getTypeDescriptor();
        final int identifierPropertyId = typeDesc.getIdentifierPropertyId();
        Object value = null;
        if (identifierPropertyId != -1)
            value = getFieldValue(identifierPropertyId);

        if (value == null)
            value = getUID();

        return value;
    }

    @Override
    public int getPreviousVersion() {
        return _previousVersion;
    }

    @Override
    public void setPreviousVersion(int previousVersion) {
        _previousVersion = previousVersion;
    }

    @Override
    public boolean hasPreviousVersion() {
        return _previousVersion != 0;
    }

    public Object getRoutingFieldValue() {
        if (_typeDesc.isAutoGenerateRouting())
            return SpaceUidFactory.extractPartitionId(getUID());

        int routingPropertyId = _typeDesc.getRoutingPropertyId();
        return routingPropertyId == -1 ? null : getFieldValue(routingPropertyId);
    }

    public Object toObject(QueryResultTypeInternal resultType) {
        return toObject(resultType, StorageTypeDeserialization.EAGER);
    }

    public Object toObject(QueryResultTypeInternal resultType, StorageTypeDeserialization storageTypeDeserialization) {
        if (resultType.isPbs())
            return this;
        EntryType entryType = resultType.getEntryType();
        if (entryType == null && this.getTypeDescriptor() != null)
            entryType = this.getTypeDescriptor().getObjectType();
        return toObject(entryType, storageTypeDeserialization);
    }

    /**
     * true if the entry packet has an array of fixed properties
     */
    public boolean hasFixedPropertiesArray() {
        return false;
    }

    public Object toObject(EntryType entryType) {
        return toObject(entryType, StorageTypeDeserialization.EAGER);
    }

    public Object toObject(EntryType entryType, StorageTypeDeserialization storageTypeDeserialization) {
        ITypeIntrospector<?> introspector = _typeDesc.getIntrospector(entryType);
        return introspector.toObject(this, storageTypeDeserialization);
    }

    private static final short FLAG_OPERATION_ID = 1 << 0;
    private static final short FLAG_TYPE_DESC = 1 << 1;
    private static final short FLAG_TYPE_DESC_CHECKSUM = 1 << 2;
    private static final short FLAG_REQUEST_TYPE = 1 << 3;
    private static final short FLAG_PREVIOUS_VERSION = 1 << 4;

    @Override
    protected void writeExternalImpl(ObjectOutput out)
            throws IOException {
        super.writeExternalImpl(out);
        writeExternal(out, LRMIInvocationContext.getEndpointLogicalVersion());
    }

    @Override
    protected void readExternalImpl(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternalImpl(in);
        readExternal(in, LRMIInvocationContext.getEndpointLogicalVersion());
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        serialize(out);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException, ClassNotFoundException {
        deserialize(in);
    }

    protected void writeExternal(ObjectOutput out, PlatformLogicalVersion version)
            throws IOException {
        serialize(out);
    }

    protected void readExternal(ObjectInput in, PlatformLogicalVersion version)
            throws IOException, ClassNotFoundException {
        deserialize(in);
    }

    private final void serialize(ObjectOutput out)
            throws IOException {
        // Calculate flags:
        final short flags = buildFlags();
        // Write flags:
        out.writeShort(flags);

        // Write remaining fields according to flags:
        if (_operationID != null)
            IOUtils.writeObject(out, _operationID);
        if (isSerializeTypeDesc() && _typeDesc != null)
            IOUtils.writeObject(out, _typeDesc);
        if (_typeDescChecksum != 0)
            out.writeInt(_typeDescChecksum);
        if (_entryType != null)
            out.writeByte(_entryType.getTypeCode());
        if (_previousVersion != 0)
            out.writeInt(_previousVersion);
    }

    private void deserialize(ObjectInput in) throws IOException, ClassNotFoundException {
        // Read flags:
        final short flags = in.readShort();

        // Set boolean fields according to flags:
        _serializeTypeDesc = (flags & FLAG_TYPE_DESC) != 0;

        // Read remaining fields according to flags:
        if ((flags & FLAG_OPERATION_ID) != 0)
            _operationID = IOUtils.readObject(in);
        if (_serializeTypeDesc)
            _typeDesc = IOUtils.readObject(in);
        if ((flags & FLAG_TYPE_DESC_CHECKSUM) != 0)
            _typeDescChecksum = in.readInt();
        if ((flags & FLAG_REQUEST_TYPE) != 0) {
            _entryTypeCode = in.readByte();
            _entryType = EntryType.fromByte(_entryTypeCode);
        }
        if ((flags & FLAG_PREVIOUS_VERSION) != 0)
            _previousVersion = in.readInt();
    }

    private short buildFlags() {
        short flags = 0;

        if (_operationID != null)
            flags |= FLAG_OPERATION_ID;
        if (isSerializeTypeDesc() && _typeDesc != null)
            flags |= FLAG_TYPE_DESC;
        if (_entryType != null)
            flags |= FLAG_REQUEST_TYPE;
        if (_typeDescChecksum != 0)
            flags |= FLAG_TYPE_DESC_CHECKSUM;
        if (_previousVersion != 0)
            flags |= FLAG_PREVIOUS_VERSION;

        return flags;
    }

    protected void validateStorageType() {
        ITypeDesc typeDesc = this._typeDesc;
        if (typeDesc.isAllPropertiesObjectStorageType())
            return;
        PropertyInfo[] properties = typeDesc.getProperties();
        for (int i = 0; i < properties.length; i++) {
            PropertyInfo property = properties[i];
            StorageType storageType = property.getStorageType();
            if (storageType != StorageType.OBJECT || !property.supportsEqualsMatching()) {
                Object fieldValue = getFieldValue(i);
                if (fieldValue != null) {
                    String errorMessage = storageType != StorageType.OBJECT
                            ? "storage type other than " + StorageType.OBJECT + " storage type (current storage type is " + storageType + ")."
                            : "storage adapter does not support matching [" + property.getStorageAdapterName() + "]";
                    throw new IllegalArgumentException("[" + property.getName() + "] Cannot match property - " + errorMessage);
                }
            }
        }
    }

    @Override
    public boolean isExternalizableEntryPacket() {
        return false;
    }
}
