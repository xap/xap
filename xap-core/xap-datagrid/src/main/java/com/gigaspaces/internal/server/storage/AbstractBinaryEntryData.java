package com.gigaspaces.internal.server.storage;

import com.gigaspaces.document.DocumentProperties;
import com.gigaspaces.internal.metadata.EntryTypeDesc;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.j_spaces.core.server.transaction.EntryXtnInfo;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

public abstract class AbstractBinaryEntryData implements ITransactionalEntryData {
    protected final EntryTypeDesc _entryTypeDesc;
    protected final int _versionID;
    protected final long _expirationTime;
    protected final EntryXtnInfo _entryTxnInfo;
    protected Map<String, Object> _dynamicProperties;

    public AbstractBinaryEntryData(EntryTypeDesc entryTypeDesc, int version, long expirationTime, EntryXtnInfo entryXtnInfo, Map<String, Object> dynamicProperties) {
        this._entryTypeDesc = entryTypeDesc;
        this._versionID = version;
        this._expirationTime = expirationTime;
        this._entryTxnInfo = entryXtnInfo;
        this._dynamicProperties = dynamicProperties;
    }

    protected static byte[] serializeFields(Object[] fieldsValues, ITypeDesc typeDesc) {
        try {
            return typeDesc.getClassBinaryStorageAdapter().toBinary(typeDesc, fieldsValues);
        } catch (IOException e) {
            throw new UncheckedIOException("com.gigaspaces.internal.server.storage.BinaryEntryData.serializeFields failed", e);
        }
    }

    @Override
    public EntryTypeDesc getEntryTypeDesc() {
        return _entryTypeDesc;
    }

    @Override
    public int getVersion() {
        return _versionID;
    }

    @Override
    public long getExpirationTime() {
        return _expirationTime;
    }

    @Override
    public EntryXtnInfo getEntryXtnInfo() {
        return _entryTxnInfo;
    }

    @Override
    public abstract ITransactionalEntryData createCopy(int newVersion, long newExpiration, EntryXtnInfo newEntryXtnInfo, boolean shallowCloneData);

    @Override
    public abstract ITransactionalEntryData createCopy(IEntryData newEntryData, long newExpirationTime);

    @Override
    public EntryDataType getEntryDataType() {
        return EntryDataType.FLAT;
    }

    @Override
    public abstract Object getFixedPropertyValue(int index);

    @Override
    public abstract void setFixedPropertyValue(int index, Object value);

    @Override
    public abstract Object[] getFixedPropertiesValues();

    @Override
    public Map<String, Object> getDynamicProperties() {
        return _dynamicProperties;
    }

    @Override
    public void setDynamicProperties(Map<String, Object> dynamicProperties) {
        _dynamicProperties = dynamicProperties;
    }

    @Override
    public void setDynamicPropertyValue(String propertyName, Object value) {
        if (!_entryTypeDesc.getTypeDesc().supportsDynamicProperties())
            throw new UnsupportedOperationException(_entryTypeDesc.getTypeDesc().getTypeName() + " does not support dynamic properties");

        if (_dynamicProperties == null)
            _dynamicProperties = new DocumentProperties();

        _dynamicProperties.put(propertyName, value);
    }

    @Override
    public abstract void setFixedPropertyValues(Object[] values);

    public abstract byte[] getSerializedFields();

    public abstract void setSerializedFields(byte[] serializedFields);
}
