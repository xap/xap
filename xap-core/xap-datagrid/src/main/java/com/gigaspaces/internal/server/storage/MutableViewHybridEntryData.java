package com.gigaspaces.internal.server.storage;

import com.j_spaces.core.server.transaction.EntryXtnInfo;

import java.util.Arrays;
import java.util.Map;

public class MutableViewHybridEntryData extends ViewPropertiesEntryData implements ITransactionalEntryData {

    public void view(ITransactionalEntryData entryData, ViewPropertiesEntryData viewEntryData) {
        this.entry = entryData;
        this.dynamicProperties = entryData.getDynamicProperties();
        this.propertiesHolder = viewEntryData.getPropertiesHolder().clone();
    }

    public void view(ITransactionalEntryData entryData) {
        this.entry = entryData;
        this.dynamicProperties = entryData.getDynamicProperties();
        HybridEntryData hybridBinaryEntryData = (HybridEntryData) entryData;
        this.propertiesHolder = new HybridPropertiesHolder(getEntryTypeDesc().getTypeDesc(),
                Arrays.copyOf(hybridBinaryEntryData.getNonSerializedProperties(), hybridBinaryEntryData.getNonSerializedProperties().length),
                Arrays.copyOf(hybridBinaryEntryData.getPackedSerializedProperties(), hybridBinaryEntryData.getPackedSerializedProperties().length));
    }

    @Override
    public void setFixedPropertyValue(int index, Object value) {
        propertiesHolder.setFixedProperty(entry.getEntryTypeDesc().getTypeDesc(), index, value);
    }

    @Override
    public void setFixedPropertyValues(Object[] values) {
        throw new IllegalStateException("com.gigaspaces.internal.server.storage.MutableHybridViewEntryData.setFixedPropertyValues(Object[] values) should not be called");
    }

    @Override
    public void setDynamicProperties(Map<String, Object> dynamicProperties) {
        getEntry().setDynamicProperties(dynamicProperties);
    }

    @Override
    public EntryXtnInfo getEntryXtnInfo() {
        return ((ITransactionalEntryData) getEntry()).getEntryXtnInfo();
    }

    @Override
    public ITransactionalEntryData createCopy(int newVersion, long newExpiration, EntryXtnInfo newEntryXtnInfo, boolean shallowCloneData) {
        return ((ITransactionalEntryData) getEntry()).createCopy(newVersion, newExpiration, newEntryXtnInfo, shallowCloneData);
    }

    @Override
    public ITransactionalEntryData createCopy(IEntryData newEntryData, long newExpirationTime) {
        return ((ITransactionalEntryData) getEntry()).createCopy(newEntryData, newExpirationTime);
    }

    @Override
    public void setDynamicPropertyValue(String propertyName, Object value) {
        ((ITransactionalEntryData) getEntry()).setDynamicPropertyValue(propertyName, value);
    }

    public boolean isDeserialized() {
        return propertiesHolder.isDirty();
    }
}
