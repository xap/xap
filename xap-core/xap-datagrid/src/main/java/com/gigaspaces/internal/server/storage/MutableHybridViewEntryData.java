package com.gigaspaces.internal.server.storage;

import com.j_spaces.core.server.transaction.EntryXtnInfo;

import java.util.Map;

public class MutableHybridViewEntryData extends HybridViewEntryData implements ITransactionalEntryData {

    private boolean deserialized;

    public void view(ITransactionalEntryData entryData, HybridViewEntryData viewEntryData) {
        this.entry = entryData;
        this.dynamicProperties = entryData.getDynamicProperties();
        this.hybridBinaryData = viewEntryData.getHybridBinaryData().clone();
    }

    public void view(ITransactionalEntryData entryData) {
        this.entry = entryData;
        this.dynamicProperties = entryData.getDynamicProperties();
        this.hybridBinaryData = entryData instanceof HybridBinaryEntryData ?
                new HybridBinaryData(getEntryTypeDesc().getTypeDesc(),
                        ((HybridBinaryEntryData) entryData).getNonSerializedFields(),
                        ((HybridBinaryEntryData) entryData).getSerializedFields())
                : ((HybridViewEntryData) entryData).getHybridBinaryData().clone();
    }

    @Override
    public void setFixedPropertyValue(int index, Object value) {
        hybridBinaryData.setFixedProperty(entry.getEntryTypeDesc().getTypeDesc(), index, value);
        if(entry.getEntryTypeDesc().getTypeDesc().getFixedProperty(index).isBinarySpaceProperty()){
            this.deserialized = true;
        }
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
        return this.deserialized;
    }
}
