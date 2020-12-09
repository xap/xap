package com.gigaspaces.internal.server.storage;

import com.j_spaces.core.server.transaction.EntryXtnInfo;

import java.util.Arrays;
import java.util.Map;

public class MutableHybridViewEntryData extends HybridViewEntryData implements ITransactionalEntryData {

    public void view(ITransactionalEntryData entryData, HybridViewEntryData viewEntryData) {
        this.entry = entryData;
        this.dynamicProperties = entryData.getDynamicProperties();
        this.hybridBinaryData = viewEntryData.getHybridBinaryData().clone();
    }

    public void view(ITransactionalEntryData entryData) {
        this.entry = entryData;
        this.dynamicProperties = entryData.getDynamicProperties();
        HybridBinaryEntryData hybridBinaryEntryData = (HybridBinaryEntryData) entryData;
        this.hybridBinaryData = new HybridPayload(getEntryTypeDesc().getTypeDesc(),
                Arrays.copyOf(hybridBinaryEntryData.getNonSerializedFields(), hybridBinaryEntryData.getNonSerializedFields().length),
                Arrays.copyOf(hybridBinaryEntryData.getSerializedFields(), hybridBinaryEntryData.getSerializedFields().length));
    }

    @Override
    public void setFixedPropertyValue(int index, Object value) {
        hybridBinaryData.setFixedProperty(entry.getEntryTypeDesc().getTypeDesc(), index, value);
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
        return hybridBinaryData.isDeserialized();
    }
}
