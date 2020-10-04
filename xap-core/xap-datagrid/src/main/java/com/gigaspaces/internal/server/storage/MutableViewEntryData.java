package com.gigaspaces.internal.server.storage;

import com.j_spaces.core.server.transaction.EntryXtnInfo;

import java.util.Arrays;
import java.util.Map;

public class MutableViewEntryData extends ViewEntryData implements ITransactionalEntryData {


    public void view(ITransactionalEntryData entryData) {
        super.view(entryData);
    }

    public void view(ITransactionalEntryData entryData, Object[] fieldValues) {
        super.view(entryData, Arrays.copyOf(fieldValues, fieldValues.length));
    }

    @Override
    public void setFixedPropertyValue(int index, Object value) {
        getFixedPropertiesValues()[index] = value;
    }

    @Override
    public void setFixedPropertyValues(Object[] values) {
        this.fixedProperties = values;
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
        return ((ITransactionalEntryData) getEntry()).createCopy(newVersion,newExpiration,newEntryXtnInfo,shallowCloneData);
    }

    @Override
    public ITransactionalEntryData createCopy(IEntryData newEntryData, long newExpirationTime) {
        return ((ITransactionalEntryData) getEntry()).createCopy(newEntryData,newExpirationTime);
    }

    @Override
    public void setDynamicPropertyValue(String propertyName, Object value) {
        ((ITransactionalEntryData) getEntry()).setDynamicPropertyValue(propertyName,value);
    }
}
