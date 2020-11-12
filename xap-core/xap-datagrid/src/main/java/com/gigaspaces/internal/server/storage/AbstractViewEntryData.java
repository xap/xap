package com.gigaspaces.internal.server.storage;

import com.gigaspaces.internal.metadata.EntryTypeDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class AbstractViewEntryData implements IEntryData {
    private static Logger logger = LoggerFactory.getLogger(AbstractViewEntryData.class);
    protected IEntryData entry;
    protected Map<String, Object> dynamicProperties;

    @Override
    public EntryDataType getEntryDataType() {
        return entry.getEntryDataType();
    }

    @Override
    public EntryTypeDesc getEntryTypeDesc() {
        return entry.getEntryTypeDesc();
    }

    @Override
    public int getVersion() {
        return entry.getVersion();
    }

    @Override
    public long getExpirationTime() {
        return entry.getExpirationTime();
    }

    public boolean isViewOf(IEntryData entryData) {
        return this.entry == entryData;
    }

    public abstract void view(IEntryData entryData);

    public abstract void view(IEntryData entryData, Object[] fieldValues);

    @Override
    public abstract Object[] getFixedPropertiesValues();

    @Override
    public abstract Object getFixedPropertyValue(int position);

    @Override
    public Map<String, Object> getDynamicProperties() {
        return dynamicProperties;
    }

    public IEntryData getEntry() {
        return entry;
    }

    @Override
    public void setFixedPropertyValue(int index, Object value) {
        throw new IllegalStateException("Data cannot be modified on entry data view");
    }

    @Override
    public void setFixedPropertyValues(Object[] values) {
        throw new IllegalStateException("Data cannot be modified on entry data view");
    }

    @Override
    public void setDynamicProperties(Map<String, Object> dynamicProperties) {
        throw new IllegalStateException("Data cannot be modified on entry data view");
    }

}
