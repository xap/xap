package com.gigaspaces.internal.server.storage;

import com.gigaspaces.internal.metadata.EntryTypeDesc;

import java.util.Map;

/**
 * Helper class for optimizing scanning entry data.
 *
 * If the data (fixed properties) in an entry data is stored in a packed format, retrieving it more than once is inefficient.
 * This class provides a mutable view of an entry data - when a viewed entry is set it prefetches the data and caches it,
 * using it to server get requests (set requests are illegal). This helps reducing footprint on the actual entry data in
 * cache manager and gc activity when querying the space.
 *
 * @author Yechiel, Yael, Niv
 * @since 15.8
 */
public class ViewEntryData implements IEntryData {

    private IEntryData entry;
    private Object[] fixedProperties;
    private Map<String, Object> dynamicProperties;

    public void view(IEntryData entry) {
        this.entry = entry;
        this.fixedProperties = entry.getFixedPropertiesValues();
        this.dynamicProperties = entry.getDynamicProperties();
    }

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

    @Override
    public Object[] getFixedPropertiesValues() {
        return fixedProperties;
    }

    @Override
    public Object getFixedPropertyValue(int position) {
        return fixedProperties[position];
    }

    @Override
    public Map<String, Object> getDynamicProperties() {
        return dynamicProperties;
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
