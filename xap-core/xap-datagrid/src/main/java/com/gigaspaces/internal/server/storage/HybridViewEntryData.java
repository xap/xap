package com.gigaspaces.internal.server.storage;

import com.gigaspaces.internal.metadata.EntryTypeDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Helper class for optimizing scanning entry data.
 * <p>
 * If the data (fixed properties) in an entry data is stored in a packed format, retrieving it more than once is inefficient.
 * This class provides a mutable view of an entry data - when a viewed entry is set it prefetches the data and caches it,
 * using it to server get requests (set requests are illegal). This helps reducing footprint on the actual entry data in
 * cache manager and gc activity when querying the space.
 *
 * @author Yechiel, Yael, Niv
 * @since 15.8
 */
public class HybridViewEntryData implements IEntryData {
    private static Logger logger = LoggerFactory.getLogger(HybridViewEntryData.class);
    protected IEntryData entry;
    protected Map<String, Object> dynamicProperties;
    HybridPayload hybridBinaryData;

    public void view(IEntryData entryData) {
        this.entry = entryData;
        this.dynamicProperties = entryData.getDynamicProperties();
        this.hybridBinaryData = new HybridPayload(getEntryTypeDesc().getTypeDesc(),
                ((HybridBinaryEntryData) entryData).getNonSerializedFields(),
                ((HybridBinaryEntryData) entryData).getSerializedFields());
    }

    public void view(IEntryData entryData, HybridPayload hybridPayload) {
        this.hybridBinaryData = hybridPayload;
        this.dynamicProperties = entryData.getDynamicProperties();
    }

    public HybridPayload getHybridBinaryData() {
        return hybridBinaryData;
    }

    @Override
    public EntryDataType getEntryDataType() {
        return entry.getEntryDataType();
    }

    @Override
    public Object[] getFixedPropertiesValues() {
        return this.hybridBinaryData.getFixedProperties(getEntryTypeDesc().getTypeDesc());
    }

    @Override
    public Object getFixedPropertyValue(int position) {
        return this.hybridBinaryData.getFixedProperty(getEntryTypeDesc().getTypeDesc(), position);
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
