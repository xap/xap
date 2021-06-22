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
public class ViewPropertiesEntryData implements IEntryData {
    private static Logger logger = LoggerFactory.getLogger(ViewPropertiesEntryData.class);
    protected IEntryData entry;
    protected Map<String, Object> dynamicProperties;
    HybridPropertiesHolder propertiesHolder;

    public void view(IEntryData entryData) {
        this.entry = entryData;
        this.dynamicProperties = entryData.getDynamicProperties();
        this.propertiesHolder = new HybridPropertiesHolder(getEntryTypeDesc().getTypeDesc(),
                ((HybridEntryData) entryData).getNonSerializedProperties(),
                ((HybridEntryData) entryData).getPackedSerializedProperties());
    }

    public void view(IEntryData entryData, HybridPropertiesHolder holder) {
        this.propertiesHolder = holder;
        this.dynamicProperties = entryData.getDynamicProperties();
    }

    public HybridPropertiesHolder getPropertiesHolder() {
        return propertiesHolder;
    }

    @Override
    public EntryDataType getEntryDataType() {
        return entry.getEntryDataType();
    }

    @Override
    public Object[] getFixedPropertiesValues() {
        return this.propertiesHolder.getFixedProperties(getEntryTypeDesc().getTypeDesc());
    }

    @Override
    public Object getFixedPropertyValue(int position) {
        return this.propertiesHolder.getFixedProperty(getEntryTypeDesc().getTypeDesc(), position);
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

    @Override
    public boolean isView() {
        return true;
    }
}
