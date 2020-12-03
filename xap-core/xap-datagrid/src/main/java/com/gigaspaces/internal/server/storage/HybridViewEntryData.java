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

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;

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
public class HybridViewEntryData extends AbstractViewEntryData {
    HybridBinaryData hybridBinaryData;

    public void view(IEntryData entryData) {
        this.entry = entryData;
        this.dynamicProperties = entryData.getDynamicProperties();
        this.hybridBinaryData = new HybridBinaryData(getEntryTypeDesc().getTypeDesc(),
                ((HybridBinaryEntryData) entryData).getNonSerializedFields(),
                ((HybridBinaryEntryData) entryData).getSerializedFields());
    }

    public void view(IEntryData entryData, Object[] fieldValues) {
        this.entry = entryData;
        ITypeDesc typeDesc = getEntryTypeDesc().getTypeDesc();
        Object[] nonSerializedProperties = new Object[typeDesc.getNonSerializedProperties().length];
        Object[] serializedProperties = new Object[typeDesc.getSerializedProperties().length];
        int nonSerializedFieldsIndex = 0;
        int serializedFieldsIndex = 0;
        for (PropertyInfo property : typeDesc.getProperties()) {
            if (property.getStorageType() != null && property.isBinarySpaceProperty()) {
                serializedProperties[serializedFieldsIndex] = fieldValues[typeDesc.getFixedPropertyPosition(property.getName())];
                serializedFieldsIndex++;
            } else {
                nonSerializedProperties[nonSerializedFieldsIndex] = fieldValues[typeDesc.getFixedPropertyPosition(property.getName())];
                nonSerializedFieldsIndex++;
            }
        }
        this.hybridBinaryData = new HybridBinaryData(serializedProperties, nonSerializedProperties,
                ((HybridBinaryEntryData) entryData).getSerializedFields() ,true);
        this.dynamicProperties = entryData.getDynamicProperties();
    }

    public HybridBinaryData getHybridBinaryData() {
        return hybridBinaryData;
    }

    @Override
    public Object[] getFixedPropertiesValues() {
        return this.hybridBinaryData.getFixedProperties(getEntryTypeDesc().getTypeDesc());
    }

    @Override
    public Object getFixedPropertyValue(int position) {
        return this.hybridBinaryData.getFixedProperty(getEntryTypeDesc().getTypeDesc(), position);
    }
}
