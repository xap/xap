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
