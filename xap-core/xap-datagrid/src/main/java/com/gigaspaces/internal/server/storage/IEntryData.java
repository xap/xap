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

import com.gigaspaces.internal.lease.LeaseUtils;
import com.gigaspaces.internal.metadata.*;
import com.gigaspaces.internal.query.valuegetter.SpaceEntryPathGetter;
import com.gigaspaces.server.ServerEntry;

import java.util.Map;

/**
 * Abstraction for getting and setting fields values on an entry.
 *
 * @author Niv Ingberg
 * @since 7.0
 */
public interface IEntryData extends ServerEntry {
    EntryDataType getEntryDataType();

    EntryTypeDesc getEntryTypeDesc();

    @Override
    default ITypeDesc getSpaceTypeDescriptor() {
        return getEntryTypeDesc().getTypeDesc();
    }

    default int getNumOfFixedProperties() {
        return getSpaceTypeDescriptor().getNumOfFixedProperties();
    }

    void setFixedPropertyValue(int index, Object value);

    void setFixedPropertyValues(Object[] values);

    Object[] getFixedPropertiesValues();

    Map<String, Object> getDynamicProperties();

    void setDynamicProperties(Map<String, Object> dynamicProperties);

    default long getTimeToLive(boolean useDummyIfRelevant) {
        return LeaseUtils.getTimeToLive(getExpirationTime(), useDummyIfRelevant);
    }

    @Override
    default Object getPropertyValue(String name) {
        ITypeDesc typeDesc = getSpaceTypeDescriptor();
        int pos = typeDesc.getFixedPropertyPosition(name);
        if (pos != -1)
            return getFixedPropertyValue(pos);

        if (typeDesc.supportsDynamicProperties()) {
            Map<String, Object> dynamicProperties = getDynamicProperties();
            return dynamicProperties != null ? dynamicProperties.get(name) : null;
        }

        throw new IllegalArgumentException("Unknown property name '" + name + "' in type " + getSpaceTypeDescriptor().getTypeName());
    }

    @Override
    default Object getPathValue(String path) {
        if (!path.contains("."))
            return getPropertyValue(path);
        return new SpaceEntryPathGetter(path).getValue(this);
    }
}
