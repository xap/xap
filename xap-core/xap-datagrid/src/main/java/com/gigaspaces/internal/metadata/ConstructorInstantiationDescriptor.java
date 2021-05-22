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

package com.gigaspaces.internal.metadata;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.ObjectUtils;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.kernel.ClassLoaderHelper;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Constructor;

/**
 * @author Dan Kilman
 * @since 9.6
 */
@com.gigaspaces.api.InternalApi
public class ConstructorInstantiationDescriptor implements SmartExternalizable {
    private static final long serialVersionUID = 1L;

    private static final int NO_MATCH = -1;

    // not serialized         
    private transient Constructor<?> constructor;

    private Class<?>[] constructorParameterTypes;
    private String[] constructorParameterNames;
    private boolean[] excludedIndexes;
    private int[] spacePropertyToConstructorIndex;

    private int idPropertyConstructorIndex = NO_MATCH;
    private int dynamicPropertiesConstructorIndex = NO_MATCH;
    private int versionConstructorIndex = NO_MATCH;
    private int persistConstructorIndex = NO_MATCH;
    private int leaseConstructorIndex = NO_MATCH;

    /* for Externalizable */
    public ConstructorInstantiationDescriptor() {
    }

    public Class<?>[] getConstructorParameterTypes() {
        return constructorParameterTypes;
    }

    public void setConstructorParameterTypes(Class<?>[] constructorParameterTypes) {
        this.constructorParameterTypes = constructorParameterTypes;
    }

    public String[] getConstructorParameterNames() {
        return constructorParameterNames;
    }

    public void setConstructorParameterNames(String[] constructorParameterNames) {
        this.constructorParameterNames = constructorParameterNames;
    }

    public boolean[] getExcludedIndexes() {
        return excludedIndexes;
    }

    public void setExcludedIndexes(boolean[] excludedIndexes) {
        this.excludedIndexes = excludedIndexes;
    }

    public int getSpacePropertyToConstructorIndex(int spacePropertyIndex) {
        if (spacePropertyToConstructorIndex == null ||
                spacePropertyIndex < 0 || spacePropertyIndex >= spacePropertyToConstructorIndex.length)
            return NO_MATCH;

        return spacePropertyToConstructorIndex[spacePropertyIndex];
    }

    public void setSpacePropertyToConstructorIndex(
            int[] spacePropertyToConstructorIndex) {
        this.spacePropertyToConstructorIndex = spacePropertyToConstructorIndex;
    }

    public int getIdPropertyConstructorIndex() {
        return idPropertyConstructorIndex;
    }

    public void setIdPropertyConstructorIndex(int idPropertyConstructorIndex) {
        this.idPropertyConstructorIndex = idPropertyConstructorIndex;
    }

    public int getDynamicPropertiesConstructorIndex() {
        return dynamicPropertiesConstructorIndex;
    }

    public void setDynamicPropertiesConstructorIndex(
            int dynamicPropertiesConstructorIndex) {
        this.dynamicPropertiesConstructorIndex = dynamicPropertiesConstructorIndex;
    }

    public int getVersionConstructorIndex() {
        return versionConstructorIndex;
    }

    public void setVersionConstructorIndex(int versionConstructorIndex) {
        this.versionConstructorIndex = versionConstructorIndex;
    }

    public int getPersistConstructorIndex() {
        return persistConstructorIndex;
    }

    public void setPersistConstructorIndex(int persistConstructorIndex) {
        this.persistConstructorIndex = persistConstructorIndex;
    }

    public int getLeaseConstructorIndex() {
        return leaseConstructorIndex;
    }

    public void setLeaseConstructorIndex(int leaseConstructorIndex) {
        this.leaseConstructorIndex = leaseConstructorIndex;
    }

    public Constructor<?> getConstructor() {
        return constructor;
    }

    public void setConstructor(Constructor<?> constructor) {
        this.constructor = constructor;
    }

    public int getNumberOfParameters() {
        return constructorParameterNames == null ? 0 : constructorParameterNames.length;
    }

    public boolean isIndexExcluded(int index) {
        if (excludedIndexes == null || index < 0 || index >= excludedIndexes.length)
            return false;

        return excludedIndexes[index];
    }

    public int indexOfProperty(String propertyName) {
        if (propertyName == null)
            return NO_MATCH;

        for (int i = 0; i < constructorParameterNames.length; i++) {
            if (propertyName.equals(constructorParameterNames[i]))
                return i;
        }

        return NO_MATCH;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeStringArray(out, constructorParameterNames);
        IOUtils.writeBooleanArray(out, excludedIndexes);
        IOUtils.writeIntegerArray(out, spacePropertyToConstructorIndex);
        for (Class<?> type : constructorParameterTypes)
            IOUtils.writeString(out, type.getName());
        out.writeInt(idPropertyConstructorIndex);
        out.writeInt(dynamicPropertiesConstructorIndex);
        out.writeInt(versionConstructorIndex);
        out.writeInt(persistConstructorIndex);
        out.writeInt(leaseConstructorIndex);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        constructorParameterNames = IOUtils.readStringArray(in);
        excludedIndexes = IOUtils.readBooleanArray(in);
        spacePropertyToConstructorIndex = IOUtils.readIntegerArray(in);
        constructorParameterTypes = new Class<?>[constructorParameterNames.length];
        for (int i = 0; i < constructorParameterTypes.length; i++) {
            String typeName = IOUtils.readString(in);
            Class<?> type = ObjectUtils.isPrimitive(typeName) ? ObjectUtils.getPrimitive(typeName)
                    : ClassLoaderHelper.loadClass(typeName, false /* localOnly */);
            constructorParameterTypes[i] = type;
        }
        idPropertyConstructorIndex = in.readInt();
        dynamicPropertiesConstructorIndex = in.readInt();
        versionConstructorIndex = in.readInt();
        persistConstructorIndex = in.readInt();
        leaseConstructorIndex = in.readInt();
    }

}
