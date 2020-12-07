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

package com.gigaspaces.internal.transport;

import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.IntegerSet;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.metadata.TypeDesc;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.metadata.StorageType;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * Holds projection template data
 *
 * @author eitany
 * @since 9.5
 */
@com.gigaspaces.api.InternalApi
public class ProjectionTemplate extends AbstractProjectionTemplate {
    private static final long serialVersionUID = 1L;

    private int[] _fixedIndexes;
    private String[] _dynamicProperties;
    private String[] _fixedPaths;
    private String[] _dynamicPaths;


    public ProjectionTemplate() {
    }

    private ProjectionTemplate(int[] fixedIndexes,
                               String[] dynamicProperties, String[] fixedPaths, String[] dynamicPaths) {
        _fixedIndexes = fixedIndexes;
        _dynamicProperties = dynamicProperties;
        _fixedPaths = fixedPaths;
        _dynamicPaths = dynamicPaths;
    }

    @Override
    public int[] getFixedPropertiesIndexes() {
        return _fixedIndexes;
    }

    @Override
    public String[] getDynamicProperties() {
        return _dynamicProperties;
    }

    @Override
    public String[] getFixedPaths() {
        return _fixedPaths;
    }

    @Override
    public String[] getDynamicPaths() {
        return _dynamicPaths;
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeIntegerArray(out, _fixedIndexes);
        IOUtils.writeRepetitiveStringArray(out, _dynamicProperties);

        final PlatformLogicalVersion endpointVersion = LRMIInvocationContext.getEndpointLogicalVersion();
        if (endpointVersion.greaterOrEquals(PlatformLogicalVersion.v9_7_0) ||
                endpointVersion.patchSameOrGreater(PlatformLogicalVersion.v9_6_2_PATCH3)) {
            IOUtils.writeRepetitiveStringArray(out, _fixedPaths);
            IOUtils.writeRepetitiveStringArray(out, _dynamicPaths);
        } else {
            if (_fixedPaths != null || _dynamicPaths != null) {
                throw new UnsupportedOperationException("server version do not support paths projection");
            }
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _fixedIndexes = IOUtils.readIntegerArray(in);
        _dynamicProperties = IOUtils.readRepetitiveStringArray(in);
        final PlatformLogicalVersion endpointVersion = LRMIInvocationContext.getEndpointLogicalVersion();
        if (endpointVersion.greaterOrEquals(PlatformLogicalVersion.v9_7_0) ||
                endpointVersion.patchSameOrGreater(PlatformLogicalVersion.v9_6_2_PATCH3)) {
            _fixedPaths = IOUtils.readRepetitiveStringArray(in);
            _dynamicPaths = IOUtils.readRepetitiveStringArray(in);
        }
    }

    public static ProjectionTemplate create(String[] projections, ITypeDesc typeDesc) {
        IntegerSet fixedIndexes = null;
        List<String> dynamicProperties = null;
        List<String> fixedPaths = null;
        List<String> dynamicPaths = null;
        HashSet<String> existing = null;

        for (String projection : projections) {
            int fixedPropertyPosition = typeDesc != null ? typeDesc.getFixedPropertyPosition(projection) : TypeDesc.NO_SUCH_PROPERTY;
            if (fixedPropertyPosition != TypeDesc.NO_SUCH_PROPERTY) {
                if (fixedIndexes == null)
                    fixedIndexes = CollectionsFactory.getInstance().createIntegerSet(projections.length);
                fixedIndexes.add(fixedPropertyPosition);
                if (existing == null)
                    existing = new HashSet<String>();
                existing.add(projection);
                continue;
            }

            int dotPos = projection.indexOf(".");
            if (dotPos == -1) {//dynamic
                if (dynamicProperties == null)
                    dynamicProperties = new ArrayList<String>(projections.length);
                dynamicProperties.add(projection);
                if (existing == null)
                    existing = new HashSet<String>();
                existing.add(projection);
                continue;
            }
            String property = projection.substring(0, dotPos);
            if (existing != null && existing.contains(property))
                throw new UnsupportedOperationException("Projection cannot contain both a path and a projection of its root property  [" + projection + "]");

            fixedPropertyPosition = typeDesc.getFixedPropertyPosition(property);
            if (fixedPropertyPosition == TypeDesc.NO_SUCH_PROPERTY) {
                String token = PathProjectionInfo.removeCollectionInfoIfExist(property);
                if (token != property) {
                    property = token;
                    fixedPropertyPosition = typeDesc.getFixedPropertyPosition(property);
                }
            }
            if (fixedPropertyPosition != TypeDesc.NO_SUCH_PROPERTY) {
                PropertyInfo pi = typeDesc.getFixedProperty(fixedPropertyPosition);
                if (typeDesc.getClassBinaryStorageAdapter() == null &&
                        (pi.getStorageType() != StorageType.OBJECT && (!(pi.getStorageType() == StorageType.DEFAULT && typeDesc.getStorageType() == StorageType.OBJECT))))
                    throw new UnsupportedOperationException("Projection of paths is valid only with StorageType = OBJECT [" + projection + "]");
                if (fixedPaths == null)
                    fixedPaths = new ArrayList<String>(projections.length);
                fixedPaths.add(projection);
            } else {
                if (typeDesc.getStorageType() != StorageType.OBJECT)
                    throw new UnsupportedOperationException("Projection of paths is valid only with StorageType = OBJECT [" + projection + "]");
                if (dynamicPaths == null)
                    dynamicPaths = new ArrayList<String>(projections.length);
                dynamicPaths.add(projection);
            }
        }
        int[] fixedIndexesArray = fixedIndexes != null ? fixedIndexes.toArray() : null;
        String[] dynamicPropertiesArray = dynamicProperties != null ? dynamicProperties.toArray(new String[dynamicProperties.size()]) : null;
        String[] fixpathsArray = fixedPaths != null ? fixedPaths.toArray(new String[fixedPaths.size()]) : null;
        String[] dynamicpathsArray = dynamicPaths != null ? dynamicPaths.toArray(new String[dynamicPaths.size()]) : null;
        return new ProjectionTemplate(fixedIndexesArray, dynamicPropertiesArray, fixpathsArray, dynamicpathsArray);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(_dynamicProperties);
        result = prime * result + Arrays.hashCode(_fixedIndexes);
        result = prime * result + Arrays.hashCode(_fixedPaths);
        result = prime * result + Arrays.hashCode(_dynamicPaths);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ProjectionTemplate other = (ProjectionTemplate) obj;
        if (!Arrays.equals(_dynamicProperties, other._dynamicProperties))
            return false;
        if (!Arrays.equals(_fixedIndexes, other._fixedIndexes))
            return false;
        if (!Arrays.equals(_fixedPaths, other._fixedPaths))
            return false;
        if (!Arrays.equals(_dynamicPaths, other._dynamicPaths))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "ProjectionTemplate [fixedIndexes=" + _fixedIndexes
                + ", dynamicProperties=" + _dynamicProperties + ", fixedPaths" + _fixedPaths + ", dynamicPaths" + _dynamicPaths + "]";
    }


}
