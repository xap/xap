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

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.serialization.pbs.PbsInputStream;
import com.gigaspaces.serialization.pbs.PbsStreamResource;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;


/**
 * Holds PBS projection data.
 *
 * @author Idan Moyal
 * @since 9.5
 */
@com.gigaspaces.api.InternalApi
public class PbsProjectionTemplate extends AbstractProjectionTemplate {
    private static final long serialVersionUID = 1L;

    private byte[] _serializedFixedPropertiesIndexes;
    private String[] _dynamicProperties;
    private String[] _fixedPaths;
    private String[] _dynamicPaths;

    private transient int[] _fixedPropertiesIndexes;
    private transient boolean _unmarshed;


    public PbsProjectionTemplate() {
    }

    public PbsProjectionTemplate(byte[] serializedFixedPropertiesIndexes,
                                 String[] dynamicProperties, String[] fixedPaths, String[] dynamicPaths) {
        _serializedFixedPropertiesIndexes = serializedFixedPropertiesIndexes;
        _dynamicProperties = dynamicProperties;
        _fixedPaths = fixedPaths;
        _dynamicPaths = dynamicPaths;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _serializedFixedPropertiesIndexes = IOUtils.readByteArray(in);
        _dynamicProperties = IOUtils.readRepetitiveStringArray(in);
        final PlatformLogicalVersion endpointVersion = LRMIInvocationContext.getEndpointLogicalVersion();
        if (endpointVersion.greaterOrEquals(PlatformLogicalVersion.v9_7_0) ||
                endpointVersion.samePatch(PlatformLogicalVersion.v9_6_2_PATCH3)) {
            _fixedPaths = IOUtils.readRepetitiveStringArray(in);
            _dynamicPaths = IOUtils.readRepetitiveStringArray(in);
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeByteArray(out, _serializedFixedPropertiesIndexes);
        IOUtils.writeRepetitiveStringArray(out, _dynamicProperties);
        final PlatformLogicalVersion endpointVersion = LRMIInvocationContext.getEndpointLogicalVersion();
        if (endpointVersion.greaterOrEquals(PlatformLogicalVersion.v9_7_0) ||
                endpointVersion.samePatch(PlatformLogicalVersion.v9_6_2_PATCH3)) {
            IOUtils.writeRepetitiveStringArray(out, _fixedPaths);
            IOUtils.writeRepetitiveStringArray(out, _dynamicPaths);
        } else {
            if (_fixedPaths != null || _dynamicPaths != null)
                throw new UnsupportedOperationException("server version do not support paths projection");
        }
    }

    @Override
    public int[] getFixedPropertiesIndexes() {
        if (!_unmarshed)
            unmarsh();
        return _fixedPropertiesIndexes;
    }

    private void unmarsh() {
        if (_serializedFixedPropertiesIndexes == null) {
            _unmarshed = true;
            return;
        }

        PbsInputStream input = PbsStreamResource.getInputStream(_serializedFixedPropertiesIndexes);
        try {
            _fixedPropertiesIndexes = input.readIntArray();
            _unmarshed = true;
        } finally {
            PbsStreamResource.releasePbsStream(input);
        }
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
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(_dynamicProperties);
        result = prime * result
                + Arrays.hashCode(_serializedFixedPropertiesIndexes);
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
        PbsProjectionTemplate other = (PbsProjectionTemplate) obj;
        if (!Arrays.equals(_dynamicProperties, other._dynamicProperties))
            return false;
        if (!Arrays.equals(_serializedFixedPropertiesIndexes,
                other._serializedFixedPropertiesIndexes))
            return false;
        if (!Arrays.equals(_fixedPaths, other._fixedPaths))
            return false;
        if (!Arrays.equals(_dynamicPaths, other._dynamicPaths))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "PbsProjectionTemplate [fixedPropertiesIndexes="
                + (_fixedPropertiesIndexes != null ? _fixedPropertiesIndexes
                : "serialized")
                + ", dynamicProperties=" + _dynamicProperties
                + ", fixedPaths=" + _fixedPaths
                + ", dynamicPaths=" + _dynamicPaths + "]";
    }

}
