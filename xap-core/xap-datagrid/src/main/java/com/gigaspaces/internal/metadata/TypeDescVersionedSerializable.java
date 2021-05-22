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

import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Dan Kilman
 * @since 9.1.1
 */
@com.gigaspaces.api.InternalApi
public class TypeDescVersionedSerializable implements SmartExternalizable {
    private static final long serialVersionUID = 6859190936042333450L;

    private TypeDesc _typeDesc;

    /* For Externalizable */
    public TypeDescVersionedSerializable() {
    }

    public TypeDescVersionedSerializable(TypeDesc typeDesc) {
        _typeDesc = typeDesc;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        PlatformLogicalVersion version = PlatformLogicalVersion.getLogicalVersion();
        version.writeExternal(out);
        _typeDesc.writeExternal(out, version, false);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        PlatformLogicalVersion version = new PlatformLogicalVersion();
        version.readExternal(in);
        _typeDesc = new TypeDesc();
        _typeDesc.readExternal(in, version, false);
    }

    public ITypeDesc getTypeDesc() {
        return _typeDesc;
    }

}
