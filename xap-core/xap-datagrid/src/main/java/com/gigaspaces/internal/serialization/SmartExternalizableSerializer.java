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
package com.gigaspaces.internal.serialization;

import com.gigaspaces.internal.io.MarshalInputStream;
import com.gigaspaces.internal.io.MarshalOutputStream;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Niv Ingberg
 * @since 16.0
 */
public class SmartExternalizableSerializer<T extends Externalizable> implements IClassSerializer<T> {

    public static final SmartExternalizableSerializer instance = new SmartExternalizableSerializer();

    private SmartExternalizableSerializer() {
    }

    @Override
    public byte getCode() {
        return CODE_SMART_EXTERNALIZABLE;
    }

    @Override
    public void write(ObjectOutput out, T obj) throws IOException {
        ((MarshalOutputStream)out).writeSmartExternalizable((SmartExternalizable) obj);
    }

    @Override
    public T read(ObjectInput in) throws IOException, ClassNotFoundException {
        return (T) ((MarshalInputStream)in).readSmartExternalizable();
    }
}
