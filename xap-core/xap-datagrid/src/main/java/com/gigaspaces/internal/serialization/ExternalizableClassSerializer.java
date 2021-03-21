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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.Supplier;

/**
 * Serializer for Externalizable.
 *
 * @author Niv Ingberg
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class ExternalizableClassSerializer implements IClassSerializer<Externalizable> {
    private final byte _code;
    private final Supplier<Externalizable> factory;
    private final Class<Externalizable> _type;

    public ExternalizableClassSerializer(byte code, Supplier<Externalizable> factory) {
        this._code = code;
        this.factory = factory;
        this._type = (Class<Externalizable>) factory.get().getClass();
    }

    public byte getCode() {
        return _code;
    }

    public Class<Externalizable> getType() {
        return _type;
    }

    public void write(ObjectOutput out, Externalizable obj) throws IOException {
        obj.writeExternal(out);
    }

    public Externalizable read(ObjectInput in) throws IOException, ClassNotFoundException {
        Externalizable result = factory.get();
        result.readExternal(in);
        return result;
    }
}
