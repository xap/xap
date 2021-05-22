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

package com.gigaspaces.internal.client.mutators;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

/**
 * Result for changing a map property
 *
 * @author eitany
 * @since 9.7
 */
public final class MapChangeSpaceEntryMutatorResult
        implements SmartExternalizable {

    private static final long serialVersionUID = 1L;
    private Serializable _value;
    private int _size;

    public MapChangeSpaceEntryMutatorResult() {
    }

    public MapChangeSpaceEntryMutatorResult(Serializable value, int size) {
        _value = value;
        _size = size;
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, _value);
        out.writeInt(_size);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _value = IOUtils.readObject(in);
        _size = in.readInt();
    }

    public int getSize() {
        return _size;
    }

    public Serializable getValue() {
        return _value;
    }

    @Override
    public String toString() {
        return "MapChangeSpaceEntryMutatorResult [getSize()=" + getSize()
                + ", getValue()=" + getValue() + "]";
    }


}
