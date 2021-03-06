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

package com.gigaspaces.internal.cluster.node.impl.directPersistency;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Boris
 * @since 10.2.0
 */
@com.gigaspaces.api.InternalApi
public class DirectPersistencySyncListBatch implements SmartExternalizable {

    private static final long serialVersionUID = 1L;

    private List<String> buff;

    public DirectPersistencySyncListBatch() {
        this.buff = new LinkedList<String>();
    }

    public List<String> getBatch() {
        return buff;
    }

    public void addEntryToBatch(String s) {
        buff.add(s);
    }

    public int size() {
        return buff.size();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(buff.size());
        for (String s : buff) {
            out.writeUTF(s);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        if (size > 0) {
            for (int i = 0; i < size; i++) {
                buff.add(in.readUTF());
            }
        }
    }

}
