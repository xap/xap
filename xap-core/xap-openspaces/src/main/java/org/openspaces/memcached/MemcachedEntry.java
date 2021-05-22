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

package org.openspaces.memcached;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;
import com.gigaspaces.annotation.pojo.SpaceVersion;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author kimchy (shay.banon)
 */
@SpaceClass
public class MemcachedEntry implements SmartExternalizable {

    private static final long serialVersionUID = 7080552232191270155L;

    private Key key;

    private byte[] value;

    private int flags;

    private int version;

    public MemcachedEntry() {
    }

    public MemcachedEntry(Key key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    @SpaceId(autoGenerate = false)
    @SpaceRouting
    public Key getKey() {
        return key;
    }

    public void setKey(Key key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    @SpaceVersion
    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getFlags() {
        return flags;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        if (key == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            key.writeExternal(out);
        }
        if (value == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeInt(value.length);
            out.write(value);
        }
        out.writeInt(flags);
        out.writeInt(version);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        if (in.readBoolean()) {
            key = new Key();
            key.readExternal(in);
        }
        if (in.readBoolean()) {
            value = new byte[in.readInt()];
            in.readFully(value);
        }
        flags = in.readInt();
        version = in.readInt();
    }
}
