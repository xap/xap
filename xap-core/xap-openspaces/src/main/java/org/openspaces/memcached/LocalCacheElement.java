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

import com.gigaspaces.serialization.SmartExternalizable;
import org.openspaces.memcached.util.BufferUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Represents information about a cache entry.
 */
public final class LocalCacheElement implements SmartExternalizable {

    private static final long serialVersionUID = -1132098185117104961L;

    private int expire;
    private int flags;
    private byte[] data;
    private Key key;
    private long casUnique = 0L;

    public LocalCacheElement() {
    }

    public LocalCacheElement(Key key) {
        this.key = key;
    }

    public LocalCacheElement(Key key, int flags, int expire, long casUnique) {
        this.key = key;
        this.flags = flags;
        this.expire = expire;
        this.casUnique = casUnique;
    }

    /**
     * @return the current time in seconds
     */
    public static int Now() {
        return (int) (System.currentTimeMillis() / 1000);
    }

    public int size() {
        return getData().length;
    }

    public LocalCacheElement append(LocalCacheElement element) {
        int newLength = getData().length + element.getData().length;
        LocalCacheElement replace = new LocalCacheElement(getKey(), getFlags(), getExpire(), 0L);
        ByteBuffer b = ByteBuffer.allocateDirect(newLength);
        b.put(getData());
        b.put(element.getData());
        replace.setData(new byte[newLength]);
        b.flip();
        b.get(replace.getData());
        replace.setCasUnique(replace.getCasUnique() + 1);

        return replace;
    }

    public LocalCacheElement prepend(LocalCacheElement element) {
        int newLength = getData().length + element.getData().length;

        LocalCacheElement replace = new LocalCacheElement(getKey(), getFlags(), getExpire(), 0L);
        ByteBuffer b = ByteBuffer.allocateDirect(newLength);
        b.put(element.getData());
        b.put(getData());
        replace.setData(new byte[newLength]);
        b.flip();
        b.get(replace.getData());
        replace.setCasUnique(replace.getCasUnique() + 1);

        return replace;
    }

    public static class IncrDecrResult {
        int oldValue;
        LocalCacheElement replace;

        public IncrDecrResult(int oldValue, LocalCacheElement replace) {
            this.oldValue = oldValue;
            this.replace = replace;
        }
    }

    public IncrDecrResult add(int mod) {
        // TODO handle parse failure!
        int old_val = BufferUtils.atoi(getData()) + mod; // change value
        if (old_val < 0) {
            old_val = 0;

        } // check for underflow

        byte[] newData = BufferUtils.itoa(old_val);

        LocalCacheElement replace = new LocalCacheElement(getKey(), getFlags(), getExpire(), 0L);
        replace.setData(newData);
        replace.setCasUnique(replace.getCasUnique() + 1);

        return new IncrDecrResult(old_val, replace);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LocalCacheElement that = (LocalCacheElement) o;

        if (casUnique != that.casUnique) return false;
        if (expire != that.expire) return false;
        if (flags != that.flags) return false;
        if (!Arrays.equals(data, that.data)) return false;
        if (!key.equals(that.key)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = expire;
        result = 31 * result + flags;
        result = 31 * result + (data != null ? Arrays.hashCode(data) : 0);
        result = 31 * result + key.hashCode();
        result = 31 * result + (int) (casUnique ^ (casUnique >>> 32));
        return result;
    }

    public static LocalCacheElement key(Key key) {
        return new LocalCacheElement(key);
    }

    public int getExpire() {
        return expire;
    }

    public int getFlags() {
        return flags;
    }

    public byte[] getData() {
        return data;
    }

    public Key getKey() {
        return key;
    }

    public long getCasUnique() {
        return casUnique;
    }

    public void setCasUnique(long casUnique) {
        this.casUnique = casUnique;
    }


    public void setData(byte[] data) {
        this.data = data;
    }

    public void readExternal(ObjectInput in) throws IOException {
        expire = in.readInt();
        flags = in.readInt();

        final int length = in.readInt();
        int readSize = 0;
        data = new byte[length];
        while (readSize < length)
            readSize += in.read(data, readSize, length - readSize);

        key = new Key(new byte[in.readInt()]);
        in.read(key.bytes);
        casUnique = in.readLong();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(expire);
        out.writeInt(flags);
        out.writeInt(data.length);
        out.write(data);
        out.write(key.bytes.length);
        out.write(key.bytes);
        out.writeLong(casUnique);
    }
}