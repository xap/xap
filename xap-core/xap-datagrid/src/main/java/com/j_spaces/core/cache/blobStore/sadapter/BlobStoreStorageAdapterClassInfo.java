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

package com.j_spaces.core.cache.blobStore.sadapter;


import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;

/**
 * class info  used & stored in B.S. Storage Adapter TypeDesc
 *
 * @author yechiel
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class BlobStoreStorageAdapterClassInfo implements SmartExternalizable {

    private static final long serialVersionUID = 8551182143707595380L;

    private boolean[] _indexesRelatedFixedProperties;
    private HashSet<String> _indexesRelatedDynamicProperties;
    private short _storedVersion;   //stored in entries using those indexing version


    public BlobStoreStorageAdapterClassInfo(boolean[] indexesRelatedFixedProperties, HashSet<String> indexesRelatedDynamicProperties, short storedVersion) {
        _indexesRelatedFixedProperties = indexesRelatedFixedProperties;
        _indexesRelatedDynamicProperties = indexesRelatedDynamicProperties;
        _storedVersion = storedVersion;

    }


    public BlobStoreStorageAdapterClassInfo() {
    }


    public boolean[] getIndexesRelatedFixedProperties() {
        return _indexesRelatedFixedProperties;
    }

    public HashSet<String> getIndexesRelatedDynamicProperties() {
        return _indexesRelatedDynamicProperties;
    }

    public short getStoredVersion() {
        return _storedVersion;
    }


    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _indexesRelatedFixedProperties = new boolean[in.readInt()];
        for (int i = 0; i < _indexesRelatedFixedProperties.length; i++)
            _indexesRelatedFixedProperties[i] = in.readBoolean();

        if (in.readBoolean()) {
            _indexesRelatedDynamicProperties = new HashSet<String>();
            int len = in.readInt();
            for (int i = 0; i < len; i++)
                _indexesRelatedDynamicProperties.add(in.readUTF());
        }

        _storedVersion = in.readShort();

    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(_indexesRelatedFixedProperties.length);
        for (boolean val : _indexesRelatedFixedProperties)
            out.writeBoolean(val);

        if (_indexesRelatedDynamicProperties != null) {
            out.writeBoolean(true);
            out.writeInt(_indexesRelatedDynamicProperties.size());
            for (String val : _indexesRelatedDynamicProperties)
                out.writeUTF(val);
        } else
            out.writeBoolean(false);

        out.writeShort(_storedVersion);
    }


}
