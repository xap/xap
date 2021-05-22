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

package com.gigaspaces.internal.query;

import com.gigaspaces.internal.metadata.TypeDesc;
import com.gigaspaces.serialization.IllegalSerializationVersionException;
import com.gigaspaces.serialization.SmartExternalizable;
import com.gigaspaces.server.ServerEntry;
import com.j_spaces.core.cache.CacheManager;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.List;

/**
 * Base class for all custom query interfaces.
 *
 * @author Niv Ingberg
 * @since 7.1
 */
public abstract class AbstractCustomQuery implements ICustomQuery, SmartExternalizable {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;
    // If serialization changes, increment GigaspacesVersionID and modify read/writeExternal appropiately.
    private static final byte GigaspacesVersionID = 1;

    // list of custom defined indexes
    private List<IQueryIndexScanner> _customIndexes = new LinkedList<IQueryIndexScanner>();

    /**
     * Default constructor for Externalizable.
     */
    public AbstractCustomQuery() {
    }

    public abstract boolean matches(CacheManager cacheManager, ServerEntry entry, String skipAlreadyMatchedIndexPath);

    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        byte version = in.readByte();

        if (version == GigaspacesVersionID)
            readExternalV1(in);
        else {
            switch (version) {
                default:
                    throw new IllegalSerializationVersionException(TypeDesc.class, version);
            }
        }
    }

    /**
     * @param customIndex
     */
    public void addCustomIndex(IQueryIndexScanner customIndex) {
        _customIndexes.add(customIndex);
    }

    public List<IQueryIndexScanner> getCustomIndexes() {
        return _customIndexes;
    }

    public void writeExternal(ObjectOutput out)
            throws IOException {
        out.writeByte(GigaspacesVersionID);
        writeExternalV1(out);
    }

    private void readExternalV1(ObjectInput in)
            throws IOException, ClassNotFoundException {
        boolean hasCustomIndex = in.readBoolean();

        if (hasCustomIndex)
            _customIndexes = (List<IQueryIndexScanner>) in.readObject();
    }

    private void writeExternalV1(ObjectOutput out)
            throws IOException {
        if (_customIndexes != null) {
            out.writeBoolean(true);
            out.writeObject(_customIndexes);
        } else {
            out.writeBoolean(false);
        }
    }
}
