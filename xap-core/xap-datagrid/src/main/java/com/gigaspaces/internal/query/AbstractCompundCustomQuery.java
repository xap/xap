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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for compound custom queries.
 *
 * @author Niv Ingberg
 * @since 7.1
 */
public abstract class AbstractCompundCustomQuery extends AbstractCustomQuery {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;
    // If serialization changes, increment GigaspacesVersionID and modify read/writeExternal appropiately.
    private static final byte GigaspacesVersionID = 1;

    protected List<ICustomQuery> _subQueries;

    /**
     * Default constructor for Externalizable.
     */
    public AbstractCompundCustomQuery() {
    }

    protected AbstractCompundCustomQuery(List<ICustomQuery> subQueries) {
        this._subQueries = subQueries;
    }

    public List<ICustomQuery> get_subQueries() {
        return _subQueries;
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);

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

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);

        out.writeByte(GigaspacesVersionID);
        writeExternalV1(out);
    }

    private void readExternalV1(ObjectInput in)
            throws IOException, ClassNotFoundException {
        int length = in.readInt();
        if (length >= 0) {
            _subQueries = new ArrayList<ICustomQuery>(length);
            for (int i = 0; i < length; i++) {
                ICustomQuery subQuery = (ICustomQuery) in.readObject();
                _subQueries.add(subQuery);
            }
        }
    }

    private void writeExternalV1(ObjectOutput out)
            throws IOException {
        if (_subQueries == null)
            out.writeInt(-1);
        else {
            int length = _subQueries.size();
            out.writeInt(length);
            for (int i = 0; i < length; i++)
                out.writeObject(_subQueries.get(i));
        }
    }
}
