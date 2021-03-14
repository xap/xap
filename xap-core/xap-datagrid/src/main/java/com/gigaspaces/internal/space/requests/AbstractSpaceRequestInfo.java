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

package com.gigaspaces.internal.space.requests;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.ISwapExternalizable;
import com.j_spaces.core.SpaceContext;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Niv Ingberg
 * @since 8.0
 */
public abstract class AbstractSpaceRequestInfo implements SpaceRequestInfo, Externalizable, ISwapExternalizable {
    private static final long serialVersionUID = 1L;

    private static final int FROM_GATEWAY = 1; // 1 << 0 same as 1
    private static final int SPACE_CONTEXT = 1 << 1;

    private SpaceContext _spaceContext;
    private boolean _fromGateway = false;

    /**
     * Required for Externalizable
     */
    public AbstractSpaceRequestInfo() {
    }

    public boolean isFromGateway() {
        return _fromGateway;
    }

    public void setFromGateway(boolean fromGateway) {
        _fromGateway = fromGateway;
    }

    private int buildFlags() {
        int flags = 0;
        if (_fromGateway) {
            flags |= FROM_GATEWAY;
        }
        if (_spaceContext != null) {
            flags |= SPACE_CONTEXT;
        }
        return flags;
    }

    @Override
    public SpaceContext getSpaceContext() {
        return _spaceContext;
    }

    public void setSpaceContext(SpaceContext spaceContext) {
        _spaceContext = spaceContext;
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        serialize(out);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        serialize(out);
    }

    private void serialize(ObjectOutput out) throws IOException {
        final int flags = buildFlags();
        out.writeInt(flags);
        if ((flags & SPACE_CONTEXT) != 0) {
            IOUtils.writeObject(out, _spaceContext);
        }
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        deserialize(in);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        deserialize(in);
    }

    private void deserialize(ObjectInput in) throws IOException,
            ClassNotFoundException {
        int flags = in.readInt();
        if ((flags & FROM_GATEWAY) != 0)
            _fromGateway = true;
        if ((flags & SPACE_CONTEXT) != 0)
            _spaceContext = IOUtils.readObject(in);
    }
}
