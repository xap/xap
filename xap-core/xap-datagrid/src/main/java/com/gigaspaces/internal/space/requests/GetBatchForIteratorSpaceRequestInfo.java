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
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.client.ReadModifiers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alon Shoham
 * @since 15.2.0
 */
@com.gigaspaces.api.InternalApi
public class GetBatchForIteratorSpaceRequestInfo extends AbstractSpaceRequestInfo {
    private static final long serialVersionUID = 1L;

    private static final Logger _devLogger = LoggerFactory.getLogger(Constants.LOGGER_DEV);

    private ITemplatePacket _templatePacket;
    private UUID _iteratorId;
    private int _modifiers;
    private int _batchSize;
    private int _batchNumber;
    private long _maxInactiveDuration;

    /**
     * Required for Externalizable.
     */
    public GetBatchForIteratorSpaceRequestInfo() {
    }

    public GetBatchForIteratorSpaceRequestInfo(
            ITemplatePacket templatePacket, int modifiers, int batchSize, int batchNumber, UUID iteratorId, long maxInactiveDuration) {
        this._templatePacket = templatePacket;
        this._modifiers = modifiers;
        this._batchSize = batchSize;
        this._batchNumber = batchNumber;
        this._iteratorId = iteratorId;
        this._maxInactiveDuration = maxInactiveDuration;
    }

    public ITemplatePacket getTemplatePacket() {
        return _templatePacket;
    }

    public int getBatchSize() {
        return _batchSize;
    }

    public int getModifiers() {
        return _modifiers;
    }

    public UUID getIteratorId() {
        return _iteratorId;
    }

    public long getMaxInactiveDuration() {
        return _maxInactiveDuration;
    }

    private static final short FLAG_MODIFIERS = 1;

    private static final int DEFAULT_MODIFIERS = ReadModifiers.REPEATABLE_READ;

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);
        final short flags = buildFlags();
        out.writeShort(flags);
        IOUtils.writeObject(out, _templatePacket);
        IOUtils.writeUUID(out, _iteratorId);
        out.writeInt(_batchSize);
        out.writeInt(_batchNumber);
        out.writeLong(_maxInactiveDuration);
        if (flags != 0) {
            if (_modifiers != DEFAULT_MODIFIERS)
                out.writeInt(_modifiers);
        }
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);
        final short flags = in.readShort();
        _templatePacket = IOUtils.readObject(in);
        _iteratorId = IOUtils.readUUID(in);
        _batchSize = in.readInt();
        _batchNumber = in.readInt();
        _maxInactiveDuration = in.readLong();
        if (flags != 0) {
            _modifiers = (flags & FLAG_MODIFIERS) != 0 ? in.readInt() : DEFAULT_MODIFIERS;
        } else {
            _modifiers = DEFAULT_MODIFIERS;
        }
    }

    private short buildFlags() {
        short flags = 0;
        if (_modifiers != DEFAULT_MODIFIERS)
            flags |= FLAG_MODIFIERS;
        return flags;
    }

    public int getBatchNumber() {
        return _batchNumber;
    }
}
