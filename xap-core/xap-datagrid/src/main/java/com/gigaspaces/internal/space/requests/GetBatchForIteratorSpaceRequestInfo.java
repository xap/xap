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

import com.gigaspaces.internal.client.SpaceIteratorBatchResult;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.query.QueryUtils;
import com.gigaspaces.internal.query.explainplan.ExplainPlanImpl;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterExecutionType;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterRemoteOperationRouter;
import com.gigaspaces.internal.server.space.operations.SpaceOperationsCodes;
import com.gigaspaces.internal.space.requests.AbstractSpaceRequestInfo;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.GetBatchForIteratorException;
import com.j_spaces.core.client.ReadModifiers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Alon Shoham
 * @since 15.2.0
 */
@com.gigaspaces.api.InternalApi
public class GetBatchForIteratorSpaceRequestInfo extends AbstractSpaceRequestInfo {
    private static final long serialVersionUID = 1L;

    private static final Logger _devLogger = Logger.getLogger(Constants.LOGGER_DEV);

    private ITemplatePacket _templatePacket;
    private UUID _iteratorId;
    private int _modifiers;
    private int _batchSize;
    private int _batchNumber;

    /**
     * Required for Externalizable.
     */
    public GetBatchForIteratorSpaceRequestInfo() {
    }

    public GetBatchForIteratorSpaceRequestInfo(
            ITemplatePacket templatePacket, int modifiers, int batchSize, int batchNumber, UUID iteratorId) {
        this._templatePacket = templatePacket;
        this._modifiers = modifiers;
        this._batchSize = batchSize;
        this._batchNumber = batchNumber;
        this._iteratorId = iteratorId;
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

    public long getLease() {
        //TODO add lease field to flow
        return 1000;
    }

    public boolean isFirstTime() {
        return _batchNumber == 0;
    }

    public int getBatchNumber() {
        return _batchNumber;
    }
}
