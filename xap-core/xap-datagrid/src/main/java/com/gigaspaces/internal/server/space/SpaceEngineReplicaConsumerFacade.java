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

package com.gigaspaces.internal.server.space;

import com.gigaspaces.events.GSEventRegistration;
import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.internal.cluster.node.impl.backlog.sync.IMarker;
import com.gigaspaces.internal.cluster.node.impl.replica.ISpaceReplicaConsumeFacade;
import com.gigaspaces.internal.cluster.node.replica.SpaceCopyReplicaParameters.ReplicaType;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.logger.Constants;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@com.gigaspaces.api.InternalApi
public class SpaceEngineReplicaConsumerFacade
        implements ISpaceReplicaConsumeFacade {
    private final static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_REPLICA);
    private final SpaceEngine _spaceEngine;

    public SpaceEngineReplicaConsumerFacade(SpaceEngine spaceEngine) {
        _spaceEngine = spaceEngine;
    }

    public void addTypeDesc(ITypeDesc typeDescriptor) throws Exception {
        _spaceEngine.getTypeManager().addTypeDesc(typeDescriptor);
        if (_spaceEngine.getCacheManager().isBlobStoreCachePolicy()) //need to be stored in case blobStore recovery will be used
            _spaceEngine.getCacheManager().getStorageAdapter().introduceDataType(typeDescriptor);

    }

    public void write(IEntryPacket entryPacket, IMarker evictionMarker, ReplicaType replicaType)
            throws Exception {
        if (_logger.isTraceEnabled())
            _logger.trace(_spaceEngine.getReplicationNode()
                    + " inserting entry " + entryPacket);
        if (evictionMarker != null && _spaceEngine.getCacheManager().requiresEvictionReplicationProtection()) {
            _spaceEngine.getCacheManager()
                    .getEvictionReplicationsMarkersRepository()
                    .insert(entryPacket.getUID(), evictionMarker, false);
        }
        _spaceEngine.write(entryPacket,
                null /* txn */,
                entryPacket.getTTL(),
                0 /* modifiers */,
                _spaceEngine.isReplicated() /* fromRepl */,
                replicaType == ReplicaType.COPY/* origin */,
                null);
    }

    @Override
    public void remove(String uidToRemove, IMarker evictionMarker, ReplicaType replicaType) throws Exception {
        throw new UnsupportedOperationException("remove operation is not supported in SpaceEngineReplicaConsumerFacade");
    }

    public GSEventRegistration insertNotifyTemplate(
            ITemplatePacket templatePacket, String templateUid,
            NotifyInfo notifyInfo) throws Exception {
        if (_logger.isTraceEnabled())
            _logger.trace(_spaceEngine.getReplicationNode()
                    + " inserting notify template " + templatePacket);
        return _spaceEngine.notify(templatePacket,
                templatePacket.getTTL(),
                _spaceEngine.isReplicated() /* fromRepl */,
                templateUid,
                null,
                notifyInfo);
    }

}
