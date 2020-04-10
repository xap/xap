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
import com.gigaspaces.internal.cluster.node.replica.SpaceCopyReplicaParameters;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.transport.TemplatePacketFactory;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.UpdateOrWriteContext;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.client.ReadModifiers;
import com.j_spaces.core.client.UpdateModifiers;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@com.gigaspaces.api.InternalApi
public class SpaceEngineReplicaDirectPersistencySyncConsumerFacade implements ISpaceReplicaConsumeFacade {
    private final static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_REPLICA);
    private final SpaceEngine _spaceEngine;

    public SpaceEngineReplicaDirectPersistencySyncConsumerFacade(SpaceEngine spaceEngine) {
        _spaceEngine = spaceEngine;
    }


    public void remove(String uidToRemove, IMarker evictionMarker, SpaceCopyReplicaParameters.ReplicaType replicaType)
            throws Exception {
        if (_logger.isTraceEnabled()) {
            _logger.trace(_spaceEngine.getReplicationNode()
                    + " removing entry with uid: " + uidToRemove);
        }

        if (!_spaceEngine.getCacheManager().isEntryInPureCache(uidToRemove)) {
            if (_logger.isTraceEnabled()) {
                _logger.trace(_spaceEngine.getReplicationNode()
                        + " the entry with uid: " + uidToRemove + " does not exists, will not be deleted");
            }
            return;
        }

        ITemplatePacket template = TemplatePacketFactory.createUidPacket(uidToRemove, 0, true);

        _spaceEngine.read(template,
                null /* xtn */,
                0 /* timeout */,
                true/* ifExists */,
                true /* isTake */,
                null/* context */,
                true/* returnUID */,
                true /* fromRepl */,
                replicaType == SpaceCopyReplicaParameters.ReplicaType.COPY/*  /* origin */,
                ReadModifiers.MATCH_BY_ID /* modifiers */);

    }

    @Override
    public GSEventRegistration insertNotifyTemplate(ITemplatePacket templatePacket, String templateUid, NotifyInfo notifyInfo) throws Exception {
        if (_logger.isTraceEnabled())
            _logger.trace(_spaceEngine.getReplicationNode()
                    + " inserting notify template " + templatePacket);
        return _spaceEngine.notify(templatePacket,
                templatePacket.getTTL(),
                true /* fromRepl */,
                templateUid,
                null,
                notifyInfo);
    }

    @Override
    public void addTypeDesc(ITypeDesc typeDescriptor) throws Exception {
        _spaceEngine.getTypeManager().addTypeDesc(typeDescriptor);
        if (_spaceEngine.getCacheManager().isBlobStoreCachePolicy()) //need to be stored in case blobStore recovery will be used
            _spaceEngine.getCacheManager().getStorageAdapter().introduceDataType(typeDescriptor);
    }

    @Override
    public void write(IEntryPacket entryPacket, IMarker evictionMarker, SpaceCopyReplicaParameters.ReplicaType replicaType) throws Exception {
        if (_logger.isTraceEnabled())
            _logger.trace(_spaceEngine.getReplicationNode()
                    + " inserting entry " + entryPacket);

        boolean exists = _spaceEngine.getCacheManager().isEntryInPureCache(entryPacket.getUID());

        if (_logger.isDebugEnabled()) {
            String opStr = exists ? "UPDATE" : "WRITE";
            _logger.debug(_spaceEngine.getReplicationNode() + " [" + _spaceEngine.getFullSpaceName() + "]" + " performing " + opStr +
                    " operation, uid= " + entryPacket.getUID() + " ,version before operation: " + entryPacket.getVersion());
        }

        if (exists) {
            UpdateOrWriteContext ctx = new UpdateOrWriteContext(entryPacket,
                    entryPacket.getTTL(),
                    0 /* timeout */,
                    null,
                    null,
                    Modifiers.OVERRIDE_VERSION | UpdateModifiers.UPDATE_OR_WRITE,
                    false,
                    true,
                    false/* fromWriteMultiple */);

            _spaceEngine.updateOrWrite(ctx,
                    true /* fromReplication */,
                    false,
                    false);
        } else {
            _spaceEngine.write(entryPacket,
                    null,
                    entryPacket.getTTL(),
                    0 /* modifiers */,
                    true /* fromRepl */,
                    false,
                    null);
        }
    }

}
