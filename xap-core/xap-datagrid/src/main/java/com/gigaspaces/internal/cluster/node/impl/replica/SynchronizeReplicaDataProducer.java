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

package com.gigaspaces.internal.cluster.node.impl.replica;

import com.gigaspaces.internal.cluster.node.replica.SpaceCopyReplicaParameters;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.j_spaces.core.cluster.IReplicationFilterEntry;
import com.j_spaces.kernel.SystemProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

/**
 * SynchronizeReplicaDataProducer copies all the entries in space and all the notifications
 *
 * @author anna
 * @version 1.0
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class SynchronizeReplicaDataProducer
        extends AbstractMultiSpaceReplicaDataProducer {

    public SynchronizeReplicaDataProducer(SpaceEngine engine,
                                          SpaceCopyReplicaParameters parameters, Object requestContext) {
        super(engine, parameters, requestContext);
    }

    @Override
    protected List<ISingleStageReplicaDataProducer<? extends IExecutableSpaceReplicaData>> buildDataProducers(
            SpaceCopyReplicaParameters parameters) {
        ArrayList<ISingleStageReplicaDataProducer<? extends IExecutableSpaceReplicaData>> dataProducers = new ArrayList<ISingleStageReplicaDataProducer<? extends IExecutableSpaceReplicaData>>();
        dataProducers.add(new SpaceTypeReplicaDataProducer(_engine));
        if (_logger.isLoggable(Level.FINEST))
            _logger.finest(_engine.getReplicationNode() + "created SpaceTypeReplicaDataProducer");
        for (ITemplatePacket templatePacket : parameters.getTemplatePackets()) {
            // create unique EntryReplicaProducer that deals with direct persistency sync list
            if (parameters.getSynchronizationListFetcher() != null) {
                dataProducers.add(new DirectPersisntecyEntryReplicaProducer(_engine, parameters, templatePacket, _requestContext));
            } else {
                dataProducers.add(new EntryReplicaProducer(_engine, parameters, templatePacket, _requestContext));
            }
            if (_logger.isLoggable(Level.FINEST))
                _logger.finest(_engine.getReplicationNode() + "created EntryReplicaProducer for templatePacket " + templatePacket);
        }

        if (parameters.isCopyNotifyTemplates()) {
            dataProducers.add(new NotifyTemplateReplicaProducer(_engine, _requestContext));
            if (_logger.isLoggable(Level.FINEST))
                _logger.finest(_engine.getReplicationNode() + "created NotifyTemplateReplicaProducer");
        }
        return dataProducers;
    }

    @Override
    public void onClose() {
        //On close we want to force lease reaper cycle before we mark the backlog the replication is completed to avoid false warning on lease
        //expiration of entries that were expired during the recovery and not copied to target but the lease reaper didn't run yet so it didn't create their 
        //replication events which will not be filtered if the backlog would mark the current key as the copy stage completion
        try {
            if (!_engine.getLeaseManager().isCurrentLeaseReaperThread()) {
                if (_engine.getCacheManager().isBlobStoreCachePolicy()) {
                    long forceLeaseReaperCycleTimeToWait = Long.getLong(SystemProperties.LEASE_MANAGER_FORCE_REAPER_CYCLE_TIME_TO_WAIT, SystemProperties.LEASE_MANAGER_FORCE_REAPER_CYCLE_TIME_TO_WAIT_DEFAULT);
                    _engine.getLeaseManager().forceLeaseReaperCycle(true, forceLeaseReaperCycleTimeToWait);
                } else {
                    _engine.getLeaseManager().forceLeaseReaperCycle(true);
                }

            }

        } catch (InterruptedException e) {
        }
    }

    public IReplicationFilterEntry toFilterEntry(IExecutableSpaceReplicaData data) {
        return data.toFilterEntry(_engine.getTypeManager());
    }

    @Override
    public String getName() {
        return "SynchronizeReplicaDataProducer";
    }

}
