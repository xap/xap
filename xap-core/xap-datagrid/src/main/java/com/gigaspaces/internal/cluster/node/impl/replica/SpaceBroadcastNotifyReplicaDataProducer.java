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
import com.j_spaces.core.cluster.IReplicationFilterEntry;

import java.util.ArrayList;
import java.util.List;


/**
 * {@link BroadcastNotifyTemplateReplicaProducer} creates a replica of the space broadcast
 * notifications. Used to recover broadcast notify template from other partitions.
 *
 * @author anna
 * @version 1.0
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceBroadcastNotifyReplicaDataProducer
        extends AbstractMultiSpaceReplicaDataProducer {

    public SpaceBroadcastNotifyReplicaDataProducer(SpaceEngine engine,
                                                   SpaceCopyReplicaParameters parameters, Object requestContext) {
        super(engine, parameters, requestContext);
    }

    @Override
    protected List<ISingleStageReplicaDataProducer<? extends IExecutableSpaceReplicaData>> buildDataProducers(
            SpaceCopyReplicaParameters parameters) {
        ArrayList<ISingleStageReplicaDataProducer<? extends IExecutableSpaceReplicaData>> dataProducers = new ArrayList<ISingleStageReplicaDataProducer<? extends IExecutableSpaceReplicaData>>();

        dataProducers.add(new SpaceTypeReplicaDataProducer(_engine));
        if (_logger.isTraceEnabled())
            _logger.trace(_engine.getReplicationNode() + "created SpaceTypeReplicaDataProducer");
        if (parameters.isCopyNotifyTemplates()) {
            dataProducers.add(new BroadcastNotifyTemplateReplicaProducer(_engine, _requestContext));
            if (_logger.isTraceEnabled())
                _logger.trace(getLogPrefix() + "created BroadcastNotifyTemplateReplicaProducer");
        }
        return dataProducers;

    }

    public IReplicationFilterEntry toFilterEntry(IExecutableSpaceReplicaData data) {
        return data.toFilterEntry(_engine.getTypeManager());
    }

    @Override
    public String getName() {
        return "SpaceBroadcastNotifyReplicaDataProducer";
    }

}
