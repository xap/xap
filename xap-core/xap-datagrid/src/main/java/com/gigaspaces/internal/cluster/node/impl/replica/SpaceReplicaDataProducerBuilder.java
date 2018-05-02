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

import com.gigaspaces.internal.cluster.node.replica.ISpaceCopyReplicaParameters;
import com.gigaspaces.internal.cluster.node.replica.SpaceCopyReplicaParameters;
import com.gigaspaces.internal.server.space.SpaceEngine;

@com.gigaspaces.api.InternalApi
public class SpaceReplicaDataProducerBuilder
        implements ISpaceReplicaDataProducerBuilder<IExecutableSpaceReplicaData> {

    private final SpaceEngine _engine;

    public SpaceReplicaDataProducerBuilder(SpaceEngine engine) {
        _engine = engine;
    }

    public ISpaceReplicaDataProducer<IExecutableSpaceReplicaData> createProducer(
            ISpaceCopyReplicaParameters parameters, Object requestContext) {
        SpaceCopyReplicaParameters copyParameters = (SpaceCopyReplicaParameters) parameters;
        switch (copyParameters.getReplicaType()) {
            case SYNCRONIZE:
                return new SynchronizeReplicaDataProducer(_engine, copyParameters, requestContext);
            case COPY:
                return new SpaceCopyReplicaDataProducer(_engine, copyParameters, requestContext);
            case BROADCAST_NOTIFY_TEMPLATES_COPY:
                return new SpaceBroadcastNotifyReplicaDataProducer(_engine,
                        copyParameters, requestContext);
            default:
                throw new IllegalArgumentException("Illegal replica type "
                        + copyParameters.getReplicaType());
        }
    }
    public SpaceEngine getSpaceEngine()
    {
        return _engine;
    }

}
