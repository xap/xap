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

package com.gigaspaces.internal.sync.mirror;

import com.gigaspaces.cluster.replication.async.mirror.MirrorStatisticsImpl;
import com.gigaspaces.internal.cluster.node.IReplicationInBatchContext;
import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInDataTypeCreatedHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInDataTypeIndexAddedHandler;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.space.requests.AddTypeIndexesRequestInfo;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.sync.AddIndexDataImpl;
import com.gigaspaces.sync.IntroduceTypeDataImpl;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 8.0.5
 */
@com.gigaspaces.api.InternalApi
public class MirrorReplicationMetadataEventHandler extends MirrorReplicationInHandler implements
        IReplicationInDataTypeCreatedHandler,
        IReplicationInDataTypeIndexAddedHandler {

    private static final Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_REPLICATION);

    public MirrorReplicationMetadataEventHandler(MirrorBulkExecutor bulkExecutor,
                                                 MirrorStatisticsImpl operationStatisticsHandler) {
        super(bulkExecutor, operationStatisticsHandler);
    }

    @Override
    public void inDataTypeIntroduce(IReplicationInContext context, ITypeDesc typeDesc)
            throws Exception {
        IReplicationInBatchContext batchContext = (IReplicationInBatchContext) context;
        execute(batchContext, null);

        try {
            getTypeManager().addTypeDesc(typeDesc);
            _syncEndpoint.onIntroduceType(new IntroduceTypeDataImpl(typeDesc));
            batchContext.currentConsumed();
        } catch (Exception e) {
            if (_logger.isErrorEnabled())
                _logger.error("Conflict in type introduction.", e);
        }
    }

    @Override
    public void inDataTypeAddIndex(IReplicationInContext context, AddTypeIndexesRequestInfo requestInfo)
            throws Exception {
        IReplicationInBatchContext batchContext = (IReplicationInBatchContext) context;
        execute(batchContext, null);

        try {
            getTypeManager().addIndexes(requestInfo.getTypeName(), requestInfo.getIndexes());
            _syncEndpoint.onAddIndex(new AddIndexDataImpl(requestInfo.getTypeName(), requestInfo.getIndexes()));
        } catch (Exception e) {
            if (_logger.isWarnEnabled()) {
                _logger.warn("Error adding new index to type [" + requestInfo.getTypeName() + "].", e);
            }
        }
        batchContext.currentConsumed();
    }
}
