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

package com.gigaspaces.internal.cluster.node.impl.handlers;

import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInDataTypeCreatedHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInDataTypeIndexAddedHandler;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.space.requests.AddTypeIndexesRequestInfo;
import com.gigaspaces.logger.Constants;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Niv Ingberg
 * @since 8.0.5
 */
@com.gigaspaces.api.InternalApi
public class SpaceReplicationMetadataEventHandler implements
        IReplicationInDataTypeCreatedHandler,
        IReplicationInDataTypeIndexAddedHandler {
    private static final Logger _logger = Logger.getLogger(Constants.LOGGER_REPLICATION);
    private final SpaceTypeManager _typeManager;
    private final SpaceEngine _spaceEngine;


    public SpaceReplicationMetadataEventHandler(SpaceTypeManager typeManager, SpaceEngine spaceEngine) {
        this._typeManager = typeManager;
        _spaceEngine = spaceEngine;
    }

    @Override
    public void inDataTypeIntroduce(IReplicationInContext context, ITypeDesc typeDesc)
            throws Exception {
        try {
            _typeManager.addTypeDesc(typeDesc);
            if (_spaceEngine.getCacheManager().isBlobStoreCachePolicy()) //need to be stored in case blobStore recovery will be used
                _spaceEngine.getCacheManager().getStorageAdapter().introduceDataType(typeDesc);

        } catch (Exception e) {
            if (_logger.isLoggable(Level.SEVERE))
                _logger.log(Level.SEVERE, "Conflict in type introduction.", e);
        }
    }

    @Override
    public void inDataTypeAddIndex(IReplicationInContext context, AddTypeIndexesRequestInfo requestInfo)
            throws Exception {
        try {
            _typeManager.addIndexes(requestInfo.getTypeName(), requestInfo.getIndexes());
            if (_spaceEngine.getCacheManager().isBlobStoreCachePolicy()) //need to be stored in case blobStore recovery will be used
                _spaceEngine.getCacheManager().getStorageAdapter().addIndexes(requestInfo.getTypeName(), requestInfo.getIndexes());
        } catch (Exception e) {
            if (_logger.isLoggable(Level.SEVERE))
                _logger.log(Level.SEVERE, "Error adding new index to type [" + requestInfo.getTypeName() + "].", e);
        }
    }
}
