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
package com.gigaspaces.internal.server.space.iterator;

import com.gigaspaces.logger.Constants;
import com.j_spaces.core.ServerIteratorAnswerHolder;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ServerIteratorManager {
    private final Logger _logger;
    private final Map<UUID, ServerIteratorInfo> serverIteratorInfoMap = new ConcurrentHashMap<>();

    public ServerIteratorManager() {
        _logger = Logger.getLogger(Constants.LOGGER__SERVER_GSITERATOR);
    }

    public ServerIteratorInfo getOrCreateServerIteratorInfo(ServerIteratorRequestInfo serverIteratorRequestInfo) throws IllegalStateException{
        UUID uuid = serverIteratorRequestInfo.getUuid();
        boolean containsUuid = serverIteratorInfoMap.containsKey(uuid);
        boolean firstTime = serverIteratorRequestInfo.isFirstTime();
        boolean createNew = !containsUuid && firstTime;
        boolean foundActive = containsUuid && !firstTime;
        if(createNew){
            ServerIteratorInfo result = new ServerIteratorInfo(serverIteratorRequestInfo.getUuid(), serverIteratorRequestInfo.getBatchSize(), serverIteratorRequestInfo.getLease());
            serverIteratorInfoMap.put(uuid, result);
            if(_logger.isLoggable(Level.FINE))
                _logger.fine("Space iterator " + uuid + " was created in server");
            return result;
        }
        if(foundActive) {
            return serverIteratorInfoMap.get(uuid);
        }
        if(containsUuid && firstTime) {
            throw new IllegalStateException("Space iterator " + uuid + " was already created in server");
        }
        throw new IllegalStateException("Requesting batch number " + serverIteratorRequestInfo.getRequestedBatchNumber() + " for space iterator " + uuid + " , which was not found in server");
    }

    public void closeServerIterator(UUID uuid){
        ServerIteratorInfo serverIteratorInfo = serverIteratorInfoMap.remove(uuid);
        if(serverIteratorInfo != null){
            if(_logger.isLoggable(Level.FINE))
                _logger.fine("Space iterator " + uuid + " was closed in server");
            serverIteratorInfo.setStatus(ServerIteratorStatus.CLOSED);
        }
    }
}
