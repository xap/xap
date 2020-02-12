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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class ServerIteratorManager {
    //TODO add logger
    private final Map<UUID, ServerIteratorInfo> serverIteratorInfoMap = new ConcurrentHashMap<>();

    public ServerIteratorManager() {
    }

    public ServerIteratorInfo getOrCreateServerIteratorInfo(ServerIteratorRequestInfo serverIteratorRequestInfo){
        if(serverIteratorRequestInfo == null)
            return null;
        UUID uuid = serverIteratorRequestInfo.getUuid();
        boolean containsUuid = serverIteratorInfoMap.containsKey(uuid);
        boolean firstTime = serverIteratorRequestInfo.isFirstTime();
        if(containsUuid && !firstTime) {
            return serverIteratorInfoMap.get(uuid);
        }
        if(containsUuid && firstTime) {
            serverIteratorInfoMap.remove(uuid);
        }
        ServerIteratorInfo result = new ServerIteratorInfo(serverIteratorRequestInfo.getUuid(), serverIteratorRequestInfo.getBatchSize(), serverIteratorRequestInfo.getLease());
        serverIteratorInfoMap.put(uuid, result);
        return result;
    }
}
