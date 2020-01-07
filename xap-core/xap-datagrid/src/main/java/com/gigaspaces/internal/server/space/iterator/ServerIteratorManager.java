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
