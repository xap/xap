package com.gigaspaces.internal.server.space.iterator;

import com.gigaspaces.logger.Constants;

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

    public ServerIteratorInfo getOrCreateServerIteratorInfo(ServerIteratorRequestInfo serverIteratorRequestInfo){
        UUID uuid = serverIteratorRequestInfo.getUuid();
        boolean containsUuid = serverIteratorInfoMap.containsKey(uuid);
        boolean firstTime = serverIteratorRequestInfo.isFirstTime();
        if(containsUuid && !firstTime) {
            return serverIteratorInfoMap.get(uuid);
        }
        if(containsUuid && firstTime) {
            throw new IllegalStateException("Space iterator " + uuid + " was already created in server");
        }
        if(!containsUuid && !firstTime){
            throw new IllegalStateException("Space iterator " + uuid + " is not found in server");
        } //TODO simplify condition flow
        ServerIteratorInfo result = new ServerIteratorInfo(serverIteratorRequestInfo.getUuid(), serverIteratorRequestInfo.getBatchSize(), serverIteratorRequestInfo.getLease());
        serverIteratorInfoMap.put(uuid, result);
        if(_logger.isLoggable(Level.FINE))
            _logger.fine("Space iterator " + uuid + " was created in server");
        return result;
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
