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
package com.gigaspaces.proxy_pool;

import com.gigaspaces.internal.utils.GsEnv;
import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.driver.GConnection;
import com.j_spaces.kernel.SystemProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Meshi Ruff
 * @since 16.0.
 */

public class SpaceProxyPool {
    private static final Logger logger = LoggerFactory.getLogger(SpaceProxyPool.class.getName());
    TreeMap<SessionIdEntry, GConnection> treeMap = new TreeMap<>();
    HashMap<String, HashValue> hashMap = new HashMap<>();
    private final int capacity;
    private final long expirationTime;
    private final long waitingTimeout;
    private final int CAPACITY_DEFAULT = 50;
    private final int EXPIRATION_DEFAULT = 15 * 60 * 1000;
    private final int WAITING_TIMEOUT_DEFAULT = 3 * 60 * 1000;
    private int proxyCounter = 0;
    final private ReentrantLock lock = new ReentrantLock();
    final private Condition conditionCreatingConnection = lock.newCondition();
    final private Condition conditionWaitingForCapacity = lock.newCondition();
    final private SpaceProxyPoolFactory spaceProxyPoolFactory;

    public SpaceProxyPool(SpaceProxyPoolFactory spaceProxyPoolFactory) {
        capacity = GsEnv.propertyInt(SystemProperties.PROXY_POOL_CAPACITY).get(CAPACITY_DEFAULT);
        expirationTime = GsEnv.propertyInt(SystemProperties.PROXY_POOL_EXPIRATION_TIME).get(EXPIRATION_DEFAULT);
        waitingTimeout = GsEnv.propertyInt(SystemProperties.PROXY_POOL_WAITING_TIMEOUT).get(WAITING_TIMEOUT_DEFAULT);
        this.spaceProxyPoolFactory = spaceProxyPoolFactory;

    }

    /**
     * Constructor for tests.
     **/
    public SpaceProxyPool(int capacity, long expirationTime, long waitingTimeout, SpaceProxyPoolFactory spaceProxyPoolFactory) {
        this.capacity = capacity;
        this.expirationTime = expirationTime;
        this.waitingTimeout = waitingTimeout;
        this.spaceProxyPoolFactory = spaceProxyPoolFactory;

    }


    /**
     * Get connection for specific sessionId.
     * If sessionId does not exist, create new connection
     *
     * @param ijSpace
     * @param sessionId
     * @param props
     * @return gConnection
     * @throws SQLException
     * @throws TimeoutException        if timeout reached while waiting for connection
     * @throws SpaceProxyPoolException if the session expired while getting the connection
     */
    public GConnection getOrCreate(IJSpace ijSpace, String sessionId, Properties props) throws SQLException, TimeoutException, SpaceProxyPoolException, InterruptedException {
        String spaceName = ijSpace.getName();
        lock.lock();
        try {
            HashValue hashValue = hashMap.get(sessionId);
            // session exists
            if (hashValue != null) {
                Long timestamp = hashValue.getHashMapIdle().get(spaceName);

                //ijSpace exists and is idle
                if (timestamp != null) {
                    return getConnectionIfSessionAndSpaceExist(hashValue, timestamp, sessionId, spaceName);
                }
                //ijSpace not exist in idle map
                else {
                    BusyValue busyValue = hashValue.getHashMapBusy().get(spaceName);

                    //ijSpace exists in busy map
                    if (busyValue != null) {
                        return getConnectionIfExistInBusy(busyValue, sessionId);
                    }
                    //ijSpace doesn't exist in both maps
                    else {
                        return getConnectionIfNotExistInBothMaps(hashValue, sessionId, spaceName, ijSpace, props);
                    }
                }
            }
            //sessionId doesn't exist
            else {
                HashValue newHashValue = new HashValue();
                hashMap.put(sessionId, newHashValue);
                return getConnectionIfNotExistInBothMaps(newHashValue, sessionId, spaceName, ijSpace, props);

            }

        } finally {
            lock.unlock();
        }
    }

    /**
     * Put back the connection after usage
     *
     * @param gConnection
     * @param sessionId
     * @param spaceName
     * @throws SpaceProxyPoolException if the session expired while getting the connection
     */
    public void putBack(GConnection gConnection, String sessionId, String spaceName) throws SpaceProxyPoolException {
        lock.lock();
        try {
            HashValue hashValue = hashMap.get(sessionId);
            if (hashValue != null) {
                BusyValue busyValue = hashValue.getHashMapBusy().get(spaceName);
                int refCount = busyValue.getRefCount() - 1;

                //no refs on connection
                if (refCount == 0) {
                    long currentTime = System.currentTimeMillis();
                    SessionIdEntry sessionIdEntry = new SessionIdEntry(currentTime, sessionId, spaceName);
                    treeMap.put(sessionIdEntry, gConnection);
                    hashMap.get(sessionId).getHashMapBusy().remove(spaceName);
                    hashMap.get(sessionId).getHashMapIdle().put(spaceName, currentTime);
                } else {
                    hashValue.getHashMapBusy().get(spaceName).setRefCount(refCount);
                }
            }
            //if session doesn't exist
            else {
                throw new SpaceProxyPoolException("The current session is expired");
            }

        } finally {
            lock.unlock();
        }
    }

    private GConnection getConnectionIfSessionAndSpaceExist(HashValue hashValue, Long timestamp, String sessionId, String spaceName) {
        GConnection removedConnection = treeMap.remove(new SessionIdEntry(timestamp, sessionId, spaceName));
        // removedConnection successes
        if (removedConnection != null) {
            hashValue.getHashMapIdle().remove(spaceName);
            hashValue.getHashMapBusy().put(spaceName, new BusyValue(1, removedConnection));
            return removedConnection;
        } else {
            logger.warn("Proxy pool is not consistent");
            return null;
        }
    }

    private GConnection getConnectionIfExistInBusy(BusyValue busyValue, String sessionId) throws TimeoutException, SpaceProxyPoolException, InterruptedException {
        busyValue.setRefCount(busyValue.getRefCount() + 1);
        GConnection connection = busyValue.getConnection();
        if (connection != null) {
            return connection;
        } else {

            long end = System.currentTimeMillis() + waitingTimeout;
            while (busyValue.getConnection() == null) {
                long timeToWait = end - System.currentTimeMillis();
                if (timeToWait <= 0) {
                    busyValue.setRefCount(busyValue.getRefCount() - 1);
                    throw new TimeoutException("Timeout reached for waiting to space connection");
                }
                conditionCreatingConnection.await(timeToWait, TimeUnit.MILLISECONDS);

                if (hashMap.get(sessionId) == null) {
                    throw new SpaceProxyPoolException("The current session is expired");
                }
            }

            return busyValue.getConnection();

        }
    }

    private GConnection getConnectionIfNotExistInBothMaps(HashValue hashValue, String sessionId, String spaceName, IJSpace space, Properties props) throws TimeoutException, SQLException, SpaceProxyPoolException, InterruptedException {
        fullPoolLRU(hashValue, sessionId, spaceName);

        if (hashMap.get(sessionId) == null)
            throw new SpaceProxyPoolException("The current session is expired");
        if (hashValue.getHashMapBusy().containsKey(spaceName))
            return getConnectionIfExistInBusy(hashValue.getHashMapBusy().get(spaceName), sessionId);
        Long timestampIdle = hashValue.getHashMapIdle().get(spaceName);
        if (timestampIdle != null)
            return getConnectionIfSessionAndSpaceExist(hashValue, timestampIdle, sessionId, spaceName);

        hashValue.getHashMapBusy().put(spaceName, new BusyValue(1, null));
        proxyCounter++;

        GConnection clonedProxy = null;
        lock.unlock();
        try {
            clonedProxy = spaceProxyPoolFactory.createClonedProxy(space, props);
        } finally {
            lock.lock();
        }
        HashValue hashValueAfterCreatingProxy = hashMap.get(sessionId);
        if (hashValueAfterCreatingProxy == null) {
            throw new SpaceProxyPoolException("The current session is expired");
        }

        BusyValue busyValue = hashValueAfterCreatingProxy.getHashMapBusy().get(spaceName);
        if (busyValue == null) {
            throw new SpaceProxyPoolException("Space with name" + spaceName + " is not found in the Connection Pool");
        }

        busyValue.setConnection(clonedProxy);
        conditionCreatingConnection.signalAll();
        return clonedProxy;
    }

    /**
     * If proxy counter reaches the capacity, try to remove the oldest connection map to make room for a new one
     *
     * @throws TimeoutException
     * @throws InterruptedException
     * @throws SpaceProxyPoolException
     */
    private void fullPoolLRU(HashValue inputHashValue, String inputSessionId, String inputSpaceName) throws TimeoutException, InterruptedException, SpaceProxyPoolException {
        Map.Entry<SessionIdEntry, GConnection> entry = null;
        long end = System.currentTimeMillis() + waitingTimeout;

        while (proxyCounter >= capacity) {
            entry = treeMap.pollFirstEntry();
            if (entry == null) {
                long timeToWait = end - System.currentTimeMillis();
                if (timeToWait > 0) {
                    conditionWaitingForCapacity.await(timeToWait, TimeUnit.MILLISECONDS);
                    if (hashMap.get(inputSessionId) == null) {
                        throw new SpaceProxyPoolException("The current session is expired");
                    }

                    if (inputHashValue.getHashMapBusy().containsKey(inputSpaceName) || inputHashValue.getHashMapIdle().containsKey(inputSpaceName))
                        return;
                } else {
                    throw new TimeoutException("Timed out while waiting for capacity");
                }
            }
            if (entry != null) {
                String removedSessionId = entry.getKey().getSessionId();
                HashValue hashValue = hashMap.get(removedSessionId);
                proxyCounter--;
                hashValue.getHashMapIdle().remove(entry.getKey().getSpaceName());
            }
        }
    }

    /**
     * Evict the connections which pass the expiration time.
     * Runs periodically
     *
     * @throws SQLException
     */
    public void evict() throws SQLException {
        lock.lock();
        int evicted = 0;
        try {
            if (proxyCounter > 0) {
                Iterator<SessionIdEntry> iterator = treeMap.keySet().iterator();
                long currentTime = System.currentTimeMillis();
                while (iterator.hasNext()) {
                    SessionIdEntry next = iterator.next();
                    if (currentTime - next.getTime() > expirationTime) {
                        iterator.remove();
                        HashMap<String, Long> hashMapIdle = hashMap.get(next.getSessionId()).getHashMapIdle();
                        hashMapIdle.remove(next.getSpaceName(), next.getTime());
                        proxyCounter--;
                        evicted++;
                    } else {
                        break;
                    }
                }
                if (evicted > 0) {
                    if (logger.isDebugEnabled())
                        logger.debug(evicted + " connection to space were evicted from the Connection Pool");
                    conditionWaitingForCapacity.signalAll();
                }
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * When a session is logged out, remove all connections of this session
     *
     * @param sessionId
     */
    public void deleteLoggedOutSession(String sessionId) {
        lock.lock();
        try {
            HashValue remove = hashMap.remove(sessionId);
            if (remove != null) {
                Set<Map.Entry<String, Long>> idleConnections = remove.getHashMapIdle().entrySet();

                for (Map.Entry<String, Long> entry : idleConnections) {
                    treeMap.remove(new SessionIdEntry(entry.getValue(), sessionId, entry.getKey()));
                    proxyCounter--;

                }
                int numberOfBusy = remove.getHashMapBusy().size();
                proxyCounter = proxyCounter - numberOfBusy;
                conditionWaitingForCapacity.signalAll();
                conditionCreatingConnection.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    public long getExpirationTime() {

        return expirationTime;
    }

    public int getProxyCounter() {
        lock.lock();
        try {

            return proxyCounter;
        } finally {
            lock.unlock();
        }
    }


    public boolean isSessionIdExists(String sessionId) {

        lock.lock();
        try {
            HashValue hashValue = hashMap.get(sessionId);
            if (hashValue != null) {
                int size = hashValue.getHashMapBusy().size() + hashValue.getHashMapIdle().size();
                return size > 0;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    public int getHashMapSize() {
        lock.lock();
        try {
            return hashMap.size();
        } finally {
            lock.unlock();
        }
    }

}


