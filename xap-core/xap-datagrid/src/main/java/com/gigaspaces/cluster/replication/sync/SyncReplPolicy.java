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

/*
 * @(#)SyncReplPolicy.java 1.0  02.09.2004  12:31:44
 */

package com.gigaspaces.cluster.replication.sync;

import com.gigaspaces.cluster.replication.ConsistencyLevel;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;

/**
 * This class contains all configuration information about Synchronize Replication.
 *
 * @author Igor Goldenberg
 * @version 4.0
 **/
@com.gigaspaces.api.InternalApi
public class SyncReplPolicy implements SmartExternalizable {
    private static final long serialVersionUID = 2L;


    final public static String DEFAULT_IP_GROUP = "224.0.0.1";
    final static public int DEFAULT_PORT = 28672;
    final static public int DEFAULT_MULTICAST_TTL = 4;
    final static public long DEFAULT_TODO_QUEUE_TIMEOUT = 1500;
    final static public long DEFAULT_ASYNC_INTERVAL_TO_CHECK = DEFAULT_TODO_QUEUE_TIMEOUT * 3;
    final static public long DEFAULT_RESPONSE_TIMEOUT = DEFAULT_TODO_QUEUE_TIMEOUT + (long) (DEFAULT_TODO_QUEUE_TIMEOUT * 0.3);
    final static public long DEFAULT_ACK_INTERVAL = DEFAULT_TODO_QUEUE_TIMEOUT / 3;
    final static public long DEFAULT_ASYNC_ONE_WAY_HEARTBEAT_INTERVAL = DEFAULT_TODO_QUEUE_TIMEOUT;
    final static public int DEFAULT_UNICAST_MIN_WORK_THREADS = 4;
    final static public int DEFAULT_UNICAST_MAX_WORK_THREADS = 16;
    final static public int DEFAULT_MULTICAST_MIN_WORK_THREADS = 4;
    final static public int DEFAULT_MULTICAST_MAX_WORK_THREADS = 16;
    public final static int DEFAULT_MULTIPLE_OPERATION_CHUNK_SIZE = -1;
    final static public boolean DEFAULT_THROTTLE_WHEN_INACTIVE = true;
    final static public int DEFAULT_MAX_THROTTLE_TP_WHEN_INACTIVE = 50000;
    final static public int DEFAULT_MIN_THROTTLE_TP_WHEN_ACTIVE = 1000;
    final static public long DEFAULT_TARGET_CONSUME_TIMEOUT = 10000;
    final static public ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.ANY;
    public String nodeName;

    /**
     * Interval time for async replication to check whether missed packet available or update last
     * process packet.
     **/
    private long asyncIntervalToCheck = DEFAULT_ASYNC_INTERVAL_TO_CHECK;

    /**
     * Time to wait to get response from appropriate request (Client side peer). After timeout
     * expired and no ack hasn't arrived for this interval, will be thrown Exception.
     **/
    private long responseTimeout = DEFAULT_RESPONSE_TIMEOUT;

    /**
     * MulticastAdaptor: the interval time to Ack a sender while Request packet in process
     **/
    private long ackIntervalTime = DEFAULT_ACK_INTERVAL;

    private String multicastIpGroup = DEFAULT_IP_GROUP;

    private int multicastPort = DEFAULT_PORT;

    private int ttl = DEFAULT_MULTICAST_TTL;

    /**
     * Min/Max threads of working group for Unicast ProtocolAdaptor Dispatcher
     */
    private int unicastMinThreadPoolSize = DEFAULT_UNICAST_MIN_WORK_THREADS;

    private int unicastMaxThreadPoolSize = DEFAULT_UNICAST_MAX_WORK_THREADS;

    /**
     * Min/Max threads of working group for Multicast ProtocolAdaptor Dispatcher
     */
    private int multicastMinThreadPoolSize = DEFAULT_MULTICAST_MIN_WORK_THREADS;

    private int multicastMaxThreadPoolSize = DEFAULT_MULTICAST_MAX_WORK_THREADS;


    /**
     * max time to wait for a line(consumpation) if timeout expired, MissedSequencePacketException
     * will be thrown by ReplicationTodoQueueManager
     **/
    private long todoQueueTimeout = DEFAULT_TODO_QUEUE_TIMEOUT;

    /**
     * HeartBeat interval time to sent heartbeat status from Target to Source space
     */
    private long asyncOneWayHeartBeatInterval = DEFAULT_ASYNC_ONE_WAY_HEARTBEAT_INTERVAL;

    /**
     * if <code>true</code> multicast protocol will be adopted with TCP/IP. Adoptive binary
     * algorithm will find an max event size threshold for multicast packet. if the multicast packet
     * is greater that max threashold this packet packet will be sent by TCP/IP.
     **/
    private boolean isAdaptiveMulticast = true;

    /**
     * holds transaction lock until sync replication will be finished
     */
    private boolean isHoldTxnLockUntilSyncReplication = false;

    /**
     * maximum operations chunk size for clear with null template, writeMultiple, takeMultiple and
     * etc...
     */
    private int multipleOperChunkSize = DEFAULT_MULTIPLE_OPERATION_CHUNK_SIZE;

    private boolean throttleWhenInactive = DEFAULT_THROTTLE_WHEN_INACTIVE;

    private int maxThrottleTPWhenInactive = DEFAULT_MAX_THROTTLE_TP_WHEN_INACTIVE;

    private int minThrottleTPWhenActive = DEFAULT_MIN_THROTTLE_TP_WHEN_ACTIVE;

    /**
     * Maximum time to wait for consumption of replication packet, after which the an error will be
     * logged and the channel will move to asynchronous state until backlog is consumed
     */
    private long targetConsumeTimeout = DEFAULT_TARGET_CONSUME_TIMEOUT;

    private ConsistencyLevel consistencyLevel = DEFAULT_CONSISTENCY_LEVEL;

    private interface DefaultsBitMap {
        int DEFAULT_RESPONSE_TIMEOUT = 1 << 0;
        int DEFAULT_ACK_INTERVAL = 1 << 1;
        int DEFAULT_IP_GROUP = 1 << 2;
        int DEFAULT_PORT = 1 << 3;
        int DEFAULT_MULTICAST_TTL = 1 << 4;
        int DEFAULT_UNICAST_MIN_WORK_THREADS = 1 << 5;
        int DEFAULT_UNICAST_MAX_WORK_THREADS = 1 << 6;
        int DEFAULT_MULTICAST_MIN_WORK_THREADS = 1 << 7;
        int DEFAULT_MULTICAST_MAX_WORK_THREADS = 1 << 8;
        int DEFAULT_TODO_QUEUE_TIMEOUT = 1 << 9;
        int DEFAULT_ASYNC_ONE_WAY_HEARTBEAT_INTERVAL = 1 << 10;
        int DEFAULT_ADAPTIVEMULTICAST = 1 << 11;
        int DEFAULT_HOLD_TXN = 1 << 12;
        int DEFAULT_MULTIPLE_OPERATION_CHUNK_SIZE = 1 << 13;
        int DEFAULT_ASYNC_INTERVAL_TO_CHECK = 1 << 14;
        int DEFAULT_THROTTLE_WHEN_INACTIVE = 1 << 15;
        int DEFAULT_MAX_THROTTLE_TP_WHEN_INACTIVE = 1 << 16;
        int DEFAULT_MIN_THROTTLE_TP_WHEN_ACTIVE = 1 << 17;
        int DEFAULT_TARGET_CONSUME_TIMEOUT = 1 << 18;
        int DEFAULT_CONSISTENCY_LEVEL = 1 << 19;
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(nodeName);

        // create defaults header
        int flags = 0;
        if (responseTimeout == DEFAULT_RESPONSE_TIMEOUT)
            flags |= DefaultsBitMap.DEFAULT_RESPONSE_TIMEOUT;
        if (ackIntervalTime == DEFAULT_ACK_INTERVAL)
            flags |= DefaultsBitMap.DEFAULT_ACK_INTERVAL;
        if (multicastIpGroup.equals(DEFAULT_IP_GROUP))
            flags |= DefaultsBitMap.DEFAULT_IP_GROUP;
        if (multicastPort == DEFAULT_PORT)
            flags |= DefaultsBitMap.DEFAULT_PORT;
        if (ttl == DEFAULT_MULTICAST_TTL)
            flags |= DefaultsBitMap.DEFAULT_MULTICAST_TTL;
        if (unicastMinThreadPoolSize == DEFAULT_UNICAST_MIN_WORK_THREADS)
            flags |= DefaultsBitMap.DEFAULT_UNICAST_MIN_WORK_THREADS;
        if (unicastMaxThreadPoolSize == DEFAULT_UNICAST_MAX_WORK_THREADS)
            flags |= DefaultsBitMap.DEFAULT_UNICAST_MAX_WORK_THREADS;
        if (multicastMinThreadPoolSize == DEFAULT_MULTICAST_MIN_WORK_THREADS)
            flags |= DefaultsBitMap.DEFAULT_MULTICAST_MIN_WORK_THREADS;
        if (multicastMaxThreadPoolSize == DEFAULT_MULTICAST_MAX_WORK_THREADS)
            flags |= DefaultsBitMap.DEFAULT_MULTICAST_MAX_WORK_THREADS;
        if (todoQueueTimeout == DEFAULT_TODO_QUEUE_TIMEOUT)
            flags |= DefaultsBitMap.DEFAULT_TODO_QUEUE_TIMEOUT;
        if (asyncOneWayHeartBeatInterval == DEFAULT_ASYNC_ONE_WAY_HEARTBEAT_INTERVAL)
            flags |= DefaultsBitMap.DEFAULT_ASYNC_ONE_WAY_HEARTBEAT_INTERVAL;
        if (isAdaptiveMulticast == true)
            flags |= DefaultsBitMap.DEFAULT_ADAPTIVEMULTICAST;
        if (isHoldTxnLockUntilSyncReplication == false)
            flags |= DefaultsBitMap.DEFAULT_HOLD_TXN;
        if (multipleOperChunkSize == DEFAULT_MULTIPLE_OPERATION_CHUNK_SIZE)
            flags |= DefaultsBitMap.DEFAULT_MULTIPLE_OPERATION_CHUNK_SIZE;
        if (asyncIntervalToCheck == DEFAULT_ASYNC_INTERVAL_TO_CHECK)
            flags |= DefaultsBitMap.DEFAULT_ASYNC_INTERVAL_TO_CHECK;

        if (throttleWhenInactive == DEFAULT_THROTTLE_WHEN_INACTIVE)
            flags |= DefaultsBitMap.DEFAULT_THROTTLE_WHEN_INACTIVE;
        if (maxThrottleTPWhenInactive == DEFAULT_MAX_THROTTLE_TP_WHEN_INACTIVE)
            flags |= DefaultsBitMap.DEFAULT_MAX_THROTTLE_TP_WHEN_INACTIVE;
        if (minThrottleTPWhenActive == DEFAULT_MIN_THROTTLE_TP_WHEN_ACTIVE)
            flags |= DefaultsBitMap.DEFAULT_MIN_THROTTLE_TP_WHEN_ACTIVE;
        if (targetConsumeTimeout == DEFAULT_TARGET_CONSUME_TIMEOUT)
            flags |= DefaultsBitMap.DEFAULT_TARGET_CONSUME_TIMEOUT;
        if (consistencyLevel == DEFAULT_CONSISTENCY_LEVEL)
            flags |= DefaultsBitMap.DEFAULT_CONSISTENCY_LEVEL;

        out.writeInt(flags);

        // now write all the non defualt ones
        if (responseTimeout != DEFAULT_RESPONSE_TIMEOUT)
            out.writeLong(responseTimeout);
        if (ackIntervalTime != DEFAULT_ACK_INTERVAL)
            out.writeLong(ackIntervalTime);
        if (!multicastIpGroup.equals(DEFAULT_IP_GROUP))
            out.writeUTF(multicastIpGroup);
        if (multicastPort != DEFAULT_PORT)
            out.writeInt(multicastPort);
        if (ttl != DEFAULT_MULTICAST_TTL)
            out.writeInt(ttl);
        if (unicastMinThreadPoolSize != DEFAULT_UNICAST_MIN_WORK_THREADS)
            out.writeInt(unicastMinThreadPoolSize);
        if (unicastMaxThreadPoolSize != DEFAULT_UNICAST_MAX_WORK_THREADS)
            out.writeInt(unicastMaxThreadPoolSize);
        if (multicastMinThreadPoolSize != DEFAULT_MULTICAST_MIN_WORK_THREADS)
            out.writeInt(multicastMinThreadPoolSize);
        if (multicastMaxThreadPoolSize != DEFAULT_MULTICAST_MAX_WORK_THREADS)
            out.writeInt(multicastMaxThreadPoolSize);
        if (todoQueueTimeout != DEFAULT_TODO_QUEUE_TIMEOUT)
            out.writeLong(todoQueueTimeout);
        if (asyncOneWayHeartBeatInterval != DEFAULT_ASYNC_ONE_WAY_HEARTBEAT_INTERVAL)
            out.writeLong(asyncOneWayHeartBeatInterval);
        if (isAdaptiveMulticast != true)
            out.writeBoolean(isAdaptiveMulticast);
        if (isHoldTxnLockUntilSyncReplication != false)
            out.writeBoolean(isHoldTxnLockUntilSyncReplication);
        if (multipleOperChunkSize != DEFAULT_MULTIPLE_OPERATION_CHUNK_SIZE)
            out.writeInt(multipleOperChunkSize);
        if (asyncIntervalToCheck != DEFAULT_ASYNC_INTERVAL_TO_CHECK)
            out.writeLong(asyncIntervalToCheck);
        if (throttleWhenInactive != DEFAULT_THROTTLE_WHEN_INACTIVE)
            out.writeBoolean(throttleWhenInactive);
        if (maxThrottleTPWhenInactive != DEFAULT_MAX_THROTTLE_TP_WHEN_INACTIVE)
            out.writeInt(maxThrottleTPWhenInactive);
        if (minThrottleTPWhenActive != DEFAULT_MIN_THROTTLE_TP_WHEN_ACTIVE)
            out.writeInt(minThrottleTPWhenActive);
        if (targetConsumeTimeout != DEFAULT_TARGET_CONSUME_TIMEOUT)
            out.writeLong(targetConsumeTimeout);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_5_1)) {
            if (consistencyLevel != DEFAULT_CONSISTENCY_LEVEL)
                out.writeByte(consistencyLevel.getCode());
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        nodeName = (String) in.readObject();

        int flags = in.readInt();

        if ((flags & DefaultsBitMap.DEFAULT_RESPONSE_TIMEOUT) == 0) {
            responseTimeout = in.readLong();
        } else {
            responseTimeout = DEFAULT_RESPONSE_TIMEOUT;
        }
        if ((flags & DefaultsBitMap.DEFAULT_ACK_INTERVAL) == 0) {
            ackIntervalTime = in.readLong();
        } else {
            ackIntervalTime = DEFAULT_ACK_INTERVAL;
        }
        if ((flags & DefaultsBitMap.DEFAULT_IP_GROUP) == 0) {
            multicastIpGroup = in.readUTF();
        } else {
            multicastIpGroup = DEFAULT_IP_GROUP;
        }
        if ((flags & DefaultsBitMap.DEFAULT_PORT) == 0) {
            multicastPort = in.readInt();
        } else {
            multicastPort = DEFAULT_PORT;
        }
        if ((flags & DefaultsBitMap.DEFAULT_MULTICAST_TTL) == 0) {
            ttl = in.readInt();
        } else {
            ttl = DEFAULT_MULTICAST_TTL;
        }
        if ((flags & DefaultsBitMap.DEFAULT_UNICAST_MIN_WORK_THREADS) == 0) {
            unicastMinThreadPoolSize = in.readInt();
        } else {
            unicastMinThreadPoolSize = DEFAULT_UNICAST_MIN_WORK_THREADS;
        }
        if ((flags & DefaultsBitMap.DEFAULT_UNICAST_MAX_WORK_THREADS) == 0) {
            unicastMaxThreadPoolSize = in.readInt();
        } else {
            unicastMaxThreadPoolSize = DEFAULT_UNICAST_MAX_WORK_THREADS;
        }
        if ((flags & DefaultsBitMap.DEFAULT_MULTICAST_MIN_WORK_THREADS) == 0) {
            multicastMinThreadPoolSize = in.readInt();
        } else {
            multicastMinThreadPoolSize = DEFAULT_MULTICAST_MIN_WORK_THREADS;
        }
        if ((flags & DefaultsBitMap.DEFAULT_MULTICAST_MAX_WORK_THREADS) == 0) {
            multicastMaxThreadPoolSize = in.readInt();
        } else {
            multicastMaxThreadPoolSize = DEFAULT_MULTICAST_MAX_WORK_THREADS;
        }
        if ((flags & DefaultsBitMap.DEFAULT_TODO_QUEUE_TIMEOUT) == 0) {
            todoQueueTimeout = in.readLong();
        } else {
            todoQueueTimeout = DEFAULT_TODO_QUEUE_TIMEOUT;
        }
        if ((flags & DefaultsBitMap.DEFAULT_ASYNC_ONE_WAY_HEARTBEAT_INTERVAL) == 0) {
            asyncOneWayHeartBeatInterval = in.readLong();
        } else {
            asyncOneWayHeartBeatInterval = DEFAULT_ASYNC_ONE_WAY_HEARTBEAT_INTERVAL;
        }
        if ((flags & DefaultsBitMap.DEFAULT_ADAPTIVEMULTICAST) == 0) {
            isAdaptiveMulticast = in.readBoolean();
        } else {
            isAdaptiveMulticast = true;
        }
        if ((flags & DefaultsBitMap.DEFAULT_HOLD_TXN) == 0) {
            isHoldTxnLockUntilSyncReplication = in.readBoolean();
        } else {
            isHoldTxnLockUntilSyncReplication = false;
        }
        if ((flags & DefaultsBitMap.DEFAULT_MULTIPLE_OPERATION_CHUNK_SIZE) == 0) {
            multipleOperChunkSize = in.readInt();
        } else {
            multipleOperChunkSize = DEFAULT_MULTIPLE_OPERATION_CHUNK_SIZE;
        }
        if ((flags & DefaultsBitMap.DEFAULT_ASYNC_INTERVAL_TO_CHECK) == 0) {
            asyncIntervalToCheck = in.readLong();
        } else {
            asyncIntervalToCheck = DEFAULT_ASYNC_INTERVAL_TO_CHECK;
        }
        if ((flags & DefaultsBitMap.DEFAULT_THROTTLE_WHEN_INACTIVE) == 0) {
            throttleWhenInactive = in.readBoolean();
        } else {
            throttleWhenInactive = DEFAULT_THROTTLE_WHEN_INACTIVE;
        }
        if ((flags & DefaultsBitMap.DEFAULT_MAX_THROTTLE_TP_WHEN_INACTIVE) == 0) {
            maxThrottleTPWhenInactive = in.readInt();
        } else {
            maxThrottleTPWhenInactive = DEFAULT_MAX_THROTTLE_TP_WHEN_INACTIVE;
        }
        if ((flags & DefaultsBitMap.DEFAULT_MIN_THROTTLE_TP_WHEN_ACTIVE) == 0) {
            minThrottleTPWhenActive = in.readInt();
        } else {
            minThrottleTPWhenActive = DEFAULT_MIN_THROTTLE_TP_WHEN_ACTIVE;
        }
        if ((flags & DefaultsBitMap.DEFAULT_TARGET_CONSUME_TIMEOUT) == 0) {
            targetConsumeTimeout = in.readLong();
        } else {
            targetConsumeTimeout = DEFAULT_TARGET_CONSUME_TIMEOUT;
        }
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_5_1)) {
            if ((flags & DefaultsBitMap.DEFAULT_CONSISTENCY_LEVEL) == 0) {
                consistencyLevel = ConsistencyLevel.fromCode(in.readByte());
            } else {
                consistencyLevel = DEFAULT_CONSISTENCY_LEVEL;
            }
        }

    }

    /**
     * Empty constructor for externalizable. Should not be used for other purposes.
     */
    public SyncReplPolicy() {
    }

    /**
     * Constructor.
     *
     * @param nodeNameThe name of node.
     **/
    public SyncReplPolicy(String nodeName) {
        this.nodeName = nodeName;
    }

    /**
     * @return if <code>true</code> hold transaction lock until sync replication will be finished.
     **/
    public boolean isHoldTxnLockUntilSyncReplication() {
        return isHoldTxnLockUntilSyncReplication;
    }

    /**
     * Set sync-replication mode under transactions.
     *
     * @param isHoldTxnLock If <code>true</code> holds transaction lock until sync-replication will
     *                      be finished. NOTE: If true, may cause to the distributed
     *                      sync-replication dead-locks. Use only if special cases.
     **/
    public void setHoldTxnLockUntilSyncReplication(boolean isHoldTxnLock) {
        isHoldTxnLockUntilSyncReplication = isHoldTxnLock;
    }


    public void setAsyncOneWayHeartBeatInterval(long heartBeatIntervalTime) {
        asyncOneWayHeartBeatInterval = heartBeatIntervalTime;
    }

    public long getAsyncOneWayHeartBeatInterval() {
        return asyncOneWayHeartBeatInterval;
    }

    /**
     * @return Returns the asyncIntervalToCheck.
     */
    public long getAsyncIntervalToCheck() {
        return asyncIntervalToCheck;
    }

    public String getMulticastIpGroup() {
        return multicastIpGroup;
    }

    public int getMulticastPort() {
        return multicastPort;
    }

    public int getMulticastTTL() {
        return ttl;
    }

    public void setMulticastTTL(int timeToLeave) {
        ttl = timeToLeave;
    }

    /**
     * @param nodeName The nodeName to set.
     */
    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public String getNodeName() {
        return nodeName;
    }

    /**
     * @return Returns the multicastMaxThreadPoolSize.
     */
    public int getMulticastMaxThreadPoolSize() {
        return multicastMaxThreadPoolSize;
    }

    /**
     * @param multicastMaxThreadPoolSize The multicastMaxThreadPoolSize to set.
     */
    public void setMulticastMaxThreadPoolSize(int multicastMaxThreadPoolSize) {
        this.multicastMaxThreadPoolSize = multicastMaxThreadPoolSize;
    }

    /**
     * @return Returns the multicastMinThreadPoolSize.
     */
    public int getMulticastMinThreadPoolSize() {
        return multicastMinThreadPoolSize;
    }

    /**
     * @param multicastMinThreadPoolSize The multicastMinThreadPoolSize to set.
     */
    public void setMulticastMinThreadPoolSize(int multicastMinThreadPoolSize) {
        this.multicastMinThreadPoolSize = multicastMinThreadPoolSize;
    }

    /**
     * @return Returns the responseTimeout.
     */
    public long getResponseTimeout() {
        return responseTimeout;
    }

    /**
     * @return Returns the todoQueueTimeout.
     */
    public long getTodoQueueTimeout() {
        return todoQueueTimeout;
    }

    /**
     * Set todoQueue timeout. According this time calculate: responseTimeout and ackIntervalTime
     * time. responseTimeout = todoQueueTimeout + (long)( todoQueueTimeout * 0.3 ); ackIntervalTime
     * = todoQueueTimeout / 2; Multicast Sync replication
     *
     * @param todoQueueTimeout The todoQueueTimeout to set.
     */
    public void setTodoQueueTimeout(long todoQueueTimeout) {
        this.todoQueueTimeout = todoQueueTimeout;
        responseTimeout = todoQueueTimeout + (long) (todoQueueTimeout * 0.3);
        ackIntervalTime = todoQueueTimeout / 3;

        /** source check heartbeat status from target space every interval time */
        asyncIntervalToCheck = todoQueueTimeout * 3;

        /** interval time sending heartbeat status from target to source every interval time */
        asyncOneWayHeartBeatInterval = todoQueueTimeout;
    }

    /**
     * if <code>true</code> multicast protocol will be adopted with TCP/IP. Adoptive binary
     * algorithm will find an max event size threshold for multicast packet. if the multicast packet
     * is greater that max threashold this packet packet will be sent by TCP/IP.
     *
     * @param isAdaptive isAdaptive multicast.
     **/
    public void setAdaptiveMulticast(boolean isAdaptive) {
        isAdaptiveMulticast = isAdaptive;
    }

    public boolean isAdaptiveMulticast() {
        return isAdaptiveMulticast;
    }

    /**
     * @return Returns the unicastMaxThreadPoolSize.
     */
    public int getUnicastMaxThreadPoolSize() {
        return unicastMaxThreadPoolSize;
    }

    /**
     * @param unicastMaxThreadPoolSize The unicastMaxThreadPoolSize to set.
     */
    public void setUnicastMaxThreadPoolSize(int unicastMaxThreadPoolSize) {
        this.unicastMaxThreadPoolSize = unicastMaxThreadPoolSize;
    }

    /**
     * @return Returns the unicastMinThreadPoolSize.
     */
    public int getUnicastMinThreadPoolSize() {
        return unicastMinThreadPoolSize;
    }

    /**
     * @param unicastMinThreadPoolSize The unicastMinThreadPoolSize to set.
     **/
    public void setUnicastMinThreadPoolSize(int unicastMinThreadPoolSize) {
        this.unicastMinThreadPoolSize = unicastMinThreadPoolSize;
    }

    /**
     * @return The ackIntervalTime.
     */
    public long getAckIntervalTime() {
        return ackIntervalTime;
    }


    /**
     * @param multicastIpGroup The multicastIpGroup to set.
     */
    public void setMulticastIpGroup(String multicastIpGroup) {
        this.multicastIpGroup = multicastIpGroup;
    }

    /**
     * @param multicastPort The multicastPort to set.
     */
    public void setMulticastPort(int multicastPort) {
        this.multicastPort = multicastPort;
    }

    /**
     * @param chunkSize maximum operations chunk size, or -1 if chunk mechanism disabled.
     */
    public void setMultipleOperationChunkSize(int chunkSmultipleOperChunkSizeize) {
        this.multipleOperChunkSize = chunkSmultipleOperChunkSizeize;
    }

    /**
     * @return the defined maximum chunk for multiple operations, or -1 if chunk mechanism disabled.
     */
    public int getMultipleOperationChunkSize() {
        return multipleOperChunkSize;
    }


    public boolean isThrottleWhenInactive() {
        return throttleWhenInactive;
    }

    public void setThrottleWhenInactive(boolean throttleWhenInactive) {
        this.throttleWhenInactive = throttleWhenInactive;
    }

    public int getMaxThrottleTPWhenInactive() {
        return maxThrottleTPWhenInactive;
    }

    public void setMaxThrottleTPWhenInactive(int maxThrottleTPWhenInactive) {
        this.maxThrottleTPWhenInactive = maxThrottleTPWhenInactive;
    }

    public int getMinThrottleTPWhenActive() {
        return minThrottleTPWhenActive;
    }

    public void setMinThrottleTPWhenActive(int minThrottleTPWhenActive) {
        this.minThrottleTPWhenActive = minThrottleTPWhenActive;
    }

    public long getTargetConsumeTimeout() {
        return targetConsumeTimeout;
    }

    public void setTargetConsumeTimeout(long targetConsumeTimeout) {
        this.targetConsumeTimeout = targetConsumeTimeout;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    /**
     * Debug dump info.
     */
    public String getDump() {
        StringBuilder dumpInfo = new StringBuilder();
        dumpInfo.append("\n------------- SYNC-REPLICATION DUMP [" + nodeName
                + "] ------------------");

        try {
            Field[] fields = getClass().getDeclaredFields();
            for (int i = 0; i < fields.length; i++) {
                dumpInfo.append("\n" + fields[i].getName() + " : " + fields[i].get(this));
            }
        } catch (Exception ex) {
            return ex.toString();
        }

        dumpInfo.append("\n------------- SYNC-REPLICATION DUMP [" + nodeName
                + "] ------------------");

        return dumpInfo.toString();
    }

    public String toString() {
        StringBuilder strBuffer =
                new StringBuilder("\n------------Sync Replication Policy---------\n");
        strBuffer.append("Async Interval To Check -\t" + asyncIntervalToCheck + "\n");
        strBuffer.append("Node Name -\t" + nodeName + "\n");
        strBuffer.append("Response Timeout -\t" + responseTimeout + "\n");
        strBuffer.append("ack Interval Time -\t" + ackIntervalTime + "\n");
        strBuffer.append("Multicast IP Group -\t" + multicastIpGroup + "\n");
        strBuffer.append("Multicast Port -\t" + multicastPort + "\n");
        strBuffer.append("ttl -\t" + ttl + "\n");
        strBuffer.append("Unicast Min Thread Pool Size -\t" + unicastMinThreadPoolSize + "\n");
        strBuffer.append("Unicast Max Thread Pool Size -\t" + unicastMaxThreadPoolSize + "\n");
        strBuffer.append("Multicast Min Thread Pool Size -\t" + multicastMinThreadPoolSize + "\n");
        strBuffer.append("Multicast Max Thread Pool Size -\t" + multicastMaxThreadPoolSize + "\n");
        strBuffer.append("Todo Queue Timeout -\t" + todoQueueTimeout + "\n");
        strBuffer.append("Async One Way Heart Beat Interval -\t" + asyncOneWayHeartBeatInterval + "\n");
        strBuffer.append("Is Adaptive Multicast -\t" + isAdaptiveMulticast + "\n");
        strBuffer.append("Is Hold Txn Lock Until Sync Replication -\t" + isHoldTxnLockUntilSyncReplication + "\n");
        strBuffer.append("Multiple Operations chunk size -\t" + multipleOperChunkSize + "\n");
        strBuffer.append("Throttle when inactive -\t" + throttleWhenInactive + "\n");
        strBuffer.append("Max Throttle throughput when inactive -\t" + maxThrottleTPWhenInactive + "\n");
        strBuffer.append("Min Throttle throughput when active -\t" + minThrottleTPWhenActive + "\n");
        strBuffer.append("Target consume timeout -\t" + targetConsumeTimeout + "\n");
        strBuffer.append("Consistency Level -\t" + consistencyLevel + "\n");

        return strBuffer.toString();
    }
}
