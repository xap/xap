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

package com.gigaspaces.internal.cluster.node.impl.processlog.globalorder;

import com.gigaspaces.cluster.replication.IncomingReplicationOutOfSyncException;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.backlog.IIdleStateData;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderBacklogHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderDeletedBacklogPacket;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilterCallback;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.IHandshakeIteration;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataConsumer;
import com.gigaspaces.internal.cluster.node.impl.processlog.AbstractSingleFileTargetProcessLog;
import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessLogHandshakeResponse;
import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessResult;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandler;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationTargetProcessLog;
import com.gigaspaces.internal.cluster.node.impl.processlog.ProcessLogState;
import com.j_spaces.core.exception.ClosedResourceException;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;



public abstract class AbstractGlobalOrderTargetProcessLog
        extends AbstractSingleFileTargetProcessLog
        implements IReplicationTargetProcessLog {
    protected final Object _lifeCycleLock = new Object();
    protected final Lock _lock = new ReentrantLock();
    protected long _lastProcessedKey;
    private ProcessLogState _state = ProcessLogState.OPEN;
    private boolean _firstHandshakeForTarget;

    public AbstractGlobalOrderTargetProcessLog(
            IReplicationPacketDataConsumer dataConsumer,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            IReplicationInFacade replicationInFacade, String name,
            String groupName, String sourceLookupName, long lastProcessedKey,
            boolean firstHandshakeForTarget, IReplicationGroupHistory groupHistory) {
        super(dataConsumer,
                exceptionHandler,
                replicationInFacade,
                name,
                groupName, sourceLookupName,
                groupHistory);
        _lastProcessedKey = lastProcessedKey;
        _firstHandshakeForTarget = firstHandshakeForTarget;
    }

    public long getLastProcessedKey() {
        _lock.lock();
        try {
            return _lastProcessedKey;
        } finally {
            _lock.unlock();
        }
    }

    public boolean isFirstHandshakeForTarget() {
        _lock.lock();
        try {
            return _firstHandshakeForTarget;
        } finally {
            _lock.unlock();
        }
    }

    public GlobalOrderProcessLogHandshakeResponse performHandshake(
            String memberName, IBacklogHandshakeRequest handshakeRequest) throws IncomingReplicationOutOfSyncException {
        synchronized (_lifeCycleLock) {
            if (!isOpen())
                throw new ClosedResourceException("Process log is closed");
            GlobalOrderBacklogHandshakeRequest typedHandshakeRequest = (GlobalOrderBacklogHandshakeRequest) handshakeRequest;
            // Handle first ever received handshake (target first connection,
            // can be
            // a restart of target as well)
            if (_firstHandshakeForTarget) {
                _firstHandshakeForTarget = false;
                _lastProcessedKey = typedHandshakeRequest.getLastConfirmedKey();
                return new GlobalOrderProcessLogHandshakeResponse(_lastProcessedKey);
            }
            // Handle first receive handshake from this source
            else if (handshakeRequest.isFirstHandshake() && canResetState()) {
                // In this case we always override local key since from the
                // source
                // perspective it is the first
                // connection (source is new)
                _lastProcessedKey = typedHandshakeRequest.getLastConfirmedKey();
                return new GlobalOrderProcessLogHandshakeResponse(_lastProcessedKey);
            }
            // This is a handshake probably due to disconnection of this
            // channel,
            // but the source is still the same source
            else if (typedHandshakeRequest.getLastConfirmedKey() <= _lastProcessedKey) {
                return new GlobalOrderProcessLogHandshakeResponse(_lastProcessedKey);
            } else {
                // Here we have replication sync error, the source
                // thinks it is on a newer key then target
                throw new IncomingReplicationOutOfSyncException("Replication out of sync, received last confirmed key "
                        + typedHandshakeRequest.getLastConfirmedKey()
                        + " while last processed key is "
                        + _lastProcessedKey);
            }
        }
    }

    protected boolean canResetState() {
        return true;
    }

    protected void throwClosedException() {
        throw new ClosedResourceException("Process log is closed");
    }

    protected void filterDuplicate(List<IReplicationOrderedPacket> packets) {
        // If source is in async mode (due to disconnection, recovery or temp
        // error at target, i.e out of mem)
        // duplicate packets may arrive at the intermediate stage of the source
        // moving from async to sync
        for (Iterator<IReplicationOrderedPacket> iterator = packets.iterator(); iterator.hasNext(); ) {
            IReplicationOrderedPacket packet = iterator.next();
            if (filterDuplicate(packet))
                iterator.remove();
            else
                break;
        }
    }

    protected boolean filterDuplicate(IReplicationOrderedPacket packet) {
        // If source is in async mode (due to disconnection, recovery or temp
        // error at target, i.e out of mem)
        // duplicate packets may arrive at the intermediate stage of the source
        // moving from async to sync
        return packet.getEndKey() <= _lastProcessedKey;
    }

    public boolean close(long time, TimeUnit unit) throws InterruptedException {
        // Flush to main memory
        synchronized (_lifeCycleLock) {
            _state = ProcessLogState.CLOSING;
        }

        if (!_lock.tryLock(time, unit)) {
            // Flush to main memory
            synchronized (_lifeCycleLock) {
                _state = ProcessLogState.CLOSED;
            }
            return false;
        }
        try {
            _state = ProcessLogState.CLOSED;
            onClose();

            return true;
        } finally {
            getExceptionHandler().close();
            _lock.unlock();
        }
    }

    protected boolean isOpen() {
        return _state == ProcessLogState.OPEN;
    }

    protected boolean isClosed() {
        return _state == ProcessLogState.CLOSED;
    }

    protected abstract void onClose();

    public IProcessLogHandshakeResponse resync(IBacklogHandshakeRequest handshakeRequest) {
        _lock.lock();
        try {
            if (!isOpen())
                throw new ClosedResourceException("Process log is closed");
            GlobalOrderBacklogHandshakeRequest typedHandshakeRequest = (GlobalOrderBacklogHandshakeRequest) handshakeRequest;
            _firstHandshakeForTarget = false;
            _lastProcessedKey = typedHandshakeRequest.getLastConfirmedKey();
            return new GlobalOrderProcessLogHandshakeResponse(_lastProcessedKey);
        } finally {
            _lock.unlock();
        }
    }

    public void processHandshakeIteration(String sourceMemberName,
                                          IHandshakeIteration handshakeIteration) {
        throw new UnsupportedOperationException();
    }

    public Object toWireForm(IProcessResult processResult) {
        //Optimization, reduce the cost of memory garbage by not creating an ok process result at the source
        //from serialization
        if (processResult == GlobalOrderProcessResult.OK)
            return null;

        return processResult;
    }

    protected void logDeletion(
            GlobalOrderDeletedBacklogPacket deletedBacklogPacket) {
        String deletionMessage = "packets [" + deletedBacklogPacket.getKey() + "-" + deletedBacklogPacket.getEndKey() + "] are lost due to backlog deletion at the source";
        getGroupHistory().logEvent(getSourceLookupName(), deletionMessage);
        if (_specificLogger.isWarnEnabled())
            _specificLogger.warn(deletionMessage);
    }

    public String dumpState() {
        return "State [" + _state + "] had any handshake ["
                + !_firstHandshakeForTarget + "] last process key ["
                + _lastProcessedKey + "] processing queue state " + dumpStateExtra();
    }

    protected String dumpStateExtra() {
        return "";
    }

    @Override
    public IProcessResult processIdleStateData(String string, IIdleStateData idleStateData,
                                               IReplicationInFilterCallback inFilterCallback) {
        throw new UnsupportedOperationException();
    }

}
