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


package com.j_spaces.core.filters;

import com.gigaspaces.cluster.replication.IRedoLogStatistics;
import com.gigaspaces.cluster.replication.IReplicationChannel.State;
import com.gigaspaces.cluster.replication.ReplicationTargetInfo;
import com.gigaspaces.internal.cluster.node.impl.router.ReplicationEndpointDetails;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.management.transport.ConnectionEndpointDetails;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * Represents the current state of the replication module
 *
 * @author anna
 * @since 7.0
 */

public class ReplicationStatistics
        implements SmartExternalizable {
    private static final long serialVersionUID = 1L;

    private OutgoingReplication _outgoingReplication;

    // Externalizable
    public ReplicationStatistics() {
    }

    public ReplicationStatistics(OutgoingReplication outReplication) {
        _outgoingReplication = outReplication;

    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _outgoingReplication = new OutgoingReplication();
        _outgoingReplication.readExternal(in);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        _outgoingReplication.writeExternal(out);
    }

    public static class OutgoingReplication
            implements SmartExternalizable {
        private static final long serialVersionUID = 1L;

        private long _lastKeyInRedoLog;
        private long _firstKeyInRedoLog;
        private long _lastConfirmedKey;
        private long _redoLogSize;

        private List<OutgoingChannel> _channels = new LinkedList<OutgoingChannel>();

        private long _memoryPacketsCount;
        private long _externalStoragePacketsCount;
        private long _externalStorageSpaceUsed;
        private Map<String, ReplicationTargetInfo> _replicationTargetsInfo;

        private long _memoryRedoLogSize;
        private long _externalStorageRedoLogSize;

        // For externalizable
        public OutgoingReplication() {

        }

        public OutgoingReplication(IRedoLogStatistics redoLogStatistics,
                                   Collection<OutgoingChannel> outChannels) {
            _lastKeyInRedoLog = redoLogStatistics.getLastKeyInRedoLog();
            _firstKeyInRedoLog = redoLogStatistics.getFirstKeyInRedoLog();
            _redoLogSize = redoLogStatistics.size();
            _memoryPacketsCount = redoLogStatistics.getMemoryPacketsCount();
            _externalStoragePacketsCount = redoLogStatistics.getExternalStoragePacketsCount();
            _externalStorageSpaceUsed = redoLogStatistics.getExternalStorageSpaceUsed();

            // this can be not very accurate but an approximation
            long lastConfirmedKey = _lastKeyInRedoLog;

            for (OutgoingChannel channel : outChannels) {
                if (channel.getLastConfirmedKeyFromTarget() < lastConfirmedKey)
                    lastConfirmedKey = channel.getLastConfirmedKeyFromTarget();

                getChannels().add(channel);

            }
            _lastConfirmedKey = lastConfirmedKey;

            _replicationTargetsInfo = redoLogStatistics.getReplicationTargetsInfo();

            _memoryRedoLogSize = redoLogStatistics.getMemoryPacketsWeight();
            _externalStorageRedoLogSize = redoLogStatistics.getExternalStoragePacketsWeight();

        }

        public long getNumOfPacketsToReplicate() {
            return getLastKeyInRedoLog() - getLastConfirmedKey();
        }

        public long getLastKeyInRedoLog() {
            return _lastKeyInRedoLog;
        }

        public long getFirstKeyInRedoLog() {
            return _firstKeyInRedoLog;
        }

        public long getLastConfirmedKey() {
            return _lastConfirmedKey;
        }

        /**
         * @return weight of packets awaiting replication
         */
        public long getRedoLogSize() {
            return _redoLogSize;
        }

        /**
         * @return info of replication target
         */
        public ReplicationTargetInfo getReplicationTargetInfo(String target) {
            return _replicationTargetsInfo.get(target);
        }

        /**
         * @return number of packets awaiting replication that are stored inside the jvm memory
         */
        public long getRedoLogMemoryPacketCount() {
            return _memoryPacketsCount;
        }

        /**
         * @return number of packets awaiting replication that are stored outside of the jvm memory
         * (on disk)
         */
        public long getRedoLogExternalStoragePacketCount() {
            return _externalStoragePacketsCount;
        }

        /**
         * @return number of bytes used in external storage to hold packets that are awaiting
         * replication (on disk)
         */
        public long getRedoLogExternalStorageSpaceUsed() {
            return _externalStorageSpaceUsed;
        }

        /**
         * @return number of packets held in memory
         */
        public long getMemoryRedoLogSize() {
            return _memoryRedoLogSize;
        }

        /**
         * @return total weight of packets held in external storage
         */
        public long getExternalStorageRedoLogSize() {
            return _externalStorageRedoLogSize;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder("OutgoingReplication\n    [\n     getLastKeyInRedoLog()=");
            builder.append(getLastKeyInRedoLog());
            builder.append(", \n     getFirstKeyInRedoLog()=");
            builder.append(getFirstKeyInRedoLog());
            builder.append(", \n     getLastConfirmedKey()=");
            builder.append(getLastConfirmedKey());
            builder.append(", \n     getNumOfPacketsToReplicate()=");
            builder.append(getNumOfPacketsToReplicate());
            builder.append(", \n     getRedoLogSize()=");
            builder.append(getRedoLogSize());
            builder.append(", \n     getRedoLogMemoryPacketCount()=");
            builder.append(getRedoLogMemoryPacketCount());
            builder.append(", \n     getRedoLogExternalStoragePacketCount()=");
            builder.append(getRedoLogExternalStoragePacketCount());
            builder.append(", \n     getRedoLogExternalStorageSpaceUsed()=");
            builder.append(getRedoLogExternalStorageSpaceUsed());
            builder.append(", \n     getChannels()=");
            builder.append(getChannels());
            builder.append("\n    ]");
            return builder.toString();
        }

        public List<OutgoingChannel> getChannels() {
            return _channels;
        }

        public List<OutgoingChannel> getChannels(ReplicationMode... modes) {
            LinkedList<OutgoingChannel> result = new LinkedList<ReplicationStatistics.OutgoingChannel>();
            EnumSet<ReplicationMode> enumSet = EnumSet.noneOf(ReplicationMode.class);
            for (ReplicationMode replicationMode : modes)
                enumSet.add(replicationMode);

            for (OutgoingChannel outgoingChannel : _channels) {
                if (enumSet.contains(outgoingChannel.getReplicationMode()))
                    result.add(outgoingChannel);
            }

            return result;
        }

        @SuppressWarnings("unchecked")
        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {
            _lastKeyInRedoLog = in.readLong();
            _firstKeyInRedoLog = in.readLong();
            _lastConfirmedKey = in.readLong();
            _redoLogSize = in.readLong();
            _channels = (List<OutgoingChannel>) in.readObject();
            _memoryPacketsCount = in.readLong();
            _externalStoragePacketsCount = in.readLong();
            _externalStorageSpaceUsed = in.readLong();
            if(LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v12_1_0)){
                _replicationTargetsInfo = (Map<String, ReplicationTargetInfo>) in.readObject();
            }
            if(LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v15_0_0)){
                _memoryRedoLogSize = in.readLong();
                _externalStorageRedoLogSize = in.readLong();
            }
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(_lastKeyInRedoLog);
            out.writeLong(_firstKeyInRedoLog);
            out.writeLong(_lastConfirmedKey);
            out.writeLong(_redoLogSize);
            out.writeObject(_channels);
            out.writeLong(_memoryPacketsCount);
            out.writeLong(_externalStoragePacketsCount);
            out.writeLong(_externalStorageSpaceUsed);
            if(LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v12_1_0)){
                out.writeObject(_replicationTargetsInfo);
            }
            if(LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v15_0_0)){
                out.writeLong(_memoryRedoLogSize);
                out.writeLong(_externalStorageRedoLogSize);
            }
        }

    }

    public enum ReplicationMode {
        @Deprecated
        SYNC((byte) 0),
        @Deprecated
        ASYNC((byte) 1),
        MIRROR((byte) 2),
        BACKUP_SPACE((byte) 3),
        ACTIVE_SPACE((byte) 4),
        LOCAL_VIEW((byte) 5),
        GATEWAY((byte) 6),
        DURABLE_NOTIFICATION((byte) 7);

        private final byte _code;

        private ReplicationMode(byte code) {
            _code = code;
        }

        public static ReplicationMode fromCode(byte code) {
            switch (code) {
                case 0:
                    return SYNC;
                case 1:
                    return ASYNC;
                case 2:
                    return MIRROR;
                case 3:
                    return BACKUP_SPACE;
                case 4:
                    return ACTIVE_SPACE;
                case 5:
                    return LOCAL_VIEW;
                case 6:
                    return GATEWAY;
                case 7:
                    return DURABLE_NOTIFICATION;
                default:
                    throw new IllegalArgumentException("Unknown code - " + code);
            }
        }

        public byte getCode() {
            return _code;
        }
    }

    /**
     * Specifies the replication channel current operating mode. A synchronous channel can move to
     * asynchronous state upon disconnection that is followed by a reconnection, And replication
     * errors. It will remain in an asynchronous state until the accumulated replication backlog
     * during that time period is replicated to the target and then the synchronous state is
     * restored.
     *
     * @author eitany
     * @since 8.0
     */
    public enum ReplicationOperatingMode {
        /**
         * Channel is operating in synchronous mode
         */
        SYNC((byte) 0),
        /**
         * Channel is operating in asynchronous mode, if the channel original type is synchronous
         * this means that there is compromised data that is currently being replicated
         * asynchronously, once the synchronous state is restored there is no more compromised
         * data.
         */
        ASYNC((byte) 1),
        /**
         * Channel is operating in reliable asynchronous mode which means there's at least one
         * active synchronous backup that keeps the replication redo log for this channel in case of
         * a failure.
         */
        RELIABLE_ASYNC((byte) 2);

        private final byte _code;

        private ReplicationOperatingMode(byte code) {
            _code = code;
        }

        public byte getCode() {
            return _code;
        }

        public static ReplicationOperatingMode fromCode(byte code) {
            switch (code) {
                case 0:
                    return SYNC;
                case 1:
                    return ASYNC;
                case 2:
                    return RELIABLE_ASYNC;
                default:
                    throw new IllegalArgumentException("Unknown code: " + code);
            }
        }
    }

    /**
     * Specifies the current channel state
     *
     * @author eitany
     * @since 8.0
     */
    public enum ChannelState {
        /**
         * The channel is disconnected.
         */
        DISCONNECTED,
        /**
         * The channel is physically connected (The network is connected).
         */
        CONNECTED,
        /**
         * The channel is physically connected and after handshake has been successfully executed.
         */
        ACTIVE;
    }

    /**
     * Expose outgoing replication channel statistics
     *
     * @author eitany
     */
    public static class OutgoingChannel
            implements SmartExternalizable {

        private static final long serialVersionUID = 1L;

        private long _lastKeyToReplicate;
        private long _lastConfirmedKeyFromTarget;
        private String _targetMemberName;
        private State _state;
        private ChannelState _channelState;
        private ReplicationMode _replicationMode;
        private int _throughPut;
        private long _totalNumberOfReplicatedPackets;
        private String _inconsistencyReason;
        private long _generatedTraffic;
        private long _receivedTraffic;
        private long _generatedTrafficTP;
        private long _receivedTrafficTP;
        private long _generatedTrafficPerPacket;
        private long _redologRetainedSize;
        private ReplicationOperatingMode _operatingMode;
        private String _targetShortHostname;
        private Object _targetUuid;
        private ConnectionEndpointDetails _connectionEndpointDetails;
        private ConnectionEndpointDetails _delegatorDetails;
        private String _tag;

        // Externalizable
        public OutgoingChannel() {
        }

        public OutgoingChannel(String targetMemberName,
                               ReplicationMode replicationMode, ChannelState channelState,
                               long lastKeyToReplicate, long lastConfirmedKeyFromTarget,
                               int throughPut, long totalNumberOfReplicatedPackets,
                               String inconsistencyReason,
                               long generatedTraffic, long receivedTraffic,
                               long generatedTrafficTP, long receivedTrafficTP,
                               long generatedTrafficPerPacket, long redologRetainedSize,
                               ReplicationOperatingMode operatingMode,
                               ReplicationEndpointDetails targetDetails,
                               ConnectionEndpointDetails delegatorDetails,
                               String tag) {
            _targetMemberName = targetMemberName;
            _replicationMode = replicationMode;
            _channelState = channelState;
            _totalNumberOfReplicatedPackets = totalNumberOfReplicatedPackets;
            _delegatorDetails = delegatorDetails;
            _targetShortHostname = targetDetails != null ? targetDetails.getHostname() : null;
            _connectionEndpointDetails = targetDetails != null ? targetDetails.getConnectionDetails() : null;
            _state = (_channelState == ChannelState.CONNECTED || _channelState == ChannelState.ACTIVE) ? State.CONNECTED
                    : State.DISCONNECTED;
            _lastKeyToReplicate = lastKeyToReplicate;
            _lastConfirmedKeyFromTarget = lastConfirmedKeyFromTarget;
            _throughPut = throughPut;
            _inconsistencyReason = inconsistencyReason;
            _generatedTraffic = generatedTraffic;
            _receivedTraffic = receivedTraffic;
            _generatedTrafficTP = generatedTrafficTP;
            _receivedTrafficTP = receivedTrafficTP;
            _generatedTrafficPerPacket = generatedTrafficPerPacket;
            _redologRetainedSize = redologRetainedSize;
            _operatingMode = operatingMode;
            _targetUuid = targetDetails != null ? targetDetails.getUniqueId() : null;
            _tag = tag;
        }

        public long getLastConfirmedKeyFromTarget() {
            return _lastConfirmedKeyFromTarget;
        }

        public String getTargetMemberName() {
            return _targetMemberName;
        }

        /**
         * @deprecated use {@link #getChannelState()}
         */
        @Deprecated
        public State getState() {
            return _state;
        }

        /**
         * Gets the current channel state,
         *
         * @since 8.0
         */
        public ChannelState getChannelState() {
            return _channelState;
        }

        public long getLastKeyToReplicate() {
            return _lastKeyToReplicate;
        }

        public ReplicationMode getReplicationMode() {
            return _replicationMode;
        }

        /**
         * @return an approximation of the total number of replicated packets through this channel
         */
        public long getNumberOfReplicatedPackets() {
            return _totalNumberOfReplicatedPackets;
        }

        /**
         * @return current rate of packet being sent per second.
         * @since 8.0
         */
        public int getSendPacketsPerSecond() {
            return _throughPut;
        }

        /**
         * @since 8.0
         */
        public boolean isInconsistent() {
            return getInconsistencyReason() != null;
        }

        /**
         * @return channel inconsistency reason or null if the channel is consistent
         * @since 8.0
         */
        public String getInconsistencyReason() {
            return _inconsistencyReason;
        }

        /**
         * @return total bytes sent (resets during fail-over).
         * @since 8.0
         */
        public long getSentBytes() {
            return _generatedTraffic;
        }

        /**
         * @return total bytes received (resets during fail-over).
         * @since 8.0
         */
        public long getReceivedBytes() {
            return _receivedTraffic;
        }

        /**
         * @return current rate of bytes being sent per second.
         * @since 8.0
         */
        public long getSendBytesPerSecond() {
            return _generatedTrafficTP;
        }

        /**
         * @return current rate of bytes being received per second.
         * @since 8.0
         */
        public long getReceiveBytesPerSecond() {
            return _receivedTrafficTP;
        }

        /**
         * @return current rate of bytes being sent per replication packet.
         * @since 8.0
         */
        public long getSendBytesPerPacket() {
            return _generatedTrafficPerPacket;
        }

        /**
         * @return gets the number of packets that are kept in the redolog for this channel
         * @since 8.0
         */
        public long getRedologRetainedSize() {
            return _redologRetainedSize;
        }

        /**
         * @return the current mode this channel is operation in, a sync channel can move to async
         * operating mode upon errors and during reconnection while the accumulated data in the
         * redolog is being replicated, a reliable async channel can move to async operating mode if
         * currently there is no active synchronous backup keeping the replication redolog of this
         * target in case of a primary space instance failure.
         * @since 8.0
         */
        public ReplicationOperatingMode getOperatingMode() {
            return _operatingMode;
        }

        /**
         * @return the channel target hostname
         * @since 9.0.1
         * @deprecated since 9.5 - Use getTargetDetails().getHostName() instead.
         */
        @Deprecated
        public String getTargetHostname() {
            return _targetShortHostname;
        }

        /**
         * @return the channel target hosting process id, may be -1 if connection was never
         * established.
         * @since 9.0.1
         * @deprecated since 9.5 - Use getTargetDetails().getProcessId() instead.
         */
        @Deprecated
        public long getTargetPid() {
            return getTargetDetails() != null ? getTargetDetails().getProcessId() : -1;
        }

        /**
         * @return The service ID (UUID) of the replication target. returns <code>null</code> if
         * replication hasn't yet been established.
         */
        public Object getTargetUuid() {
            return _targetUuid;
        }

        /**
         * @return the channel target connection endpoint details, may be null if connection was
         * never established.
         * @since 9.5
         */
        public ConnectionEndpointDetails getTargetDetails() {
            return _connectionEndpointDetails;
        }

        /**
         * @return the channel target delegator connection endpoint details, may be null if there is
         * no delegator and the target is connected directly or a connection was never established.
         * @since 9.5
         */
        public ConnectionEndpointDetails getDelegatorDetails() {
            return _delegatorDetails;
        }

        /**
         * @return a name/id/tag identifying the endpoint remote event listener
         * @since 14.5
         */
        public String getTag() {
            return _tag;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append(" \n     OutgoingChannel");
            builder.append("\n      (");
            builder.append("  \n        getTargetMemberName()=");
            builder.append(getTargetMemberName());
            builder.append(", \n        getTargetUuid()=");
            builder.append(getTargetUuid());
            builder.append(", \n        getTargetHostname()=");
            builder.append(getTargetHostname());
            builder.append(", \n        getTargetDetails()=");
            builder.append(getTargetDetails());
            builder.append(", \n        getDelegatorDetails()=");
            builder.append(getDelegatorDetails());
            builder.append(", \n        getTag()=");
            builder.append(getTag());
            builder.append(", \n        getReplicationMode()=");
            builder.append(getReplicationMode());
            builder.append(", \n        getState()=");
            builder.append(getState());
            builder.append(", \n        getChannelState()=");
            builder.append(getChannelState());
            builder.append(", \n        getOperatingMode()=");
            builder.append(getOperatingMode());
            builder.append(", \n        getLastKeyToReplicate()=");
            builder.append(getLastKeyToReplicate());
            builder.append(", \n        getLastConfirmedKeyFromTarget()=");
            builder.append(getLastConfirmedKeyFromTarget());
            builder.append(", \n        getRedologRetainedSize()=");
            builder.append(getRedologRetainedSize());
            builder.append(", \n        getSendPacketsPerSecond()=");
            builder.append(getSendPacketsPerSecond());
            builder.append(", \n        getSentBytes()=");
            builder.append(getSentBytes() + " Bytes");
            builder.append(", \n        getReceivedBytes()=");
            builder.append(getReceivedBytes() + " Bytes");
            builder.append(", \n        getSendBytesPerSecond()=");
            builder.append(getSendBytesPerSecond() + " Bytes");
            builder.append(", \n        getReceiveBytesPerSecond()=");
            builder.append(getReceiveBytesPerSecond() + " Bytes");
            builder.append(", \n        getSendBytesPerPacket()=");
            builder.append(getSendBytesPerPacket() + " Bytes");
            builder.append(", \n        isInconsistent()=");
            builder.append(isInconsistent());
            builder.append(", \n        getInconsistencyReason()=");
            builder.append(getInconsistencyReason() == null ? "NONE"
                    : getInconsistencyReason());
            builder.append("\n      )");
            return builder.toString();
        }

        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {
            final PlatformLogicalVersion endpointLogicalVersion = LRMIInvocationContext.getEndpointLogicalVersion();
            _lastKeyToReplicate = in.readLong();
            _lastConfirmedKeyFromTarget = in.readLong();
            _totalNumberOfReplicatedPackets = getLastConfirmedKeyFromTarget();

            if (endpointLogicalVersion.greaterOrEquals(PlatformLogicalVersion.v10_1_0)) {
                _targetUuid = IOUtils.readObject(in);
            }
            if (endpointLogicalVersion.greaterOrEquals(PlatformLogicalVersion.v9_5_0)) {
                _totalNumberOfReplicatedPackets = in.readLong();
                _connectionEndpointDetails = IOUtils.readObject(in);
                _delegatorDetails = IOUtils.readObject(in);
                _targetShortHostname = IOUtils.readRepetitiveString(in);
            } else {
                _targetShortHostname = IOUtils.readRepetitiveString(in);
                long pid = in.readLong();
                _connectionEndpointDetails = new ConnectionEndpointDetails(_targetShortHostname, null, pid, null);
            }

            _targetMemberName = IOUtils.readRepetitiveString(in);
            _state = (State) in.readObject();
            _replicationMode = ReplicationMode.fromCode(in.readByte());
            _throughPut = in.readInt();
            _inconsistencyReason = IOUtils.readString(in);
            _generatedTraffic = in.readLong();
            _receivedTraffic = in.readLong();
            _generatedTrafficTP = in.readLong();
            _receivedTrafficTP = in.readLong();
            _generatedTrafficPerPacket = in.readLong();
            _redologRetainedSize = in.readLong();
            _operatingMode = ReplicationOperatingMode.fromCode(in.readByte());
            _channelState = (ChannelState) in.readObject();

            if (endpointLogicalVersion.greaterOrEquals(PlatformLogicalVersion.v14_5_0)) {
                _tag = IOUtils.readString(in);
            }
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            final PlatformLogicalVersion endpointLogicalVersion = LRMIInvocationContext.getEndpointLogicalVersion();
            out.writeLong(_lastKeyToReplicate);
            out.writeLong(_lastConfirmedKeyFromTarget);

            if (endpointLogicalVersion.greaterOrEquals(PlatformLogicalVersion.v10_1_0)) {
                IOUtils.writeObject(out, _targetUuid);
            }
            if (endpointLogicalVersion.greaterOrEquals(PlatformLogicalVersion.v9_5_0)) {
                out.writeLong(_totalNumberOfReplicatedPackets);
                IOUtils.writeObject(out, _connectionEndpointDetails);
                IOUtils.writeObject(out, _delegatorDetails);
                IOUtils.writeRepetitiveString(out, _targetShortHostname);
            } else {
                IOUtils.writeRepetitiveString(out, _targetShortHostname);
                out.writeLong(getTargetPid());
            }

            IOUtils.writeRepetitiveString(out, _targetMemberName);
            out.writeObject(_state);
            out.writeByte(_replicationMode.getCode());
            out.writeInt(_throughPut);
            IOUtils.writeString(out, _inconsistencyReason);
            out.writeLong(_generatedTraffic);
            out.writeLong(_receivedTraffic);
            out.writeLong(_generatedTrafficTP);
            out.writeLong(_receivedTrafficTP);
            out.writeLong(_generatedTrafficPerPacket);
            out.writeLong(_redologRetainedSize);
            out.writeByte(_operatingMode.getCode());
            out.writeObject(_channelState);

            if (endpointLogicalVersion.greaterOrEquals(PlatformLogicalVersion.v14_5_0)) {
                IOUtils.writeString(out, _tag);
            }
        }
    }

    public OutgoingReplication getOutgoingReplication() {
        return _outgoingReplication;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ReplicationStatistics\n [_outgoingReplication=");
        builder.append(_outgoingReplication);
        builder.append("]");
        return builder.toString();
    }

}
