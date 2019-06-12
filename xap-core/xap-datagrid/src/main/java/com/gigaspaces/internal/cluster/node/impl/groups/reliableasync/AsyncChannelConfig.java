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

package com.gigaspaces.internal.cluster.node.impl.groups.reliableasync;

import com.gigaspaces.internal.cluster.node.impl.config.SourceChannelConfig;
import com.j_spaces.core.filters.ReplicationStatistics.ReplicationMode;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @since 8.0.3
 */
@com.gigaspaces.api.InternalApi
public class AsyncChannelConfig extends SourceChannelConfig {
    private static final long serialVersionUID = 1L;

    private int _batchSize;
    private long _intervalMilis;
    private int _intervalOperations;
    private ReplicationMode _channelType;
    private String _tag;

    public AsyncChannelConfig() {
    }

    public AsyncChannelConfig(int batchSize, long intervalMilis,
                              int intervalOperations, ReplicationMode channelType) {
        this(batchSize, intervalMilis, intervalOperations, channelType, SourceChannelConfig.UNLIMITED);
    }

    public AsyncChannelConfig(int batchSize, long intervalMilis,
                              int intervalOperations, ReplicationMode channelType, long maxAllowedDisconnectionTimeBeforeDrop) {
        super(maxAllowedDisconnectionTimeBeforeDrop);
        _batchSize = batchSize;
        _intervalMilis = intervalMilis;
        _intervalOperations = intervalOperations;
        _channelType = channelType;
    }

    /**
     * @param tag a name to identify the channel target.
     * @since 14.5
     */
    public void setTag(String tag) {
        this._tag = tag;
    }

    /**
     * @return a name/id/tag identifying the channel target.
     * @since 14.5
     */
    public String getTag() {
        return this._tag;
    }

    public ReplicationMode getChannelType() {
        return _channelType;
    }

    public int getBatchSize() {
        return _batchSize;
    }

    public long getIntervalMilis() {
        return _intervalMilis;
    }

    public int getIntervalOperations() {
        return _intervalOperations;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(_batchSize);
        out.writeLong(_intervalMilis);
        out.writeInt(_intervalOperations);

        out.writeByte(_channelType.getCode());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        _batchSize = in.readInt();
        _intervalMilis = in.readLong();
        _intervalOperations = in.readInt();
        _channelType = ReplicationMode.fromCode(in.readByte());
    }


}
