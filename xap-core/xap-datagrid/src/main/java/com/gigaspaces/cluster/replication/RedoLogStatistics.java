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

package com.gigaspaces.cluster.replication;

import java.util.Map;

/**
 * Redo log statistics
 *
 * Not serializable should go over the wire
 *
 * @author eitany
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class RedoLogStatistics
        implements IRedoLogStatistics {

    public static final IRedoLogStatistics EMPTY_STATISTICS = new RedoLogStatistics(-1, -1, 0, 0, 0, 0, 0, 0, null);
    private final long _lastKeyInRedoLog;
    private final long _firstKeyInRedoLog;

    private final long _size;
    private final long _memoryPacketsCount;
    private final long _externalStoragePacketsCount;
    private final long _externalStorageSpaceUsed;

    private final long _memorySize;
    private final long _externalStorageSize;

    private final Map<String, ReplicationTargetInfo> _replicationTargetsInfo;

    public RedoLogStatistics(long lastKeyInRedoLog, long firstKeyInRedoLog,
                             long size, long memoryPacketsCount,
                             long externalStoragePacketsCount, long externalStorageSpaceUsed,
                             long memorySize, long externalStorageSize, Map<String, ReplicationTargetInfo> targetsInfo) {
        super();
        _lastKeyInRedoLog = lastKeyInRedoLog;
        _firstKeyInRedoLog = firstKeyInRedoLog;
        _size = size;
        _memoryPacketsCount = memoryPacketsCount;
        _externalStoragePacketsCount = externalStoragePacketsCount;
        _externalStorageSpaceUsed = externalStorageSpaceUsed;
        _memorySize = memorySize;
        _externalStorageSize = externalStorageSize;
        _replicationTargetsInfo = targetsInfo;
    }


    public long getExternalStoragePacketsCount() {
        return _externalStoragePacketsCount;
    }

    @Override
    public long getMemoryPacketsWeight() {
        return _memorySize;
    }

    @Override
    public long getExternalStoragePacketsWeight() {
        return _externalStorageSize;
    }

    public long getExternalStorageSpaceUsed() {
        return _externalStorageSpaceUsed;
    }

    public long getMemoryPacketsCount() {
        return _memoryPacketsCount;
    }

    public long size() {
        return _size;
    }

    public long getLastKeyInRedoLog() {
        return _lastKeyInRedoLog;
    }

    public long getFirstKeyInRedoLog() {
        return _firstKeyInRedoLog;
    }

    @Override
    public Map<String, ReplicationTargetInfo> getReplicationTargetsInfo() {
        return _replicationTargetsInfo;
    }

    @Override
    public String toString() {
        return "RedoLogStatistics [_lastKeyInRedoLog=" + _lastKeyInRedoLog
                + ", _firstKeyInRedoLog=" + _firstKeyInRedoLog + ", _size="
                + _size + ", _memoryPacketsCount=" + _memoryPacketsCount
                + ", _externalStoragePacketsCount="
                + _externalStoragePacketsCount + ", _externalStorageSpaceUsed="
                + _externalStorageSpaceUsed + "]";
    }

}
