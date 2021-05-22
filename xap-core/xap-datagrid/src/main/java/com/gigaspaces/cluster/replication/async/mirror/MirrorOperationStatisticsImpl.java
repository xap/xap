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

package com.gigaspaces.cluster.replication.async.mirror;

import com.gigaspaces.metrics.LongCounter;
import com.gigaspaces.metrics.MetricRegistrator;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Statistics for single operation type
 *
 * @author anna
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class MirrorOperationStatisticsImpl implements SmartExternalizable, MirrorOperationStatistics {
    private static final long serialVersionUID = 1L;

    /**
     * Total operation count - how many operations where executed
     */
    private final LongCounter _operationCount = new LongCounter();
    /**
     * Total count of successful operations
     */
    private final LongCounter _successfulOperationCount = new LongCounter();
    /**
     * Total count of failed operations
     */
    private final LongCounter _failedOperationCount = new LongCounter();
    /**
     * Total count of discarded operations
     */
    private final LongCounter _discardedOperationCount = new LongCounter();

    /**
     * Required for Externalizable
     */
    public MirrorOperationStatisticsImpl() {
    }

    @Override
    public long getOperationCount() {
        return _operationCount.getCount();
    }

    @Override
    public long getSuccessfulOperationCount() {
        return _successfulOperationCount.getCount();
    }

    @Override
    public long getFailedOperationCount() {
        return _failedOperationCount.getCount();
    }

    @Override
    public long getDiscardedOperationCount() {
        return _discardedOperationCount.getCount();
    }

    @Override
    public long getInProgressOperationCount() {
        long completedOperationCount = getSuccessfulOperationCount()
                + getFailedOperationCount() + getDiscardedOperationCount();
        return getOperationCount() - completedOperationCount;
    }

    public void incrementOperationCount() {
        _operationCount.inc();
    }

    public void incrementSuccessfulOperationCount() {
        _successfulOperationCount.inc();
    }

    public void incrementFailedOperationCount() {
        _failedOperationCount.inc();
    }

    public void incrementDiscardedOperationCount() {
        _discardedOperationCount.inc();
    }

    public void add(MirrorOperationStatistics operationStatistics) {
        _successfulOperationCount.inc(operationStatistics.getSuccessfulOperationCount());
        _failedOperationCount.inc(operationStatistics.getFailedOperationCount());
        _discardedOperationCount.inc(operationStatistics.getDiscardedOperationCount());
        _operationCount.inc(operationStatistics.getOperationCount());
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("  MirrorOperationStatistics\n    [\n        operationCount=");
        builder.append(_operationCount);
        builder.append(", \n        successfulOperationCount=");
        builder.append(_successfulOperationCount);
        builder.append(", \n        discardedOperationCount=");
        builder.append(_discardedOperationCount);
        builder.append(", \n        failedOperationCount=");
        builder.append(_failedOperationCount);
        builder.append(", \n        inProgressOperationCount=");
        builder.append(getInProgressOperationCount());
        builder.append("\n    ]");
        return builder.toString();
    }

    public void register(MetricRegistrator registrator, String prefix) {
        registrator.register(prefix + "total", _operationCount);
        registrator.register(prefix + "successful", _successfulOperationCount);
        registrator.register(prefix + "failed", _operationCount);
        registrator.register(prefix + "discarded", _operationCount);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(_operationCount.getCount());
        out.writeLong(_successfulOperationCount.getCount());
        out.writeLong(_failedOperationCount.getCount());
        out.writeLong(_discardedOperationCount.getCount());
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _operationCount.inc(in.readLong());
        _successfulOperationCount.inc(in.readLong());
        _failedOperationCount.inc(in.readLong());
        _discardedOperationCount.inc(in.readLong());
    }
}