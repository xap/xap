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

package com.gigaspaces.lrmi.nio.info;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class NIOStatistics implements SmartExternalizable {

    private static final long serialVersionUID = -2757963460329854081L;

    private static long NA_TIMESTAMP = -1;
    private long timestamp = NA_TIMESTAMP;

    private long completedTaskCount = -1;

    private int activeThreadsCount = -1;

    private int queueSize = -1;

    public NIOStatistics() {
    }

    public NIOStatistics(long timestamp, long completedTaskCount, int activeThreadsCount, int queueSize) {
        this.timestamp = timestamp;
        this.completedTaskCount = completedTaskCount;
        this.activeThreadsCount = activeThreadsCount;
        this.queueSize = queueSize;
    }

    public boolean isNA() {
        return timestamp == NA_TIMESTAMP;
    }

    /**
     * Returns the timestamp when this was created.
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Returns the total number of completed tasks.
     */
    public long getCompletedTaskCount() {
        return completedTaskCount;
    }

    /**
     * Returns the current number of active threads.
     */
    public int getActiveThreadsCount() {
        return activeThreadsCount;
    }

    /**
     * Returns the current size of the pending tasks within the queue.
     */
    public int getQueueSize() {
        return queueSize;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(timestamp);
        out.writeLong(completedTaskCount);
        out.writeInt(activeThreadsCount);
        out.writeInt(queueSize);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        timestamp = in.readLong();
        completedTaskCount = in.readLong();
        activeThreadsCount = in.readInt();
        queueSize = in.readInt();
    }
}
