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

package com.gigaspaces.internal.jvm;

import com.gigaspaces.internal.os.ProcessCpuSampler;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class JVMStatistics implements SmartExternalizable {

    // for backwards compatibility
    private static final long serialVersionUID = 1688670775236533438L;

    private long timestamp = -1;

    private long uptime;

    private long memoryHeapCommitted;

    private long memoryHeapUsed;

    private long memoryNonHeapCommitted;

    private long memoryNonHeapUsed;

    private int threadCount;

    private int peakThreadCount;

    private long gcCollectionCount;

    private long gcCollectionTime;

    private double cpuPerc = -1;

    private long cpuTotal = -1;

    private long cpuTime = -1;

    public JVMStatistics() {
    }

    public JVMStatistics(long timestamp, long uptime,
                         long memoryHeapCommitted, long memoryHeapUsed, long memoryNonHeapCommitted, long memoryNonHeapUsed,
                         int threadCount, int peakThreadCount, long gcCollectionCount, long gcCollectionTime,
                         double cpuPerc, long cpuTotal, long cpuTime) {
        this.timestamp = timestamp;
        this.uptime = uptime;
        this.memoryHeapCommitted = memoryHeapCommitted;
        this.memoryHeapUsed = memoryHeapUsed;
        this.memoryNonHeapCommitted = memoryNonHeapCommitted;
        this.memoryNonHeapUsed = memoryNonHeapUsed;
        this.threadCount = threadCount;
        this.peakThreadCount = peakThreadCount;
        this.gcCollectionCount = gcCollectionCount;
        this.gcCollectionTime = gcCollectionTime;
        this.cpuPerc = cpuPerc;
        this.cpuTotal = cpuTotal;
        this.cpuTime = cpuTime;
    }

    public boolean isNA() {
        return timestamp == -1;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getUptime() {
        return uptime;
    }

    public long getMemoryHeapCommitted() {
        return memoryHeapCommitted;
    }

    public long getMemoryHeapUsed() {
        return memoryHeapUsed;
    }

    public long getMemoryNonHeapCommitted() {
        return memoryNonHeapCommitted;
    }

    public long getMemoryNonHeapUsed() {
        return memoryNonHeapUsed;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public int getPeakThreadCount() {
        return peakThreadCount;
    }

    public long getGcCollectionCount() {
        return gcCollectionCount;
    }

    public long getGcCollectionTime() {
        return gcCollectionTime;
    }

    public double computeCpuPerc(JVMStatistics lastJVMStatistics) {

        // This object has been created by read external by a new client 
        // The object itself has been serialized by and old (< 8.0.3) server 
        if (lastJVMStatistics.getCpuTime() <= 0 || getCpuTime() <= 0) {
            return cpuPerc;
        }

        long timeDelta = getCpuTime() - lastJVMStatistics.getCpuTime();
        long totalDelta = getCpuTotal() - lastJVMStatistics.getCpuTotal();

        if( timeDelta <= 0 || totalDelta < 0 ) {
            return -1;
        }

        return ((double)totalDelta /timeDelta)/ ProcessCpuSampler.cores;
    }

    private long getCpuTotal() {
        return cpuTotal;
    }

    private long getCpuTime() {
        return cpuTime;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(timestamp);
        out.writeLong(uptime);
        out.writeLong(memoryHeapCommitted);
        out.writeLong(memoryHeapUsed);
        out.writeLong(memoryNonHeapCommitted);
        out.writeLong(memoryNonHeapUsed);
        out.writeInt(threadCount);
        out.writeInt(peakThreadCount);
        out.writeLong(gcCollectionCount);
        out.writeLong(gcCollectionTime);
        out.writeLong(cpuTotal);
        out.writeLong(cpuTime);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        timestamp = in.readLong();
        uptime = in.readLong();
        memoryHeapCommitted = in.readLong();
        memoryHeapUsed = in.readLong();
        memoryNonHeapCommitted = in.readLong();
        memoryNonHeapUsed = in.readLong();
        threadCount = in.readInt();
        peakThreadCount = in.readInt();
        gcCollectionCount = in.readLong();
        gcCollectionTime = in.readLong();
        cpuTotal = in.readLong();
        cpuTime = in.readLong();
    }
}
