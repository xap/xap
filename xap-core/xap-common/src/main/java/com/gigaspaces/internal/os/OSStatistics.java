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

package com.gigaspaces.internal.os;

import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class OSStatistics implements SmartExternalizable {
    private static final long serialVersionUID = -6526052574312023489L;

    private long timestamp = -1;

    private long freeSwapSpaceSize = -1;
    private long freePhysicalMemorySize = -1;
    private long actualFreePhysicalMemorySize = -1;

    private double cpuPerc = -1;

    /**
     * value is in bytes
     *
     * @since 10.1
     */
    private long actualMemoryUsed;

    /**
     * @since 10.1
     */
    //value can be from 0 to 1
    private double usedMemoryPerc;

    private OSNetInterfaceStats[] netStats;

    public OSStatistics() {
    }

    public OSStatistics(long timestamp, long freeSwapSpaceSize, long freePhysicalMemorySize) {
        this(timestamp, freeSwapSpaceSize, freePhysicalMemorySize, -1, -1, -1, -1, null);
    }

    public OSStatistics(long timestamp, long freeSwapSpaceSize, long freePhysicalMemorySize, long actualFreePhysicalMemorySize,
                        double cpuPerc, long actualMemoryUsed, double usedMemoryPerc, OSNetInterfaceStats[] netStats) {
        this.timestamp = timestamp;
        this.freeSwapSpaceSize = freeSwapSpaceSize;
        this.freePhysicalMemorySize = freePhysicalMemorySize;
        this.actualFreePhysicalMemorySize = actualFreePhysicalMemorySize;
        this.cpuPerc = cpuPerc;
        this.netStats = netStats;
        this.usedMemoryPerc = usedMemoryPerc;
        this.actualMemoryUsed = actualMemoryUsed;
    }

    /**
     * @return used memory in bytes
     * @since 10.1
     */
    public long getActualMemoryUsed() {
        return actualMemoryUsed;
    }

    /**
     * @return used memory in percents, value can be from 0 to 1
     * @since 10.1
     */
    public double getUsedMemoryPerc() {
        return usedMemoryPerc;
    }

    public boolean isNA() {
        return timestamp == -1;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getFreeSwapSpaceSize() {
        return freeSwapSpaceSize;
    }

    public long getFreePhysicalMemorySize() {
        return freePhysicalMemorySize;
    }

    public long getActualFreePhysicalMemorySize() {
        return actualFreePhysicalMemorySize;
    }

    public double getCpuPerc() {
        return cpuPerc;
    }

    public OSNetInterfaceStats[] getNetStats() {
        return netStats;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(timestamp);
        out.writeLong(freeSwapSpaceSize);
        out.writeLong(freePhysicalMemorySize);
        out.writeLong(actualFreePhysicalMemorySize);
        out.writeDouble(cpuPerc);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v10_1_0)) {
            out.writeDouble(usedMemoryPerc);
            out.writeLong(actualMemoryUsed);
        }
        if (netStats == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeInt(netStats.length);
            for (OSNetInterfaceStats netStat : netStats) {
                netStat.writeExternal(out);
            }
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        timestamp = in.readLong();
        freeSwapSpaceSize = in.readLong();
        freePhysicalMemorySize = in.readLong();
        actualFreePhysicalMemorySize = in.readLong();
        cpuPerc = in.readDouble();
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v10_1_0)) {
            usedMemoryPerc = in.readDouble();
            actualMemoryUsed = in.readLong();
        }
        if (in.readBoolean()) {
            int size = in.readInt();
            if (0 < size) {
                netStats = new OSNetInterfaceStats[size];
                for (int i = 0; i < netStats.length; i++) {
                    netStats[i] = new OSNetInterfaceStats();
                    netStats[i].readExternal(in);
                }
            }
        }
    }

    public static class OSNetInterfaceStats implements SmartExternalizable {
        private static final long serialVersionUID = 2554963851193006126L;

        private String name;
        private long rxBytes;
        private long txBytes;
        private long rxPackets;
        private long txPackets;
        private long rxErrors;
        private long txErrors;
        private long rxDropped;
        private long txDropped;

        public OSNetInterfaceStats() {
        }

        public OSNetInterfaceStats(String name, long rxBytes, long txBytes, long rxPackets, long txPackets,
                                   long rxErrors, long txErrors, long rxDropped, long txDropped) {
            this.name = name;
            this.rxBytes = rxBytes;
            this.txBytes = txBytes;
            this.rxPackets = rxPackets;
            this.txPackets = txPackets;
            this.rxErrors = rxErrors;
            this.txErrors = txErrors;
            this.rxDropped = rxDropped;
            this.txDropped = txDropped;
        }

        public String getName() {
            return name;
        }

        public long getRxBytes() {
            return rxBytes;
        }

        public long getTxBytes() {
            return txBytes;
        }

        public long getRxPackets() {
            return rxPackets;
        }

        public long getTxPackets() {
            return txPackets;
        }

        public long getRxErrors() {
            return rxErrors;
        }

        public long getTxErrors() {
            return txErrors;
        }

        /**
         *  @since 14.5.0 - will return -1 when using Oshi
         */
        //TODO: remove from read/write external
        @Deprecated
        public long getRxDropped() {
            return rxDropped;
        }

        /**
         *  @since 14.5.0 - will return -1 when using Oshi
         */
        //TODO: remove from read/write external
        @Deprecated
        public long getTxDropped() {
            return txDropped;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeUTF(name);
            out.writeLong(rxBytes);
            out.writeLong(txBytes);
            out.writeLong(rxPackets);
            out.writeLong(txPackets);
            out.writeLong(rxErrors);
            out.writeLong(txErrors);
            out.writeLong(rxDropped);
            out.writeLong(txDropped);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            name = in.readUTF();
            rxBytes = in.readLong();
            txBytes = in.readLong();
            rxPackets = in.readLong();
            txPackets = in.readLong();
            rxErrors = in.readLong();
            txErrors = in.readLong();
            rxDropped = in.readLong();
            txDropped = in.readLong();
        }
    }
}
