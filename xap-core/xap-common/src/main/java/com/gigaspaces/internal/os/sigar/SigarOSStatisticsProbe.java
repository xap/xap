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

package com.gigaspaces.internal.os.sigar;

import com.gigaspaces.internal.os.OSStatistics;
import com.gigaspaces.internal.os.OSStatisticsProbe;
import com.gigaspaces.internal.sigar.SigarHolder;

import org.hyperic.sigar.CpuPerc;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.NetInterfaceStat;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.hyperic.sigar.Swap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class SigarOSStatisticsProbe implements OSStatisticsProbe {

    private Sigar sigar;

    private static final Logger _logger = LoggerFactory.getLogger("com.gigaspaces.os.statistics");

    private static final boolean _isWindows = System.getProperty("os.name").toLowerCase().startsWith("win");

    private Map<String, Integer> failedInterfacesCount = new HashMap<String, Integer>();

    public SigarOSStatisticsProbe() {
        sigar = SigarHolder.getSigar();
    }

    public synchronized OSStatistics probeStatistics() throws Exception {
        String[] ifs = sigar.getNetInterfaceList();
        List<OSStatistics.OSNetInterfaceStats> netStats = new ArrayList<OSStatistics.OSNetInterfaceStats>();
        for (String anIf : ifs) {
            // the getInterfaceStat might fail on some devices, simply filter out ones that failed more than 3 times
            Integer failureCount = failedInterfacesCount.get(anIf);
            if (failureCount == null || failureCount < 3) {
                try {
                    NetInterfaceStat stat = sigar.getNetInterfaceStat(anIf);
                    netStats.add(new OSStatistics.OSNetInterfaceStats(anIf, stat.getRxBytes(), stat.getTxBytes(),
                            stat.getRxPackets(), stat.getTxPackets(), stat.getRxErrors(), stat.getTxErrors(),
                            stat.getRxDropped(), stat.getTxDropped()));
                } catch (SigarException e) {
                    if (failureCount == null) {
                        failureCount = 1;
                    } else {
                        failureCount = failureCount + 1;
                    }
                    failedInterfacesCount.put(anIf, failureCount);
                }
            }
        }
        Swap swap = sigar.getSwap();
        Mem mem = sigar.getMem();
        CpuPerc cpuPerc = sigar.getCpuPerc();
        //retrieve memory props from Sigar, fix for GS-11887
        double memoryUsedPercFromSigar = mem.getUsedPercent();
        long actualUsedMemoryFromSigar = mem.getActualUsed();

        long totalPhysicalMemorySizeInBytes = mem.getTotal();
        long freePhysicalMemorySizeInBytes = mem.getFree();
        //calculate memory usage
        double memoryUsedPercCalculated = computePerc(
                totalPhysicalMemorySizeInBytes - freePhysicalMemorySizeInBytes, totalPhysicalMemorySizeInBytes);
        long actualUsedMemoryCalculated = totalPhysicalMemorySizeInBytes - freePhysicalMemorySizeInBytes;

        double memoryUsedPerc;
        long actualUsedMemory;

        if (_isWindows) {
            memoryUsedPerc = memoryUsedPercCalculated;
            actualUsedMemory = actualUsedMemoryCalculated;
        } else {
            memoryUsedPerc = memoryUsedPercFromSigar;
            actualUsedMemory = actualUsedMemoryFromSigar;
        }

        if (_logger.isDebugEnabled()) {
            _logger.debug(
                    "Memory probe, is winOS:" + _isWindows + ", mem used:" + mem.getUsed() +
                            ", memoryUsedPercSigar:" + memoryUsedPercFromSigar + ", memoryUsedPercCalculated:" + memoryUsedPercCalculated +
                            ", mem actual Used from Sigar:" + actualUsedMemoryFromSigar + ", mem actual Calculated:" + actualUsedMemoryCalculated +
                            ", mem free:" + mem.getFree() + ", mem ram:" + mem.getRam() + ", mem actual free:" + mem.getActualFree());
            _logger.debug(
                    "Memory probe (GB):" + ", mem actual Used from Sigar:" + (double) actualUsedMemoryFromSigar / 1024 / 1024 / 1024 +
                            ", mem actual Calculated:" + (double) actualUsedMemoryCalculated / 1024 / 1024 / 1024 +
                            ", mem free GB:" + (double) mem.getFree() / 1024 / 1024 / 1024);
            _logger.debug(
                    "Memory probe, swap used:" + swap.getUsed() + ", swap free:" +
                            swap.getFree() + ", cpu perc getCombined:" + cpuPerc.getCombined());
        }

        return new OSStatistics(System.currentTimeMillis(), swap.getFree(), mem.getFree(),
                mem.getActualFree(), cpuPerc.getCombined(), actualUsedMemory, memoryUsedPerc,
                netStats.toArray(new OSStatistics.OSNetInterfaceStats[netStats.size()]));
    }

    public static double computePerc(long value, long max) {
        return ((double) value) / max * 100;
    }
}
