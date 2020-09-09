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

package com.gigaspaces.internal.jvm.jmx;

import com.gigaspaces.internal.jvm.JVMStatistics;
import com.gigaspaces.internal.jvm.JVMStatisticsProbe;
import com.gigaspaces.internal.os.ProcessCpuSampler;

import java.lang.management.*;
import java.util.List;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class JMXJVMStatisticsProbe implements JVMStatisticsProbe {

    private static final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    private static final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    private static final long startTime = ManagementFactory.getRuntimeMXBean().getStartTime();
    private final ProcessCpuSampler cpuSampler;

    public JMXJVMStatisticsProbe(ProcessCpuSampler cpuSampler) {
        this.cpuSampler = cpuSampler;
    }

    public JVMStatistics probeStatistics() {
        long gcCollectionCount = 0;
        long gcCollectionTime = 0;
        List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean gcMxBean : gcMxBeans) {
            long tmp = gcMxBean.getCollectionCount();
            if (tmp != -1) {
                gcCollectionCount += tmp;
            }
            tmp = gcMxBean.getCollectionTime();
            if (tmp != -1) {
                gcCollectionTime += tmp;
            }
        }

        MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
        MemoryUsage nonHeapMemoryUsage = memoryMXBean.getNonHeapMemoryUsage();

        long currTime = System.currentTimeMillis();
        long uptime = currTime - startTime;
        long totalCpuTime = cpuSampler.sampleTotalCpuTime();
        double cpuPerc = cpuSampler.getCpuLoadCumulative(totalCpuTime, uptime);
        long cpuSampleTime = totalCpuTime != cpuSampler.NA ? currTime : cpuSampler.NA;
        return new JVMStatistics(currTime, uptime,
                heapMemoryUsage.getCommitted(), heapMemoryUsage.getUsed(),
                nonHeapMemoryUsage.getCommitted(), nonHeapMemoryUsage.getUsed(),
                threadMXBean.getThreadCount(), threadMXBean.getPeakThreadCount(),
                gcCollectionCount, gcCollectionTime,
                cpuPerc,
                totalCpuTime,
                cpuSampleTime);
    }
}
