package com.gigaspaces.internal.jmx;

import com.gigaspaces.internal.jvm.JVMStatistics;
import com.gigaspaces.internal.jvm.JVMStatisticsProbe;
import com.gigaspaces.internal.jvm.JavaUtils;
import com.gigaspaces.internal.oshi.OshiChecker;
import oshi.software.os.OSProcess;
import java.lang.management.*;
import java.util.List;

@com.gigaspaces.api.InternalApi
public class OshiJVMStatisticsProbe  implements JVMStatisticsProbe {
    private static final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    private static final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    private static final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    private static final long pid = JavaUtils.getPid();

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

        OSProcess osProcess = OshiChecker.getProcessInfo(pid);

        return new JVMStatistics(System.currentTimeMillis(),
                runtimeMXBean.getUptime(),
                memoryMXBean.getHeapMemoryUsage().getCommitted(),
                memoryMXBean.getHeapMemoryUsage().getUsed(),
                memoryMXBean.getNonHeapMemoryUsage().getCommitted(),
                memoryMXBean.getNonHeapMemoryUsage().getUsed(),
                threadMXBean.getThreadCount(),
                threadMXBean.getPeakThreadCount(),
                gcCollectionCount,
                gcCollectionTime,
                osProcess.getProcessCpuLoadCumulative(),
                osProcess.getKernelTime() + osProcess.getUserTime(),
                System.currentTimeMillis());
    }
}
