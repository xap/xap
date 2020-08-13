package com.gigaspaces.internal.jmx;

import com.gigaspaces.internal.jvm.JVMStatistics;
import com.gigaspaces.internal.jvm.JVMStatisticsProbe;
import com.gigaspaces.internal.oshi.OshiChecker;
import oshi.SystemInfo;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;
import java.lang.management.*;
import java.util.List;

@com.gigaspaces.api.InternalApi
public class OshiJVMStatisticsProbe  implements JVMStatisticsProbe {
    private static RuntimeMXBean runtimeMXBean;

    private static MemoryMXBean memoryMXBean;

    private static ThreadMXBean threadMXBean;

    private static OperatingSystem oshiOperatingSystem;

    static {
        runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        memoryMXBean = ManagementFactory.getMemoryMXBean();
        threadMXBean = ManagementFactory.getThreadMXBean();
        SystemInfo oshiSystemInfo = OshiChecker.getSystemInfo();
        oshiOperatingSystem = oshiSystemInfo.getOperatingSystem();
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

        OSProcess osProcess = oshiOperatingSystem.getProcess(oshiOperatingSystem.getProcessId());

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
