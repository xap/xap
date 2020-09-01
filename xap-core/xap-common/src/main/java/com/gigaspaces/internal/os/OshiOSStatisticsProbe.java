package com.gigaspaces.internal.os;

import com.gigaspaces.internal.oshi.OshiChecker;
import com.gigaspaces.internal.oshi.OshiUtils;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;

public class OshiOSStatisticsProbe implements OSStatisticsProbe {
    CentralProcessor processor = OshiChecker.getHardware().getProcessor();
    long[] oldCpuTicks = processor.getSystemCpuLoadTicks();
    GlobalMemory memory = OshiChecker.getHardware().getMemory();

    @Override
    public OSStatistics probeStatistics() throws Exception {

        long[] newCpuTicks = processor.getSystemCpuLoadTicks();
        double systemCpuLoadBetweenTicks = OshiUtils.getSystemCpuLoadBetweenTicks(oldCpuTicks,newCpuTicks);
        oldCpuTicks = newCpuTicks;

        return new OSStatistics(System.currentTimeMillis(),
                OshiUtils.calcFreeSwapMemory(memory),
                memory.getAvailable(),
                memory.getAvailable(),
                systemCpuLoadBetweenTicks,
                OshiUtils.getActualUsedMemory(memory),
                OshiUtils.getUsedMemoryPerc(memory),
                OshiUtils.calcNetStats());
    }

}
