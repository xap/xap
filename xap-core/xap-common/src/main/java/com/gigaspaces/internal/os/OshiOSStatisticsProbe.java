package com.gigaspaces.internal.os;

import com.gigaspaces.internal.oshi.OshiUtils;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;

public class OshiOSStatisticsProbe implements OSStatisticsProbe {
    GlobalMemory memory = OshiUtils.getHardware().getMemory();
    CentralProcessor processor = OshiUtils.getHardware().getProcessor();
    long[] oldCpuTicks = processor.getSystemCpuLoadTicks();

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
                OshiUtils.calcNetStats(OshiUtils.getNetworkIFs()));
    }

}
