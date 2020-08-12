package com.gigaspaces.internal.os;

import com.gigaspaces.internal.oshi.OshiChecker;
import com.gigaspaces.internal.oshi.OshiUtils;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;

import java.util.concurrent.TimeUnit;


public class OshiOSStatisticsProbe implements OSStatisticsProbe {
    SystemInfo oshiSystemInfo = OshiChecker.getSystemInfo();
    HardwareAbstractionLayer hardwareAbstractionLayer = oshiSystemInfo.getHardware();
    CentralProcessor processor = hardwareAbstractionLayer.getProcessor();
    long[] oldCpuTicks = processor.getSystemCpuLoadTicks();
    GlobalMemory memory = hardwareAbstractionLayer.getMemory();

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
