package com.gigaspaces.internal.os;

import com.gigaspaces.internal.oshi.OshiChecker;
import com.gigaspaces.internal.oshi.OshiUtils;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;


public class OshiOSStatisticsProbe implements OSStatisticsProbe {

    @Override
    public OSStatistics probeStatistics() throws Exception {
        SystemInfo oshiSystemInfo = OshiChecker.getSystemInfo();
        HardwareAbstractionLayer hardwareAbstractionLayer = oshiSystemInfo.getHardware();

        GlobalMemory memory = hardwareAbstractionLayer.getMemory();
        CentralProcessor processor = oshiSystemInfo.getHardware().getProcessor();

        return new OSStatistics(System.currentTimeMillis(),
                OshiUtils.calcFreeSwapMemory(memory),
                memory.getAvailable(),
                memory.getAvailable(),
                processor.getSystemCpuLoad(),
                OshiUtils.getActualUsedMemory(memory),
                OshiUtils.getUsedMemoryPerc(memory),
                OshiUtils.calcNetStats());
    }

}
