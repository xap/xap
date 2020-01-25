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
