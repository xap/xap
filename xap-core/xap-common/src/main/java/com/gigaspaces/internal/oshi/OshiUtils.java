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
package com.gigaspaces.internal.oshi;

import com.gigaspaces.internal.os.OSStatistics;
import com.gigaspaces.internal.utils.LazySingleton;
import oshi.hardware.*;
import oshi.software.os.OperatingSystem;

import java.util.List;

public class OshiUtils {

    private static final OperatingSystem operatingSystem = OshiChecker.getSystemInfo().getOperatingSystem();
    private static final HardwareAbstractionLayer hardware = OshiChecker.getSystemInfo().getHardware();
    private static final LazySingleton<List<NetworkIF>> networkIFs = new LazySingleton<>(hardware::getNetworkIFs);

    public static OperatingSystem getOperatingSystem() {
        return operatingSystem;
    }

    public static HardwareAbstractionLayer getHardware() {
        return hardware;
    }

    public static List<NetworkIF> getNetworkIFs() {
        return networkIFs.getOrCreate();
    }

    public static double getUsedMemoryPerc(GlobalMemory memory) {
        long usedMemory = getActualUsedMemory(memory);

        return ((double) usedMemory/memory.getTotal())*100;
    }

    public static long getActualUsedMemory(GlobalMemory memory) {
        return memory.getTotal() - memory.getAvailable();
    }

    public static long calcFreeSwapMemory(GlobalMemory memory) {
        VirtualMemory virtualMemory = memory.getVirtualMemory();
        return virtualMemory.getSwapTotal() - virtualMemory.getSwapUsed();
    }

    public static OSStatistics.OSNetInterfaceStats[] calcNetStats(List<NetworkIF> networkIFs) {
        OSStatistics.OSNetInterfaceStats[] netInterfaceConfigArray = new
                OSStatistics.OSNetInterfaceStats[networkIFs.size()];

        for (int index = 0; index < netInterfaceConfigArray.length; index++) {
            NetworkIF networkIF = networkIFs.get(index);
            networkIF.updateAttributes();
            netInterfaceConfigArray[index] = getStats(networkIF);
        }
        return netInterfaceConfigArray;
    }

    public static OSStatistics.OSNetInterfaceStats getStats(NetworkIF networkIF) {
        return new OSStatistics.OSNetInterfaceStats(networkIF.getName(),
                networkIF.getBytesRecv(), networkIF.getBytesSent(),
                networkIF.getPacketsRecv(), networkIF.getPacketsSent(),
                networkIF.getInErrors(), networkIF.getOutErrors(),
                -1,-1
                //this data is missing in Oshi - we deprecated it from the API
        );
    }

    public static double getSystemCpuLoadBetweenTicks(long[] oldTicks,long[] newTicks) {

        // Calculate total
        long total = 0;
        for (int i = 0; i < newTicks.length; i++) {
            total += newTicks[i] - oldTicks[i];
        }
        // Calculate idle from difference in idle and IOwait
        long idle = newTicks[CentralProcessor.TickType.IDLE.getIndex()] + newTicks[CentralProcessor.TickType.IOWAIT.getIndex()]
                - oldTicks[CentralProcessor.TickType.IDLE.getIndex()] - oldTicks[CentralProcessor.TickType.IOWAIT.getIndex()];

        return total > 0 && idle >= 0 ? (double) (total - idle) / total : 0d;
    }
}
