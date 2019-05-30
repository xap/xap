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
import oshi.SystemInfo;
import oshi.hardware.GlobalMemory;
import oshi.hardware.NetworkIF;

import java.net.NetworkInterface;

public class OshiUtils {

    public static SystemInfo oshiSystemInfo = OshiChecker.getSystemInfo();
    public final static GlobalMemory memory = oshiSystemInfo.getHardware().getMemory();

    public static double getUsedMemoryPerc(GlobalMemory memory) {
        long usedMemory = getActualUsedMemory(memory);

        return ((double) usedMemory/memory.getTotal())*100;
    }

    public static long getActualUsedMemory(GlobalMemory memory) {
        return memory.getTotal() - memory.getAvailable();
    }

    public static long calcFreeSwapMemory(GlobalMemory memory) {
        return memory.getSwapTotal() - memory.getSwapUsed();
    }

    public static OSStatistics.OSNetInterfaceStats[] calcNetStats() {
        NetworkIF[] networkIFs = oshiSystemInfo.getHardware().getNetworkIFs();
        OSStatistics.OSNetInterfaceStats[] netInterfaceConfigArray = new
                OSStatistics.OSNetInterfaceStats[networkIFs.length];

        for (int index = 0; index < networkIFs.length; index++) {
            NetworkIF networkIF = networkIFs[index];
            NetworkInterface netInterface = networkIF.getNetworkInterface();

            OSStatistics.OSNetInterfaceStats netInterfaceStats = new OSStatistics.OSNetInterfaceStats(networkIF.getName(),
                    networkIF.getBytesRecv(), networkIF.getBytesSent(),
                    networkIF.getPacketsRecv(), networkIF.getPacketsSent(),
                    networkIF.getInErrors(), networkIF.getOutErrors(),
                    -1,-1
                    //this data is missing in Oshi - we deprecated it from the API
            );
            netInterfaceConfigArray[index] = netInterfaceStats;
        }
        return netInterfaceConfigArray;
    }
}
