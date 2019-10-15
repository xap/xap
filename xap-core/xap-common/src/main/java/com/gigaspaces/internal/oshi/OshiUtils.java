package com.gigaspaces.internal.oshi;

import com.gigaspaces.internal.os.OSStatistics;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.NetworkIF;
import oshi.hardware.VirtualMemory;

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
        VirtualMemory virtualMemory = memory.getVirtualMemory();
        return virtualMemory.getSwapTotal() - virtualMemory.getSwapUsed();
    }

    public static OSStatistics.OSNetInterfaceStats[] calcNetStats() {
        NetworkIF[] networkIFs = oshiSystemInfo.getHardware().getNetworkIFs();
        OSStatistics.OSNetInterfaceStats[] netInterfaceConfigArray = new
                OSStatistics.OSNetInterfaceStats[networkIFs.length];

        for (int index = 0; index < networkIFs.length; index++) {
            NetworkIF networkIF = networkIFs[index];
            NetworkInterface netInterface = networkIF.queryNetworkInterface();

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
