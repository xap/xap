package com.gigaspaces.internal.oshi;

import com.gigaspaces.internal.os.OSStatistics;
import com.gigaspaces.metrics.Gauge;

import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.VirtualMemory;
import oshi.software.os.OSProcess;

import java.util.concurrent.TimeUnit;

public class OshiGaugeUtils {

    public final static SystemInfo oshiSystemInfo = OshiChecker.getSystemInfo();
    public final static CentralProcessor processor = oshiSystemInfo.getHardware().getProcessor();
    public final static GlobalMemory memory = oshiSystemInfo.getHardware().getMemory();
    public final static VirtualMemory virtualMemory = memory.getVirtualMemory();
    public final static int pid = oshiSystemInfo.getOperatingSystem().getProcessId();
    public final static OSProcess osProcess = oshiSystemInfo.getOperatingSystem().getProcess(pid);

    public static long previousCpuTime;
    public static long previousCpuTotal;
    public static double previousCpuPerc;

    public static Gauge<Double> getCpuPercGauge() {
        return new Gauge<Double>() {
            private long[] oldCpuTicks = processor.getSystemCpuLoadTicks();

            @Override
            public Double getValue() throws Exception {
                long[] newCpuTicks = processor.getSystemCpuLoadTicks();
                double systemCpuLoadBetweenTicks = OshiUtils.getSystemCpuLoadBetweenTicks(oldCpuTicks,newCpuTicks);
                oldCpuTicks = newCpuTicks;

                return systemCpuLoadBetweenTicks;
            }
        };
    }

    public static Gauge<Long> getFreeMemoryInBytesGauge() {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return memory.getAvailable();
            }
        };
    }

    public static Gauge<Long> getActualFreeMemoryInBytesGauge() {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return memory.getAvailable();
            }
        };
    }

    public static Gauge<Long> getUsedMemoryInBytesGauge() {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return OshiUtils.getActualUsedMemory(memory);
            }
        };
    }

    public static Gauge<Long> getActualUsedMemoryInBytesGauge() {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return OshiUtils.getActualUsedMemory(memory);
            }
        };
    }

    public static Gauge<Double> getUsedMemoryInPercentGauge() {
        return new Gauge<Double>() {
            @Override
            public Double getValue() throws Exception {
                return OshiUtils.getUsedMemoryPerc(memory);
            }
        };
    }

    public static Gauge<Long> getFreeSwapInBytesGauge() {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return OshiUtils.calcFreeSwapMemory(memory);
            }
        };
    }

    public static Gauge<Long> getUsedSwapInBytesGauge() {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return virtualMemory.getSwapUsed();
            }
        };
    }

    public static Gauge<Double> getUsedSwapInPercentGauge() {
        return new Gauge<Double>() {
            @Override
            public Double getValue() throws Exception {
                return virtualMemory.getSwapTotal() != 0 ?
                        (virtualMemory.getSwapUsed()/virtualMemory.getSwapTotal())*100d :
                        Double.NaN;
            }
        };
    }

    public static Gauge<Long> createRxBytesGauge(OSStatistics.OSNetInterfaceStats osNetInterfaceStats) {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return osNetInterfaceStats.getRxBytes();
            }
        };
    }

    public static Gauge<Long> createTxBytesGauge(OSStatistics.OSNetInterfaceStats osNetInterfaceStats) {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return osNetInterfaceStats.getTxBytes();
            }
        };
    }

    public static Gauge<Long> createRxPacketsGauge(OSStatistics.OSNetInterfaceStats osNetInterfaceStats) {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return osNetInterfaceStats.getRxPackets();
            }
        };
    }

    public static Gauge<Long> createTxPacketsGauge(OSStatistics.OSNetInterfaceStats osNetInterfaceStats) {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return osNetInterfaceStats.getTxPackets();
            }
        };
    }

    public static Gauge<Long> createRxErrorsGauge(OSStatistics.OSNetInterfaceStats osNetInterfaceStats) {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return osNetInterfaceStats.getRxErrors();
            }
        };
    }

    public static Gauge<Long> createTxErrorsGauge(OSStatistics.OSNetInterfaceStats osNetInterfaceStats) {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return osNetInterfaceStats.getTxErrors();
            }
        };
    }

    public static Gauge<Long> createRxDroppedGauge(OSStatistics.OSNetInterfaceStats osNetInterfaceStats) {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return osNetInterfaceStats.getRxDropped();
            }
        };
    }

    public static Gauge<Long> createTxDroppedGauge(OSStatistics.OSNetInterfaceStats osNetInterfaceStats) {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return osNetInterfaceStats.getTxDropped();
            }
        };
    }

    public static Gauge<Long> createProcessCpuTotalTimeGauge() {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return osProcess.getKernelTime() + osProcess.getUserTime();
            }
        };
    }

    public static Gauge<Double> createProcessUsedCpuInPercentGauge() {
        return new Gauge<Double>() {
            @Override
            public Double getValue() throws Exception {

                OSProcess osProcessLocal = oshiSystemInfo.getOperatingSystem().getProcess(pid);
                long currentCpuTime = System.currentTimeMillis();
                long currentCpuTotal = osProcessLocal.getKernelTime() + osProcessLocal.getUserTime();

                double cpuPerc = previousCpuPerc;

                long timeDelta = currentCpuTime - previousCpuTime;
                long totalDelta = currentCpuTotal - previousCpuTotal;

                if( timeDelta > 0 && totalDelta > 0 && totalDelta < timeDelta ) {
                    cpuPerc = ((double) totalDelta) / timeDelta;
                }

                previousCpuTime = currentCpuTime;
                previousCpuTotal = currentCpuTotal;
                previousCpuPerc = cpuPerc;

                return cpuPerc;
            }
        };
    }

}
