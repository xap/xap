package com.gigaspaces.internal.oshi;

import com.gigaspaces.internal.os.OSStatistics;
import com.gigaspaces.metrics.Gauge;

import com.gigaspaces.metrics.MetricRegistrator;
import com.gigaspaces.metrics.internal.GaugeContextProvider;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.NetworkIF;
import oshi.hardware.VirtualMemory;

public class OshiGaugeUtils {

    private final static CentralProcessor processor = OshiUtils.getHardware().getProcessor();
    private final static GlobalMemory memory = OshiUtils.getHardware().getMemory();
    private final static VirtualMemory virtualMemory = memory.getVirtualMemory();

    public static Gauge<Double> getCpuPercGauge() {
        return new Gauge<Double>() {
            private long[] oldCpuTicks = processor.getSystemCpuLoadTicks();

            @Override
            public Double getValue() throws Exception {
                long[] newCpuTicks = processor.getSystemCpuLoadTicks();
                double systemCpuLoadBetweenTicks = OshiUtils.getSystemCpuLoadBetweenTicks(oldCpuTicks,newCpuTicks);
                oldCpuTicks = newCpuTicks;
                return systemCpuLoadBetweenTicks*100d;
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

    public static void registerNetworkMetrics(NetworkIF networkIF, MetricRegistrator networkRegistrator) {
        GaugeContextProvider<OSStatistics.OSNetInterfaceStats> context = new GaugeContextProvider<OSStatistics.OSNetInterfaceStats>() {
            @Override
            protected OSStatistics.OSNetInterfaceStats loadValue() {
                networkIF.updateAttributes();
                return OshiUtils.getStats(networkIF);
            }
        };
        networkRegistrator.register("rx-bytes", createRxBytesGauge(context));
        networkRegistrator.register("tx-bytes", createTxBytesGauge(context));
        networkRegistrator.register("rx-packets", createRxPacketsGauge(context));
        networkRegistrator.register("tx-packets", createTxPacketsGauge(context));
        networkRegistrator.register("rx-errors", createRxErrorsGauge(context));
        networkRegistrator.register("tx-errors", createTxErrorsGauge(context));
        // dropped stats are deprecated and only partially supported by Oshi
        //networkRegistrator.register("rx-dropped", createRxDroppedGauge(context));
        //networkRegistrator.register("tx-dropped", createTxDroppedGauge(context));

    }

    private static Gauge<Long> createRxBytesGauge(GaugeContextProvider<OSStatistics.OSNetInterfaceStats> context) {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return context.get().getRxBytes();
            }
        };
    }

    private static Gauge<Long> createTxBytesGauge(GaugeContextProvider<OSStatistics.OSNetInterfaceStats> context) {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return context.get().getTxBytes();
            }
        };
    }

    private static Gauge<Long> createRxPacketsGauge(GaugeContextProvider<OSStatistics.OSNetInterfaceStats> context) {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return context.get().getRxPackets();
            }
        };
    }

    private static Gauge<Long> createTxPacketsGauge(GaugeContextProvider<OSStatistics.OSNetInterfaceStats> context) {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return context.get().getTxPackets();
            }
        };
    }

    private static Gauge<Long> createRxErrorsGauge(GaugeContextProvider<OSStatistics.OSNetInterfaceStats> context) {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return context.get().getRxErrors();
            }
        };
    }

    private static Gauge<Long> createTxErrorsGauge(GaugeContextProvider<OSStatistics.OSNetInterfaceStats> context) {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return context.get().getTxErrors();
            }
        };
    }
}
