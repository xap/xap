package com.gigaspaces.metrics.factories;

import com.gigaspaces.internal.utils.GsEnv;
import com.gigaspaces.metrics.Gauge;

/**
 * @author Niv Ingberg
 * @since 15.5.1
 */
public interface ProcessMetricFactory {

    int cores = GsEnv.propertyInt("com.gs.metric.process.cpu.cores")
            .get(Runtime.getRuntime().availableProcessors());

    /**
     * The CPU process usage is divided by the number of cores to get a fraction of 100%.
     * This is the default behavior.
     * <br><br>
     * In Windows the CPU usage counts all cores and hyperthreading as a fraction of 100%,
     * and that results in a value less than a 100% for a single process.
     * <br><br>
     * In Linux/CentOS/MacOS, each thread can consume 100% of a core or hyperthread,
     * and CPU usage of multiple threads per process results in a value of more than 100%.
     * To get this behavior, set 'com.gs.metric.process.cpu.cores' to 1.
     * <br><br>
     *
     * @return The number of available cores to consider when dividing the CPU usage by.
     */
    default int numberOfCores() {
        return cores;
    };

    Gauge<Long> createProcessCpuTotalTimeGauge();

    /**
     * @return A fraction of CPU used by the process
     */
    Gauge<Double> createProcessUsedCpuInPercentGauge();
}
