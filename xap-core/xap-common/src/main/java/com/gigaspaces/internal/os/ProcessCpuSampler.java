package com.gigaspaces.internal.os;

import com.gigaspaces.internal.utils.GsEnv;

/**
 * @author Niv Ingberg
 * @since 15.5.1
 */
public interface ProcessCpuSampler {
    /**
     * Indicates sample is not available.
     */
    long NA = -1;

    /**
     * Gets the number of milliseconds the process has executed in kernel or user mode.
     */
    long sampleTotalCpuTime();

    /**
     * Gets CPU usage of this process as a fraction of 100%.
     * <p>
     * This calculation sums CPU ticks across all processors and may exceed 100% for
     * multi-threaded processes. This is consistent with the cumulative CPU
     * presented by the "top" command on Linux/Unix machines. This value is divided
     * by the {@link #numberOfCores()} to return a fraction of 100%.
     *
     * @return The proportion of up time that the process was executing in kernel or
     *         user mode.
     */
    default double getCpuLoadCumulative(long totalCpuTime, long upTime) {
        return totalCpuTime > 0 && upTime >= 0 ?
                ((double) totalCpuTime / upTime) / numberOfCores()
                : NA;
    }

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
}
