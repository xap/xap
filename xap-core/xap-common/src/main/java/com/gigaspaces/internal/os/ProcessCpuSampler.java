package com.gigaspaces.internal.os;

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
     * Gets cumulative CPU usage of this process.
     * <p>
     * This calculation sums CPU ticks across all processors and may exceed 100% for
     * multi-threaded processes. This is consistent with the cumulative CPU
     * presented by the "top" command on Linux/Unix machines.
     *
     * @return The proportion of up time that the process was executing in kernel or
     *         user mode.
     */
    default double getCpuLoadCumulative(long totalCpuTime, long upTime) {
        return totalCpuTime != NA && upTime != 0 ? totalCpuTime / (double) upTime : NA;
    }
}
