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
     * Gets CPU utilization of this process as a fraction of 100%.
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

    /**
     * The CPU utilization is divided by the number of cores. To disable this default behavior, set
     * System property <code>com.gs.process.cpu.utilization.divideByCores</code> to false.
     */
    int cores = GsEnv.propertyBoolean("com.gs.process.cpu.utilization.divideByCores").get(true)
            ? Runtime.getRuntime().availableProcessors() : 1;

    /**
     * GigaSpaces expresses %CPU within a range from 0 to 100 on all machines even though some vendors
     * scale to N*100% if multiple CPUs (N CPUs) exist.
     * <br><br>
     * In Windows, the CPU utilization is defined as the fraction of time that the process
     * spends in kernel + user during the last sampling interval.
     * <br><br>
     * In Unix/MacOs, the CPU utilization is the same as "top" divided by the number of CPUs
     * (top shows utilization of a single CPU). It is the average utilization over the last sample period.
     * <br><br>
     *
     * @return The number of available cores to consider when dividing the CPU utilization by.
     */
    default int numberOfCores() {
        return cores;
    };
}
