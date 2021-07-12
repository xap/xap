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
     * by the {@link #cores} to return a fraction of 100%.
     *
     * @return The proportion of up time that the process was executing in kernel or
     *         user mode.
     */
    default double getCpuLoadCumulative(long totalCpuTime, long upTime) {
        double cpuVal = totalCpuTime > 0 && upTime > 0 ?
                ((double) totalCpuTime / upTime) / cores
                : NA;
        return cpuVal == Double.POSITIVE_INFINITY ? NA : cpuVal;
    }

    /**
     * CPU utilization is expressed as a fraction of 100% (dividing by the number of CPU cores),
     * even though some vendors scale to N*100% if multiple CPUs (N CPUs) exist.
     * <br><br>
     * The default behavior is to divide the CPU utilization by the number of cores. To show the raw
     * utilization reported by the underline CPU probe set the following System property
     * <code>com.gs.process.cpu.utilization.divideByCores</code> with false.
     */
    int cores = GsEnv.propertyBoolean("com.gs.process.cpu.utilization.divideByCores").get(true)
            ? Runtime.getRuntime().availableProcessors() : 1;
}
