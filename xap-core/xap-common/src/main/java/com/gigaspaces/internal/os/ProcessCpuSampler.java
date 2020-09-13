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
