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

package com.gigaspaces.metrics;

import com.gigaspaces.logger.Constants;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A metric which measures total and throughput values using sampling.
 *
 * @author Niv Ingberg
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class ThroughputMetric extends Metric {
    private static final double NANOSEC_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
    private static final Logger logger = LoggerFactory.getLogger(Constants.LOGGER_METRICS_MANAGER);
    private final LongCounter total = new LongCounter();
    // TODO: Doc why this is thread-safe for our use case.
    private long prevTotal;
    private long prevTime = System.nanoTime();

    /**
     * Mark the occurrence of an event.
     */
    public void increment() {
        add(1);
    }

    /**
     * Mark the occurrence of a given number of events.
     *
     * @param n the number of events
     */
    public void add(long n) {
        total.inc(n);
    }

    /**
     * Returns the total number of events marked.
     *
     * @return the total number of events.
     */
    public long getTotal() {
        return total.getCount();
    }

    /**
     * Samples the throughput (events per second) since the previous call to sampleThroughput.
     *
     * @return the average throughput since the last call to sampleThroughput
     */
    public double sampleThroughput() {
        final long total = getTotal();
        final long delta = total - prevTotal;
        prevTotal = total;

        final long currTime = System.nanoTime();
        final long elapsedTime = currTime - prevTime;
        // TODO: Doc why elapsedTime is never zero.
        prevTime = currTime;

        double result = delta / (elapsedTime / NANOSEC_PER_SECOND);
        if (logger.isTraceEnabled())
            logger.trace("delta=" + delta + ", period=" + (elapsedTime / NANOSEC_PER_SECOND) + "s, TP=" + result);
        return result;
    }

    public Metric getTotalMetric() {
        return total;
    }
}
