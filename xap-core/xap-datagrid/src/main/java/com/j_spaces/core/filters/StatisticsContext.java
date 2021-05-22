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


package com.j_spaces.core.filters;

import com.gigaspaces.metrics.MetricRegistrator;
import com.gigaspaces.metrics.ThroughputMetric;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Representation of an operation statistics object.
 *
 * @author Michael Konnikov
 * @version 4.1
 * @see com.j_spaces.core.admin.StatisticsAdmin
 */
public class StatisticsContext implements SmartExternalizable {
    private static final long serialVersionUID = 1L;

    private final ThroughputMetric metric;
    private volatile long previousCount;
    private double average;

    /**
     * Default constructor.
     */
    public StatisticsContext() {
        this.metric = new ThroughputMetric();
    }

    /**
     * The average calculated on the last time according to the statistics sampling rate.
     *
     * @return the average of operation per second.
     * @see com.j_spaces.core.admin.StatisticsAdmin#getStatisticsSamplingRate()
     */
    public double getAverage() {
        return average;
    }

    /**
     * Current count of operations.
     *
     * @return the count of operations
     */
    public long getCurrentCount() {
        return metric.getTotal();
    }

    /**
     * For internal usage only
     */
    public void add(long value) {
        metric.add(value);
    }

    /**
     * String representation of the statistics. For example: count: 1010, average time (ms): 3.2
     *
     * @return the string representation
     */
    @Override
    public String toString() {
        return "count: " + metric.getTotal() + ", average time (op/seconds): " + average;
    }

    void register(MetricRegistrator registrator, String name) {
        registrator.register(name + "-tp", metric);
        registrator.register(name + "-total", metric.getTotalMetric());
    }

    void increment() {
        metric.increment();
    }

    String report(Integer operationCode, long period) {
        String operation = FilterOperationCodes.operationCodeToString(operationCode);
        return operation.substring(operation.indexOf('_') + 1) +
                " count: " + metric.getTotal() +
                ", average throughput for last " + (period / 1000) +
                " seconds is " + average;
    }

    /**
     * Calculate the average operations for specified time period and updated the previous
     * operations count.
     *
     * @param period the period to calculate the average
     */
    void calculateAverage(long period) {
        final long count = metric.getTotal();
        if (count != 0) {
            average = (count - previousCount) / (period / 1000.0);
            previousCount = count;
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(metric.getTotal());
        out.writeLong(previousCount);
        out.writeDouble(average);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // HACK: Since metric's counter does not support set(), we use add() instead, assuming it's initialized to zero by default.
        metric.add(in.readLong());
        previousCount = in.readLong();
        average = in.readDouble();
    }
}
