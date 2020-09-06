package com.gigaspaces.metrics.factories;

import com.gigaspaces.metrics.Gauge;

/**
 * @author Niv Ingberg
 * @since 15.5.1
 */
public interface ProcessMetricFactory {
    Gauge<Long> createProcessCpuTotalTimeGauge();

    Gauge<Double> createProcessUsedCpuInPercentGauge();
}
