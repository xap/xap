package com.gigaspaces.metrics.factories;

import com.gigaspaces.internal.os.ProcessCpuSampler;
import com.gigaspaces.metrics.Gauge;
import com.gigaspaces.metrics.internal.GaugeContextProvider;
import com.gigaspaces.metrics.internal.InternalGauge;

/**
 * @author Niv Ingberg
 * @since 15.5.1
 */
public class DefaultProcessMetricFactory implements ProcessMetricFactory {

    private final GaugeContextProvider<Long> context;
    private ProcessCpuSampler sampler;

    public DefaultProcessMetricFactory(ProcessCpuSampler sampler) {
        this.sampler = sampler;
        this.context = new GaugeContextProvider<Long>() {
            @Override
            protected Long loadValue() {
                return sampler.sampleTotalCpuTime();
            }
        };
    }

    @Override
    public Gauge<Long> createProcessCpuTotalTimeGauge() {
        return new InternalGauge<Long>(context) {
            @Override
            public Long getValue() {
                return context.get();
            }
        };
    }

    @Override
    public Gauge<Double> createProcessUsedCpuInPercentGauge() {
        return new InternalGauge<Double>(context) {

            private long previousTime;
            private long previousCpuTime;
            private double previousCpuPerc;

            @Override
            public Double getValue() {
                final long currTime = System.currentTimeMillis();
                final long currCpuTime = context.get();

                long timeDiff = currTime - previousTime;
                long cpuTimeDiff = currCpuTime - previousCpuTime;
                double cpuPerc = sampler.getCpuLoadCumulative(cpuTimeDiff, timeDiff);
                double currCpuPerc = cpuPerc > 0 ? cpuPerc : previousCpuPerc;

                previousTime = currTime;
                previousCpuTime = currCpuTime;
                previousCpuPerc = currCpuPerc;

                return currCpuPerc;
            }
        };
    }

    public void reset() {
        context.reset();
    }
}
