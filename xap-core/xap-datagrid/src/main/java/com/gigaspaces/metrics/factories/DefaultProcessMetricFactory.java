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

    public DefaultProcessMetricFactory(ProcessCpuSampler sampler) {
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
                double cpuTimeDiff = currCpuTime - previousCpuTime;
                double currCpuPerc = timeDiff > 0 && cpuTimeDiff >= 0 ?
                        (cpuTimeDiff / timeDiff) / numberOfCores()
                        : previousCpuPerc;

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
