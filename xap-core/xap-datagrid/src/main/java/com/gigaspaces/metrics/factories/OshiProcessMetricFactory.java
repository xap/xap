package com.gigaspaces.metrics.factories;

import com.gigaspaces.internal.jvm.JavaUtils;
import com.gigaspaces.internal.oshi.OshiUtils;
import com.gigaspaces.metrics.Gauge;
import com.gigaspaces.metrics.internal.GaugeContextProvider;
import com.gigaspaces.metrics.internal.InternalGauge;

/**
 * @author Niv Ingberg
 * @since 15.5.1
 */
public class OshiProcessMetricFactory implements ProcessMetricFactory {

    private final ProcessCpuWrapper context = new ProcessCpuWrapper();

    private static class ProcessCpuWrapper extends GaugeContextProvider<Long> {
        private final static int pid = (int) JavaUtils.getPid();

        @Override
        protected Long loadValue() {
            return OshiUtils.getProcessCpuTime(pid);
        }
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

            public long previousTime;
            public long previousCpuTime;
            public double previousCpuPerc;

            @Override
            public Double getValue() {
                final long currTime = System.currentTimeMillis();
                final long currCpuTime = context.get();

                long timeDelta = currTime - previousTime;
                long totalDelta = currCpuTime - previousCpuTime;
                double cpuPerc = timeDelta > 0 && totalDelta >= 0 ? Math.min(((double) totalDelta) / timeDelta, 1.0) : previousCpuPerc;

                previousTime = currTime;
                previousCpuTime = currCpuTime;
                previousCpuPerc = cpuPerc;

                return cpuPerc;
            }
        };
    }
}
