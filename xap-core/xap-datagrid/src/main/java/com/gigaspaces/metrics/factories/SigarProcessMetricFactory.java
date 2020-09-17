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


package com.gigaspaces.metrics.factories;

import com.gigaspaces.internal.os.ProcessCpuSampler;
import com.gigaspaces.internal.sigar.SigarHolder;
import com.gigaspaces.metrics.Gauge;
import com.gigaspaces.metrics.internal.GaugeContextProvider;
import com.gigaspaces.metrics.internal.InternalGauge;
import org.hyperic.sigar.ProcCpu;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

/**
 * @author Barak Bar Orion
 * @since 10.1
 */

public class SigarProcessMetricFactory implements ProcessMetricFactory {

    private final SigarProcessCpuWrapper context = new SigarProcessCpuWrapper();

    private static class SigarProcessCpuWrapper extends GaugeContextProvider<ProcCpu> {
        private final Sigar sigar = SigarHolder.getSigar();
        private final long pid = sigar.getPid();

        @Override
        protected ProcCpu loadValue() {
            try {
                return sigar.getProcCpu(pid);
            } catch (SigarException e) {
                throw new RuntimeException("Failed to get process cpu info from sigar", e);
            }
        }
    }

    public void reset() {
        context.reset();
    }

    @Override
    public Gauge<Long> createProcessCpuTotalTimeGauge() {
        return new InternalGauge<Long>(context) {
            @Override
            public Long getValue() {
                return context.get().getTotal();
            }
        };
    }

    @Override
    public Gauge<Double> createProcessUsedCpuInPercentGauge() {
        return new InternalGauge<Double>(context) {
            @Override
            public Double getValue() {
                return validate(context.get().getPercent() / ProcessCpuSampler.cores);
            }
        };
    }
}
