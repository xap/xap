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

import com.gigaspaces.internal.sigar.SigarHolder;
import com.gigaspaces.metrics.Gauge;
import com.gigaspaces.metrics.internal.GaugeContextProvider;
import com.gigaspaces.metrics.internal.InternalGauge;

import org.hyperic.sigar.CpuPerc;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

/**
 * @author Niv Ingberg
 * @since 10.1
 */

public class SigarCpuMetricFactory {

    private final SigarCpuWrapper context = new SigarCpuWrapper();

    private static class SigarCpuWrapper extends GaugeContextProvider<CpuPerc> {
        private final Sigar sigar = SigarHolder.getSigar();

        @Override
        protected CpuPerc loadValue() {
            try {
                return sigar.getCpuPerc();
            } catch (SigarException e) {
                throw new RuntimeException("Failed to get cpu info from sigar", e);
            }
        }
    }

    public Gauge<Double> createUsedCpuInPercentGauge() {
        return new InternalGauge<Double>(context) {
            @Override
            public Double getValue() throws SigarException {
                return validate(context.get().getCombined())*100;
            }
        };
    }
}
