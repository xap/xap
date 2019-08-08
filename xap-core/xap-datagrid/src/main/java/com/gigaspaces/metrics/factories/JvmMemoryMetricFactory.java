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

import com.gigaspaces.metrics.Gauge;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 10.1
 */

public class JvmMemoryMetricFactory {

    private final MemoryMXBean mxBean = ManagementFactory.getMemoryMXBean();

    public Gauge<Long> createHeapUsedInBytesGauge() {
        return new Gauge<Long>() {
            @Override
            public Long getValue() {
                return mxBean.getHeapMemoryUsage().getUsed();
            }
        };
    }

    public Gauge<Double> createHeapUsedInPercentGauge() {
        return new Gauge<Double>() {
            @Override
            public Double getValue() {
                return (double)mxBean.getHeapMemoryUsage().getUsed()/mxBean.getHeapMemoryUsage().getMax();
            }
        };
    }

    public Gauge<Long> createHeapCommittedInBytesGauge() {
        return new Gauge<Long>() {
            @Override
            public Long getValue() {
                return mxBean.getHeapMemoryUsage().getCommitted();
            }
        };
    }

    public Gauge<Long> createNonHeapUsedInBytesGauge() {
        return new Gauge<Long>() {
            @Override
            public Long getValue() {
                return mxBean.getNonHeapMemoryUsage().getUsed();
            }
        };
    }

    public Gauge<Long> createNonHeapCommittedInBytesGauge() {
        return new Gauge<Long>() {
            @Override
            public Long getValue() {
                return mxBean.getNonHeapMemoryUsage().getCommitted();
            }
        };
    }

    public Gauge<Long> createCGCountGauge() {
        return new Gauge<Long>() {
            @Override
            public Long getValue() {
                long gcCollectionCount = 0;
                List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
                for (GarbageCollectorMXBean gcMxBean : gcMxBeans) {
                    long tmp = gcMxBean.getCollectionCount();
                    if (tmp != -1) {
                        gcCollectionCount += tmp;
                    }
                }
                return gcCollectionCount;
            }
        };
    }

    public Gauge<Long> createGCCollectionTimeGauge() {
        return new Gauge<Long>() {
            @Override
            public Long getValue() {
                long gcCollectionTime = 0;
                List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
                for (GarbageCollectorMXBean gcMxBean : gcMxBeans) {
                    long tmp = gcMxBean.getCollectionTime();
                    if (tmp != -1) {
                        gcCollectionTime += tmp;
                    }
                }
                return gcCollectionTime;
            }
        };
    }
}
