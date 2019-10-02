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
package com.j_spaces.core.cache.blobStore.memory_pool;

import com.gigaspaces.metrics.LongCounter;
import com.gigaspaces.metrics.MetricRegistrator;
import com.j_spaces.core.cache.blobStore.IBlobStoreOffHeapInfo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Yael Nahon
 * @since 12.3
 */
public abstract class AbstractMemoryPool {
    protected final long threshold;
    private MetricRegistrator metricRegistrator;

    protected AbstractMemoryPool(long threshold) {
        this.threshold = threshold;
    }

    protected String metricsPath(String typeName) {
        return metricRegistrator.toPath("used-bytes", typeName);
    }

    public long getThreshold() {
        return threshold;
    }

    public MetricRegistrator getMetricRegistrator() {
        return metricRegistrator;
    }

    public void setMetricRegistrator(MetricRegistrator metricRegistrator) {
        this.metricRegistrator = metricRegistrator;
    }

    public abstract void initMetrics(MetricRegistrator metricRegistrator);

    public abstract void register(String typeName, short typeCode);

    public abstract void unregister(String typeName, short typeCode);

    public abstract long getUsedBytes();

    public abstract void write(IBlobStoreOffHeapInfo info, byte[] buf);

    public abstract byte[] get(IBlobStoreOffHeapInfo info);

    public abstract void update(IBlobStoreOffHeapInfo info, byte[] buf);

    public abstract void delete(IBlobStoreOffHeapInfo info);

    public abstract boolean isPmem();

    public abstract boolean isOffHeap();

    public abstract void close();
}
