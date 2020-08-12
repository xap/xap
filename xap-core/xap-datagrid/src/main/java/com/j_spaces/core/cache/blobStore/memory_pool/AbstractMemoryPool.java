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

    protected String percent() {
        return metricRegistrator.toPath("used-percent");
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
