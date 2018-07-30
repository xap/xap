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
    private final LongCounter totalCounter = new LongCounter();
    private final Map<String, LongCounter> typesCounters = new ConcurrentHashMap<String, LongCounter>();
    private MetricRegistrator metricRegistrator;

    protected AbstractMemoryPool(long threshold) {
        this.threshold = threshold;
    }

    public void initMetrics(MetricRegistrator metricRegistrator) {
        this.metricRegistrator = metricRegistrator;
        this.metricRegistrator.register(metricsPath("total"), totalCounter);
    }

    public long getThreshold() {
        return threshold;
    }

    public void register(String typeName) {
        LongCounter counter = new LongCounter();
        typesCounters.put(typeName, counter);
        metricRegistrator.register(metricsPath(typeName), counter);
    }

    public void unregister(String typeName) {
        typesCounters.remove(typeName);
        metricRegistrator.unregisterByPrefix(metricsPath(typeName));
    }

    public long getUsedBytes() {
        return totalCounter.getCount();
    }

    private String metricsPath(String typeName) {
        return metricRegistrator.toPath("used-bytes", typeName);
    }

    protected void incrementMetrics(long n, String typeName) {
        totalCounter.inc(n);
        LongCounter typeCounter = typesCounters.get(typeName);
        if (typeCounter != null)
            typeCounter.inc(n);
    }

    protected void decrementMetrics(long n, String typeName) {
        totalCounter.dec(n);
        LongCounter typeCounter = typesCounters.get(typeName);
        if (typeCounter != null)
            typeCounter.dec(n);

    }

    public abstract void write(IBlobStoreOffHeapInfo info, byte[] buf);

    public abstract byte[] get(IBlobStoreOffHeapInfo info);

    public abstract void update(IBlobStoreOffHeapInfo info, byte[] buf);

    public abstract void delete(IBlobStoreOffHeapInfo info);

    public LongCounter getTotalCounter() {
        return totalCounter;
    }

    public Map<String, LongCounter> getTypesCounters() {
        return typesCounters;
    }
}
