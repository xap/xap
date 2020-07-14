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

package com.gigaspaces.metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class MetricGroup {
    private final Map<String, Metric> metrics = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Gauge> gauges = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongCounter> counters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ThroughputMetric> tpMetrics = new ConcurrentHashMap<>();

    Map<String, Metric> getMetrics() {
        return metrics;
    }

    public void register(String name, Metric metric) {
        if (metrics.containsKey(name))
            throw new IllegalArgumentException("A metric named " + name + " already exists");
        metrics.put(name, metric);
        Map<String, Metric> metricMap = getMetricMapByType(metric);
        if (metricMap != null)
            metricMap.put(name, metric);
    }

    @SuppressWarnings("unchecked")
    protected <T extends Metric> ConcurrentHashMap<String, T> getMetricMapByType(T metric) {
        if (metric instanceof Gauge)
            return (ConcurrentHashMap<String, T>) gauges;
        if (metric instanceof LongCounter)
            return (ConcurrentHashMap<String, T>) counters;
        if (metric instanceof ThroughputMetric)
            return (ConcurrentHashMap<String, T>) tpMetrics;
        return null;
    }

    public boolean isEmpty() {
        return gauges.isEmpty() && counters.isEmpty() && tpMetrics.isEmpty();
    }

    public MetricGroupSnapshot snapshot() {
        Map<String, Object> metricsValues = new HashMap<>(gauges.size() + counters.size() + tpMetrics.size());
        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            try {
                Object value = entry.getValue().getValue();
                if( value != null ) {
                    metricsValues.put(entry.getKey(), value);
                }
            } catch (Exception e) {
                // TODO: Consider logging this exception.
            }
        }

        for (Map.Entry<String, LongCounter> entry : counters.entrySet())
            metricsValues.put(entry.getKey(), entry.getValue().getCount());

        for (Map.Entry<String, ThroughputMetric> entry : tpMetrics.entrySet())
            metricsValues.put(entry.getKey(), entry.getValue().sampleThroughput());

        return new MetricGroupSnapshot(metricsValues);
    }

    public void remove(String metricName) {
        Metric metric = metrics.remove(metricName);
        if (metric != null) {
            Map<String, Metric> metricMap = getMetricMapByType(metric);
            if (metricMap != null)
                metricMap.remove(metricName);
        }
    }

    public void removeByPrefix(String prefix) {
        Collection<String> metricsToRemove = new ArrayList<>();
        for (String name : metrics.keySet())
            if (name.startsWith(prefix))
                metricsToRemove.add(name);

        for (String name : metricsToRemove)
            remove(name);
    }

    public Map<String, Metric> getByPrefix(String prefix) {
        Map<String, Metric> metricsToReturn = new HashMap<>();
        for (Map.Entry<String, Metric> entry : metrics.entrySet()) {
            String name = entry.getKey();
            if (name.startsWith(prefix)) {
                metricsToReturn.put(name, entry.getValue());
            }
        }
        return metricsToReturn;
    }
}