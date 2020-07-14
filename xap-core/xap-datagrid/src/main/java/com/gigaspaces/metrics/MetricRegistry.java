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

import com.gigaspaces.logger.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Niv Ingberg
 * @since 10.1
 */

public class MetricRegistry {

    private final String name;
    private final Logger logger;
    private final Map<MetricTags, MetricGroup> groups;

    /**
     * Creates a new {@link MetricRegistry}.
     */
    public MetricRegistry(String name) {
        this.name = name;
        this.logger = LoggerFactory.getLogger(Constants.LOGGER_METRICS_REGISTRY + '.' + name);
        this.groups = new ConcurrentHashMap<>();
    }

    public String getName() {
        return name;
    }

    public Map<MetricTags, MetricGroup> getGroups() {
        return groups;
    }

    /**
     * Given a {@link Gauge}, registers it under the given name.
     *
     * @param name   the name of the metric
     * @param metric the metric
     * @throws IllegalArgumentException if the name is already registered
     */
    public void register(String name, MetricTags tags, Metric metric) {
        if (logger.isDebugEnabled())
            logger.debug("Registering " + name + "[tags=" + tags.getTags() + "]");

        if (!verifyMetric(name, metric))
            return;

        synchronized (groups) {
            MetricGroup group = groups.get(tags);
            if (group == null) {
                group = new MetricGroup();
                groups.put(tags, group);
            }
            group.register(name, metric);
        }
    }

    private boolean verifyMetric(String name, Metric metric) {
        try {
            if (metric instanceof Gauge) {
                Gauge gauge = (Gauge) metric;
                Object value = gauge.getValue();
                if (logger.isDebugEnabled())
                    logger.debug("Verified gauge " + name + " => " + value);
            } else {
                if (!(metric instanceof LongCounter) && !(metric instanceof ThroughputMetric))
                    throw new IllegalArgumentException("Unsupported metric type: " + metric.getClass().getName());
            }
            return true;
        } catch (Exception e) {
            if (logger.isWarnEnabled())
                logger.warn("Registration of metric '" + name + "' was skipped because its value cannot be retrieved - " + e.getMessage(), e);
            return false;
        }
    }

    public void remove(String metricName, MetricTags tags) {
        synchronized (groups) {
            MetricGroup group = groups.get(tags);
            if (group != null) {
                group.remove(metricName);
                if (group.isEmpty())
                    groups.remove(tags);
            }
        }
    }

    public void removeByPrefix(String prefix, MetricTags tags) {
        synchronized (groups) {
            MetricGroup group = groups.get(tags);
            if (group != null) {
                group.removeByPrefix(prefix);
                if (group.isEmpty())
                    groups.remove(tags);
            }
        }
    }

    public Map<String, Object> getSnapshotsByPrefix(String prefix) {

        synchronized (groups) {
            Map<String,Object> metricsSnapshot = new HashMap<>();
            for( MetricGroup group : groups.values() ) {
                if (group != null) {
                    Map<String, Metric> metricsFilteredByPrefix = group.getByPrefix(prefix);
                    Set<Map.Entry<String, Metric>> entries = metricsFilteredByPrefix.entrySet();
                    for (Map.Entry<String, Metric> entry : entries) {
                        try {
                            Object metricSnapshotValue = getMetricSnapshot(entry.getValue());
                            if( metricSnapshotValue != null ) {
                                metricsSnapshot.put(entry.getKey(), metricSnapshotValue);
                            }
                        } catch (Exception e) {
                            if (logger.isDebugEnabled()) {
                                logger.debug(e.toString(), e);
                            }
                        }
                    }
                }
            }
            return metricsSnapshot;
        }
    }

    private Object getMetricSnapshot( Metric metric ) throws Exception{

        Object resultVal = null;
        if( metric instanceof Gauge ){
            resultVal = ( ( Gauge )metric ).getValue();
        }
        else if( metric instanceof LongCounter ){
            resultVal = ( ( LongCounter )metric ).getCount();
        }
        else if( metric instanceof ThroughputMetric ){
            resultVal = ( ( ThroughputMetric )metric ).getTotal();
        }

        return resultVal;
    }

    public boolean isEmpty() {
        return groups.isEmpty();
    }

    public MetricRegistrySnapshot snapshot(long timestamp) {
        Map<MetricTagsSnapshot, MetricGroupSnapshot> groupsSnapshots = new HashMap<>(groups.size());
        for (Map.Entry<MetricTags, MetricGroup> groupEntry : groups.entrySet())
            groupsSnapshots.put(groupEntry.getKey().snapshot(), groupEntry.getValue().snapshot());
        return new MetricRegistrySnapshot(timestamp, groupsSnapshots);
    }
}
