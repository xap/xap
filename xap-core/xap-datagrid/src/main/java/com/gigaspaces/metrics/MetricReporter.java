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

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Base class for metric reporters.
 *
 * @author Niv Ingberg
 * @since 10.1
 */
public abstract class MetricReporter implements Closeable {

    private final Map<MetricTagsSnapshot, Map<String, String>> metricsNamesCache = new ConcurrentHashMap<>();

    protected MetricReporter(MetricReporterFactory factory) {
    }

    public void report(List<MetricRegistrySnapshot> snapshots) {
        for (MetricRegistrySnapshot snapshot : snapshots)
            report(snapshot);

        flush();
    }

    protected void report(MetricRegistrySnapshot snapshot) {
        for (Map.Entry<MetricTagsSnapshot, MetricGroupSnapshot> entry : snapshot.getGroups().entrySet())
            report(snapshot, entry.getKey(), entry.getValue());
    }

    protected void report(MetricRegistrySnapshot snapshot, MetricTagsSnapshot tags, MetricGroupSnapshot group) {
        for (Map.Entry<String, Object> entry : group.getMetricsValues().entrySet())
            report(snapshot, tags, getMetricNameForReport(entry.getKey(), tags), entry.getValue());
    }

    protected void report(MetricRegistrySnapshot snapshot, MetricTagsSnapshot tags, String key, Object value) {
        throw new UnsupportedOperationException("Not Implemented");
    }

    public void flush() {
    }

    public void close() {
    }

    public String getMetricNameForReport(String metricName, MetricTagsSnapshot tags) {
        Map<String, String> taggedCache = metricsNamesCache.get(tags);
        if (taggedCache == null) {
            taggedCache = new ConcurrentHashMap<>();
            metricsNamesCache.put(tags, taggedCache);
        }

        String result = taggedCache.get(metricName);
        if (result == null) {
            result = toReportMetricName(metricName, tags);
            taggedCache.put(metricName, result);
        }
        return result;
    }

    protected String toReportMetricName(String metricName, MetricTagsSnapshot tags) {
        return metricName;
    }
}
