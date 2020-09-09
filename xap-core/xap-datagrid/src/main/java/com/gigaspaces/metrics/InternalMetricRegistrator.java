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

import java.util.Collections;
import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class InternalMetricRegistrator extends MetricRegistrator {
    private final MetricManager metricManager;
    private final String prefix;
    private final MetricTags tags;

    public InternalMetricRegistrator(MetricManager metricManager, String prefix, MetricTags tags) {
        this.metricManager = metricManager;
        this.prefix = prefix + metricManager.getSeparator();
        this.tags = tags;
    }

    public MetricRegistrator extend(String prefix) {
        return extend(prefix, Collections.EMPTY_MAP, Collections.EMPTY_MAP);
    }

    public MetricRegistrator extend(String prefix, Map<String, String> newTags, Map<String, DynamicMetricTag> newDynamicTags) {
        return new InternalMetricRegistrator(this.metricManager, this.prefix + prefix, this.tags.extend(newTags, newDynamicTags));
    }

    @Override
    public String toPath(String... names) {
        return metricManager.toPath(names);
    }

    @Override
    public void register(String name, Metric metric) {
        metricManager.register(prefix + name, tags, metric);
    }

    @Override
    public void unregister(String name) {
        metricManager.unregister(prefix + name, tags);
    }

    @Override
    public void unregisterByPrefix(String prefix) {
        metricManager.unregisterByPrefix(this.prefix + prefix, tags);
    }

    @Override
    public void clear() {
        metricManager.unregisterByPrefix(prefix, tags);
    }
}
