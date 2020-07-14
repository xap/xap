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

package com.gigaspaces.metrics.influxdb.v_08;

import com.gigaspaces.metrics.MetricGroupSnapshot;
import com.gigaspaces.metrics.MetricRegistrySnapshot;
import com.gigaspaces.metrics.MetricReporter;
import com.gigaspaces.metrics.MetricTagsSnapshot;
import com.gigaspaces.metrics.influxdb.InfluxDBDispatcher;
import com.gigaspaces.metrics.influxdb.InfluxDBReporterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author Niv Ingberg
 * @since 10.2.1
 */
public class InfluxDBReporter extends MetricReporter {

    private static final Logger logger = LoggerFactory.getLogger(InfluxDBReporter.class.getName());
    private static final String[] COLUMNS = {"time", "value"};

    private static final String SEPARATOR = ".";
    private static final String PREFIX_OS = "os" + SEPARATOR;
    private static final String PREFIX_OS_NETWORK = PREFIX_OS + "network" + SEPARATOR;
    private static final String PREFIX_PROCESS = "process" + SEPARATOR;
    private static final String PREFIX_JVM = "jvm" + SEPARATOR;
    private static final String PREFIX_LRMI = "lrmi" + SEPARATOR;
    private static final String PREFIX_LUS = "lus" + SEPARATOR;
    private static final String PREFIX_PU = "pu" + SEPARATOR;
    private static final String PREFIX_SPACE = "space" + SEPARATOR;

    private final int maxReportLength;
    private final TimeUnit timePrecision;
    private final String xapSeparator;
    private final InfluxDBDispatcher dispatcher;
    private final JSONStringBuilder json = new JSONStringBuilder();
    private final Point singlePoint = new Point();
    private final List<Point> singlePointList = Collections.singletonList(singlePoint);
    private final Set<String> processedMetrics = new HashSet<>();
    private final MultiPointList multiPointList = new MultiPointList();

    public InfluxDBReporter(InfluxDBReporterFactory factory) {
        super(factory);
        this.maxReportLength = factory.getMaxReportLength();
        this.timePrecision = factory.getTimePrecision();
        this.xapSeparator = factory.getPathSeparator();
        this.dispatcher = createDispatcher(factory);
    }

    protected InfluxDBDispatcher createDispatcher(InfluxDBReporterFactory factory) {
        final String protocol = factory.getProtocol();
        if (protocol.equalsIgnoreCase("http"))
            return new InfluxDBHttpDispatcher(factory);
        if (protocol.equalsIgnoreCase("udp"))
            return new InfluxDBUdpDispatcher(factory);
        throw new IllegalArgumentException("Unsupported InfluxDB Reporter protocol: " + protocol);
    }

    @Override
    public void close() {
        dispatcher.close();
        super.close();
    }

    @Override
    public void report(List<MetricRegistrySnapshot> snapshots) {
        if (snapshots.size() == 1)
            process(snapshots.get(0));
        else {
            processedMetrics.clear();
            multiPointList.ensureCapacity(snapshots.size());
            process(snapshots, 0);
        }

        flush();
    }

    public int getMaxReportLength() {
        return maxReportLength;
    }

    protected long convert(long timestamp) {
        return timePrecision == null ? timestamp : timePrecision.convert(timestamp, TimeUnit.MILLISECONDS);
    }

    private void process(List<MetricRegistrySnapshot> snapshots, int startPos) {
        final MetricRegistrySnapshot baseSnapshot = snapshots.get(startPos);
        for (Map.Entry<MetricTagsSnapshot, MetricGroupSnapshot> groupEntry : baseSnapshot.getGroups().entrySet()) {
            final MetricTagsSnapshot tags = groupEntry.getKey();
            for (Map.Entry<String, Object> entry : groupEntry.getValue().getMetricsValues().entrySet()) {
                final String name = entry.getKey();
                final String fullName = getMetricNameForReport(name, tags);
                if (!processedMetrics.add(fullName))
                    continue;
                multiPointList.append(convert(baseSnapshot.getTimestamp()), entry.getValue());
                if (startPos != snapshots.size() - 1) {
                    for (int i = startPos + 1; i < snapshots.size(); i++) {
                        MetricRegistrySnapshot currSnapshot = snapshots.get(i);
                        Map<String, Object> currGroup = currSnapshot.getGroups().get(tags).getMetricsValues();
                        if (currGroup.containsKey(name))
                            multiPointList.append(convert(currSnapshot.getTimestamp()), currGroup.get(name));
                    }
                }
                appendSeries(fullName, multiPointList.getList());
                multiPointList.clear();
            }
        }

        if (startPos + 1 < snapshots.size())
            process(snapshots, startPos + 1);
    }

    private void process(MetricRegistrySnapshot snapshot) {
        for (Map.Entry<MetricTagsSnapshot, MetricGroupSnapshot> groupEntry : snapshot.getGroups().entrySet()) {
            final MetricTagsSnapshot tags = groupEntry.getKey();
            for (Map.Entry<String, Object> entry : groupEntry.getValue().getMetricsValues().entrySet()) {
                singlePoint.update(convert(snapshot.getTimestamp()), entry.getValue());
                appendSeries(getMetricNameForReport(entry.getKey(), tags), singlePointList);
            }
        }
    }

    private void appendSeries(String name, List<Point> points) {
        // Save length before append:
        final int beforeAppend = json.length();
        if (beforeAppend == 0)
            json.append('[');

        // Append:
        json.append('{');
        json.appendTupleString("name", name);
        json.appendTupleArray("columns", COLUMNS);
        json.appendTupleList("points", points);
        json.replaceLastChar('}');
        json.append(',');
        // Verify max length is not breached:
        if (json.length() > getMaxReportLength()) {
            if (beforeAppend == 0) {
                finalizeReport();
                // Report is too large:
                if (logger.isWarnEnabled())
                    logger.warn("Metrics report skipped because its length (" + json.length() + ") exceeds the maximum length ("
                            + getMaxReportLength() + ").\n" +
                            "Report: " + json.toString());
                json.reset();
            } else {
                // If breached, rollback last append and flush:
                json.setLength(beforeAppend);
                flush();
                // Now re-append on the flushed (i.e. empty) json:
                appendSeries(name, points);
            }
        }
    }

    @Override
    public void flush() {
        if (json.length() != 0) {
            finalizeReport();
            dispatcher.send(json.toString());
            json.reset();
        }
    }

    private void finalizeReport() {
        // Replace last ',' with ']'
        json.replaceLastChar(']');
    }

    @Override
    protected String toReportMetricName(String metricName, MetricTagsSnapshot tags) {
        metricName = replaceSeparatorIfNeeded(metricName);
        if (metricName.startsWith(PREFIX_OS))
            return translateOSMetric(metricName, tags);
        if (metricName.startsWith(PREFIX_PROCESS) || metricName.startsWith(PREFIX_JVM) || metricName.startsWith(PREFIX_LRMI))
            return translateProcessMetric(metricName, tags);
        if (metricName.startsWith(PREFIX_LUS))
            return translateLusMetric(metricName, tags);
        if (metricName.startsWith(PREFIX_PU))
            return translatePUMetric(metricName, tags);
        if (metricName.startsWith(PREFIX_SPACE))
            return translateSpaceMetric(metricName, tags);

        return super.toReportMetricName(metricName, tags);
    }

    private String translateOSMetric(String metricName, MetricTagsSnapshot tags) {
        String prefix = "xap" + SEPARATOR +
                getHost(tags) + SEPARATOR;
        if (metricName.startsWith(PREFIX_OS_NETWORK)) {
            prefix += PREFIX_OS_NETWORK + tags.getTags().get("nic") + SEPARATOR;
            metricName = metricName.substring(PREFIX_OS_NETWORK.length());
        }

        return prefix + metricName;
    }

    private String translateProcessMetric(String metricName, MetricTagsSnapshot tags) {
        String prefix = "xap" + SEPARATOR;
        if (tags.getTags().containsKey("pu_name")) {
            prefix += PREFIX_PU +
                    getPuName(tags) + SEPARATOR +
                    getPuInstanceId(tags) + SEPARATOR;
        } else
            prefix += getHost(tags) + SEPARATOR +
                    getPid(tags) + SEPARATOR +
                    tags.getTags().get("process_name") + SEPARATOR;

        return prefix + metricName;
    }

    private String translateLusMetric(String metricName, MetricTagsSnapshot tags) {
        return "xap" + SEPARATOR +
                getHost(tags) + SEPARATOR +
                getPid(tags) + SEPARATOR +
                metricName;
    }

    private String translatePUMetric(String metricName, MetricTagsSnapshot tags) {
        return "xap" + SEPARATOR + PREFIX_PU +
                getPuName(tags) + SEPARATOR +
                getPuInstanceId(tags) + SEPARATOR +
                metricName.substring(PREFIX_PU.length());
    }

    private String translateSpaceMetric(String metricName, MetricTagsSnapshot tags) {
        return "xap" + SEPARATOR + PREFIX_PU +
                getPuName(tags) + SEPARATOR +
                getPuInstanceId(tags) + SEPARATOR +
                PREFIX_SPACE +
                tags.getTags().get("space_name") + SEPARATOR +
                getSpaceInstanceId(tags) + SEPARATOR +
                (tags.getTags().get("space_active") == Boolean.TRUE ? "active" + SEPARATOR : "backup" + SEPARATOR) +
                metricName.substring(PREFIX_SPACE.length());
    }

    private Object getHost(MetricTagsSnapshot tags) {
        return tags.getTags().get("host");
    }

    private Object getPid(MetricTagsSnapshot tags) {
        return tags.getTags().get("pid");
    }

    private Object getPuName(MetricTagsSnapshot tags) {
        return tags.getTags().get("pu_name");
    }

    private Object getPuInstanceId(MetricTagsSnapshot tags) {
        return replaceSeparatorIfNeeded((String) tags.getTags().get("pu_instance_id"));
    }

    private Object getSpaceInstanceId(MetricTagsSnapshot tags) {
        return replaceSeparatorIfNeeded((String) tags.getTags().get("space_instance_id"));
    }

    private String replaceSeparatorIfNeeded(String s) {
        return xapSeparator.equals(SEPARATOR) ? s : s.replace(xapSeparator, SEPARATOR);
    }

    public static class Point {
        public long timestamp;
        public Object value;

        public void update(long timestamp, Object value) {
            this.timestamp = timestamp;
            this.value = value;
        }
    }

    private class MultiPointList {
        private final ArrayList<Point> list = new ArrayList<>();
        private final ObjectPool<Point> pool;

        public MultiPointList() {
            this.pool = new ObjectPool<Point>() {
                @Override
                protected Point newInstance() {
                    return new Point();
                }
            };
        }

        public void append(long timestamp, Object value) {
            Point point = pool.acquire();
            point.update(timestamp, value);
            list.add(point);
        }

        public void ensureCapacity(int capacity) {
            list.ensureCapacity(capacity);
            pool.initialize(capacity);
        }

        public void clear() {
            for (Point point : list)
                pool.release(point);
            list.clear();
        }

        public List<Point> getList() {
            return list;
        }
    }
}
