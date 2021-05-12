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

package com.gigaspaces.metrics.influxdb;

import com.gigaspaces.metrics.MetricRegistrySnapshot;
import com.gigaspaces.metrics.MetricReporter;
import com.gigaspaces.metrics.MetricTagsSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.NumberFormat;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Niv Ingberg
 * @since 10.2.1
 */
public class InfluxDBReporter extends MetricReporter {

    private static final Logger logger = LoggerFactory.getLogger(InfluxDBReporter.class.getName());
    private static final String END_OF_METRIC = "\n";
    private static final NumberFormat numberFormat = createNumberFormat();

    private static NumberFormat createNumberFormat() {
        int maxFractionDigits = Integer.getInteger("com.gs.metrics.influxdb.max-fraction-digits", 5);
        NumberFormat numberFormat = NumberFormat.getInstance(Locale.US);
        numberFormat.setMaximumFractionDigits(maxFractionDigits);
        numberFormat.setGroupingUsed(false);
        return numberFormat;
    }

    private final int maxReportLength;
    private final TimeUnit timePrecision;
    private final InfluxDBDispatcher dispatcher;
    private final StringBuilder buffer = new StringBuilder();

    public InfluxDBReporter(InfluxDBReporterFactory factory) {
        super(factory);
        this.maxReportLength = factory.getMaxReportLength();
        this.timePrecision = factory.getTimePrecision();
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
    protected void report(MetricRegistrySnapshot snapshot, MetricTagsSnapshot tags, String key, Object value) {
        // Save length before append:
//        logger.info("====> report Influx");
        final int beforeAppend = buffer.length();

        // Append line (see https://influxdb.com/docs/v0.9/write_protocols/line.html)
        appendString(key);
        for (Map.Entry<String, Object> tag : tags.getTags().entrySet()) {
            buffer.append(',');
            appendString(tag.getKey());
            buffer.append('=');
            appendString(tag.getValue().toString());
        }
        buffer.append(" value=");
        appendValue(value);
        buffer.append(' ');
        buffer.append(convert(snapshot.getTimestamp()));
        buffer.append(END_OF_METRIC);
        // Verify max length is not breached:
        if (buffer.length() > maxReportLength) {
            if (beforeAppend == 0) {
                // Report is too large:
                if (logger.isWarnEnabled())
                    logger.warn("Metrics report skipped because its length (" + buffer.length() + ") exceeds the maximum length ("
                            + maxReportLength + ").\n" +
                            "Report: " + buffer.toString());
                buffer.setLength(0);
            } else {
                // If breached, rollback last append and flush:
                buffer.setLength(beforeAppend);
                flush();
                // Now re-append on the flushed (i.e. empty) buffer:
                report(snapshot, tags, key, value);
            }
        }
    }

    @Override
    public void close() {
        dispatcher.close();
        super.close();
    }

    public InfluxDBDispatcher getDispatcher() {
        return dispatcher;
    }

    protected long convert(long timestamp) {
        return timePrecision == TimeUnit.MILLISECONDS ? timestamp : timePrecision.convert(timestamp, TimeUnit.MILLISECONDS);
    }

    private void appendString(String s) {
        final char SPACE = ' ';
        final char COMMA = ',';
        final char EQUALS = '=';
        if (s.indexOf(SPACE) == -1 && s.indexOf(COMMA) == -1 && s.indexOf(EQUALS) == -1)
            buffer.append(s);
        else {
            final int length = s.length();
            buffer.ensureCapacity(buffer.length() + length);
            for (int i = 0; i < length; i++) {
                char c = s.charAt(i);
                if (c == SPACE || c == COMMA || c == EQUALS)
                    buffer.append('\\');
                buffer.append(c);
            }
        }
    }

    private void appendValue(Object value) {
        if (value instanceof Number) {
            if (value instanceof Double)
                buffer.append(numberFormat.format(((Double) value).doubleValue()));
            else if (value instanceof Float)
                buffer.append(numberFormat.format(((Float) value).floatValue()));
            else if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte)
                buffer.append(value).append('i');
            else
                throw new IllegalArgumentException("Unsupported Number class - " + value.getClass().getName());
        } else if (value instanceof String) {
            final String s = (String) value;
            buffer.append('"');
            if (s.indexOf('"') == -1)
                buffer.append(value);
            else {
                final int length = s.length();
                buffer.ensureCapacity(buffer.length() + length);
                for (int i = 0; i < length; i++) {
                    char c = s.charAt(i);
                    if (c == '"')
                        buffer.append('\\');
                    buffer.append(c);
                }
            }
            buffer.append('"');
        } else if (value instanceof Boolean)
            buffer.append(value.equals(Boolean.TRUE) ? 'T' : 'F');
        else if (value == null)
            throw new IllegalArgumentException("InfluxDB does not support null values");
        else
            throw new IllegalArgumentException("Unsupported value class: " + value.getClass().getName());
    }

    @Override
    protected void flush() {
        if (buffer.length() != 0) {
            buffer.setLength(buffer.length() - END_OF_METRIC.length());
            dispatcher.send(buffer.toString());
            buffer.setLength(0);
        }
    }
}
