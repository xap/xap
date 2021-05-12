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

import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.metrics.MetricReporter;
import com.gigaspaces.metrics.MetricReporterFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author Barak Bar Orion
 * @since 10.1
 */
public class InfluxDBReporterFactory extends MetricReporterFactory<MetricReporter> {

    // Default max length is UDP max packet length (http://en.wikipedia.org/wiki/User_Datagram_Protocol)
    public static final int DEFAULT_MAX_REPORT_LENGTH = 65507;
    public static final String DEFAULT_VERSION = "latest";
    public static final String DEFAULT_PROTOCOL = "http";
    public static final int DEFAULT_PORT_HTTP = 8086;
    public static final int DEFAULT_PORT_UDP = 4444;

    private String version = DEFAULT_VERSION;
    private String protocol = DEFAULT_PROTOCOL;
    private int maxReportLength = DEFAULT_MAX_REPORT_LENGTH;
    private TimeUnit timePrecision = TimeUnit.NANOSECONDS;
    private String host;
    private int port;
    private String database;
    private String retentionPolicy;
    private String consistency;
    private String username;
    private String password;

    @Override
    public void load(Properties properties) {
        super.load(properties);
        System.out.println("influx Properties " + properties);
        setVersion(properties.getProperty("version", DEFAULT_VERSION));
        setProtocol(properties.getProperty("protocol", DEFAULT_PROTOCOL));
        setMaxReportLength(getIntProperty(properties, "max-report-length", DEFAULT_MAX_REPORT_LENGTH));
        if (protocol.equalsIgnoreCase("http")) {
            setHost(properties.getProperty("host"));
            setPort(getIntProperty(properties, "port", DEFAULT_PORT_HTTP));
            setDatabase(properties.getProperty("database"));
            setRetentionPolicy(properties.getProperty("retention-policy"));
            setUsername(properties.getProperty("username"));
            setPassword(properties.getProperty("password"));
            setTimePrecision(StringUtils.parseTimeUnit(properties.getProperty("precision"), TimeUnit.MILLISECONDS));
            setConsistency(properties.getProperty("consistency"));
        } else if (protocol.equalsIgnoreCase("udp")) {
            setHost(properties.getProperty("host"));
            setPort(getIntProperty(properties, "port", DEFAULT_PORT_UDP));
        }
    }

    private static int getIntProperty(Properties properties, String key, int defaultValue) {
        return properties.containsKey(key) ? Integer.parseInt(properties.getProperty(key)) : defaultValue;
    }

    @Override
    public MetricReporter create() {
        if (version.equals("0.8"))
            return new com.gigaspaces.metrics.influxdb.v_08.InfluxDBReporter(this);
        if (version.equals("0.9") || version.equals("latest"))
            return new InfluxDBReporter(this);
        throw new IllegalArgumentException("Unsupported version - " + version);
    }

    public int getMaxReportLength() {
        return maxReportLength;
    }

    public void setMaxReportLength(int maxReportLength) {
        this.maxReportLength = maxReportLength;
    }

    public TimeUnit getTimePrecision() {
        return timePrecision;
    }

    public void setTimePrecision(TimeUnit timePrecision) {
        this.timePrecision = timePrecision;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getRetentionPolicy() {
        return retentionPolicy;
    }

    public void setRetentionPolicy(String retentionPolicy) {
        this.retentionPolicy = retentionPolicy;
    }

    public String getConsistency() {
        return consistency;
    }

    public void setConsistency(String consistency) {
        this.consistency = consistency;
    }
}
