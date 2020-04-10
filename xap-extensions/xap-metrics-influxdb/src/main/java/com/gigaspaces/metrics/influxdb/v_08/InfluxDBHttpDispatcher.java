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

import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.metrics.HttpUtils;
import com.gigaspaces.metrics.influxdb.InfluxDBDispatcher;
import com.gigaspaces.metrics.influxdb.InfluxDBReporterFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 10.2.1
 */
public class InfluxDBHttpDispatcher extends InfluxDBDispatcher {

    private static final Logger logger = LoggerFactory.getLogger(InfluxDBHttpDispatcher.class.getName());
    private static final String CONTENT_TYPE = System.getProperty("com.gigaspaces.metrics.influxdb.http.content_type", "application/json");
    private static final int TIMEOUT = Integer.getInteger("com.gigaspaces.metrics.influxdb.http.timeout", 30000);
    private final URL url;

    public InfluxDBHttpDispatcher(InfluxDBReporterFactory factory) {
        this.url = toUrl(factory);
        if (logger.isDebugEnabled())
            logger.debug("InfluxDBHttpDispatcher created [url=" + url + "]");
    }

    @Override
    protected void doSend(String content) throws IOException {
        int httpCode = HttpUtils.post(url, content, CONTENT_TYPE, TIMEOUT);
        if (httpCode != 200)
            throw new IOException("Failed to post [HTTP Code=" + httpCode + ", url=" + url.toString() + "]");
    }

    private static URL toUrl(InfluxDBReporterFactory factory) {
        // "http://192.168.11.62:8086/db/db1/series?u=root&p=root");
        try {
            String suffix = "";
            if (StringUtils.hasLength(factory.getUsername()))
                suffix = append(suffix, "u", URLEncoder.encode(factory.getUsername(), "utf-8"));
            if (StringUtils.hasLength(factory.getPassword()))
                suffix = append(suffix, "p", factory.getPassword());
            if (factory.getTimePrecision() != null)
                suffix = append(suffix, "time_precision", toString(factory.getTimePrecision()));
            suffix = "/db/" + factory.getDatabase() + "/series" + suffix;
            return new URL("http", factory.getHost(), factory.getPort(), suffix);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Failed to create InfluxDB reporter", e);
        } catch (MalformedURLException e) {
            throw new RuntimeException("Failed to create InfluxDB reporter", e);
        }
    }

    private static String toString(TimeUnit timeUnit) {
        // See http://influxdb.com/docs/v0.8/api/reading_and_writing_data.html
        if (timeUnit == TimeUnit.SECONDS)
            return "s";
        if (timeUnit == TimeUnit.MILLISECONDS)
            return "ms";
        if (timeUnit == TimeUnit.MICROSECONDS)
            return "u";
        throw new IllegalArgumentException("Unsupported time precision: " + timeUnit);
    }

    private static String append(String prefix, String name, String value) {
        return prefix + (prefix.length() == 0 ? '?' : '&') + name + '=' + value;
    }
}
