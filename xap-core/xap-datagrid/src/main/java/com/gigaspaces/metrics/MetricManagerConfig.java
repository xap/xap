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

import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.xml.XmlParser;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.metrics.reporters.ConsoleReporterFactory;
import com.gigaspaces.metrics.reporters.FileReporterFactory;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class MetricManagerConfig {

    private static final Logger logger = Logger.getLogger(Constants.LOGGER_METRICS_MANAGER);
    private final String separator;
    private final MetricPatternSet patterns;
    private final Map<String, MetricReporterFactory> reportersFactories;
    private final Map<String, MetricSamplerConfig> samplers;

    public MetricManagerConfig() {
        this.separator = "_"; // TODO: Configurable
        this.patterns = new MetricPatternSet(separator);
        this.reportersFactories = new HashMap<String, MetricReporterFactory>();
        this.samplers = new HashMap<String, MetricSamplerConfig>();
        this.samplers.put("off", new MetricSamplerConfig("off", 0l, null));
        this.samplers.put("default", new MetricSamplerConfig("default", MetricSamplerConfig.DEFAULT_SAMPLING_RATE, null));
    }

    public void loadXml(String path) {
        XmlParser xmlParser = XmlParser.fromPath(path);

        NodeList reporterNodes = xmlParser.getNodes("/metrics-configuration/reporters/reporter");
        for (int i = 0; i < reporterNodes.getLength(); i++)
            parseReporter((Element) reporterNodes.item(i));

        NodeList samplerNodes = xmlParser.getNodes("/metrics-configuration/samplers/sampler");
        for (int i = 0; i < samplerNodes.getLength(); i++)
            parseSampler((Element) samplerNodes.item(i));

        NodeList metricNodes = xmlParser.getNodes("/metrics-configuration/metrics/metric");
        for (int i = 0; i < metricNodes.getLength(); i++)
            parseMetric((Element) metricNodes.item(i));
    }

    public static MetricManagerConfig loadFromXml(String fileName) {
        final MetricManagerConfig config = new MetricManagerConfig();
        final File file = new File(fileName);

        if (file.exists()) {
            if (file.canRead()) {
                if (logger.isLoggable(Level.CONFIG)) {
                    logger.log(Level.CONFIG, "Loading metrics configuration from " + file.getAbsolutePath());
                }
                config.loadXml(fileName);
            } else {
                if (logger.isLoggable(Level.CONFIG)) {
                    logger.log(Level.CONFIG, "Unable to read metrics configuration from " + file.getAbsolutePath());
                }
            }
        } else {
            if (logger.isLoggable(Level.CONFIG)) {
                logger.log(Level.CONFIG, "Metrics configuration file " + file.getAbsolutePath() + " does not exist");
            }
        }
        return config;
    }

    private void parseReporter(Element element) {
        String name = element.getAttribute("name");
        String factoryClassName = element.getAttribute("factory-class");

        MetricReporterFactory factory;
        if (StringUtils.hasLength(factoryClassName)) {
            factory = fromName(factoryClassName);
        } else if (name.equals("influxdb"))
            factory = fromName("com.gigaspaces.metrics.influxdb.InfluxDBReporterFactory");
        else if (name.equals("console"))
            factory = new ConsoleReporterFactory();
        else if (name.equals("file"))
            factory = new FileReporterFactory();
        else
            throw new IllegalArgumentException("Failed to create factory '" + name + "' without a custom class");

        factory.setPathSeparator(this.separator);
        factory.load(XmlParser.parseProperties(element, "name", "value"));
        reportersFactories.put(name, factory);
    }

    private static MetricReporterFactory fromName(String factoryClassName) {
        try {
            Class factoryClass = Class.forName(factoryClassName);
            return (MetricReporterFactory) factoryClass.newInstance();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to create MetricReporterFactory", e);
        } catch (InstantiationException e) {
            throw new RuntimeException("Failed to create MetricReporterFactory", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to create MetricReporterFactory", e);
        }
    }

    private void parseSampler(Element element) {
        String levelName = element.getAttribute("name");
        Long sampleRate = StringUtils.parseDurationAsMillis(element.getAttribute("sample-rate"));
        Long reportRate = StringUtils.parseDurationAsMillis(element.getAttribute("report-rate"));
        samplers.put(levelName, new MetricSamplerConfig(levelName, sampleRate, reportRate));
    }

    private void parseMetric(Element element) {
        String prefix = element.getAttribute("prefix");
        String sampler = element.getAttribute("sampler");
        patterns.add(prefix, sampler);
    }

    public String getSeparator() {
        return separator;
    }

    public Map<String, MetricReporterFactory> getReportersFactories() {
        return reportersFactories;
    }

    public Map<String, MetricSamplerConfig> getSamplersConfig() {
        return samplers;
    }

    public MetricPatternSet getPatternSet() {
        return patterns;
    }
}
