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

import com.gigaspaces.internal.os.ProcessCpuSampler;
import com.gigaspaces.internal.os.ProcessCpuSamplerFactory;
import com.gigaspaces.internal.oshi.OshiChecker;
import com.gigaspaces.internal.oshi.OshiGaugeUtils;
import com.gigaspaces.internal.oshi.OshiUtils;
import com.gigaspaces.internal.utils.GsEnv;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.ConnectionPool;
import com.gigaspaces.lrmi.LRMIRuntime;
import com.gigaspaces.lrmi.nio.CPeer;
import com.gigaspaces.lrmi.nio.Reader;
import com.gigaspaces.lrmi.nio.Writer;
import com.gigaspaces.metrics.factories.*;
import com.gigaspaces.start.SystemBoot;
import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.start.SystemLocations;
import com.j_spaces.kernel.threadpool.DynamicThreadPoolExecutor;
import com.sun.jini.thread.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.hardware.NetworkIF;

import java.io.Closeable;
import java.net.URL;
import java.util.*;
import java.util.concurrent.BlockingQueue;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class MetricManager implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Constants.LOGGER_METRICS_MANAGER);
    private static MetricManager instance;
    private static int refCount;

    private final Object lock = new Object();
    private final MetricTags defaultTags;
    private MetricPatternSet patternSet;
    private Map<String, MetricSampler> samplers;

    private final MetricFlagsState metricFlagsState;

    public static synchronized MetricManager acquire() {
        if (instance == null)
            instance = new MetricManager();
        refCount++;
        return instance;
    }

    public MetricFlagsState getMetricFlagsState(){
        return metricFlagsState;
    }

    private static synchronized boolean release() {
        refCount--;
        if (refCount == 0) {
            instance = null;
            return true;
        }
        return false;
    }

    public boolean isMetricReporterDefined(String metricReporterClassName) {
        for (MetricSampler metricSampler : samplers.values()) {
            Collection<MetricReporter> reporters = metricSampler.getReporters();
            for (MetricReporter metricReporter : reporters) {
                if (metricReporter.getClass().getName().equals(metricReporterClassName)) {
                    return true;
                }
            }
        }

        return false;
    }

    public MetricReporter getMetricReporter(String samplerName, String metricReporterClassName) {
        for (MetricSampler metricSampler : samplers.values()) {
            if (metricSampler.getName().equals(samplerName)) {
                Collection<MetricReporter> reporters = metricSampler.getReporters();
                for (MetricReporter metricReporter : reporters) {
                    if (metricReporter.getClass().getName().equals(metricReporterClassName)) {
                        return metricReporter;
                    }
                }
            }
        }

        return null;
    }

    public static void reloadIfStarted() {
        MetricManager currInstance = instance;
        if (currInstance != null)
            currInstance.reload();
    }

    private MetricManager() {
        logger.debug("Starting Metric Manager...");
        this.defaultTags = initDefaultTags();
        reload();
        logger.debug("Started Metric Manager.");

        final String processName = SystemBoot.getProcessRole();
        if (processName != null) {
            if (processName.equals("gsa")){
                if (OshiChecker.isAvailable()) {
                    registerOperatingSystemMetricsWithOshi(createRegistrator("os"));
                } else {
                    logger.info("Skipping operating system metrics registration - Oshi is not available");
                }
            }

            Map<String, String> processTags = new HashMap<>();
            processTags.put("process_name", processName);
            registerProcessMetrics(processTags);
        }

        metricFlagsState = new MetricFlagsState();
    }

    public List<MetricRegistrator> registerProcessMetrics(Map<String, String> tags) {
        List<MetricRegistrator> registrators = new ArrayList<>();
        registrators.add(registerProcessMetricsInternal(tags));
        registrators.add(registerJvmMetrics(tags));
        registrators.add(registerLrmiMetrics(createRegistrator("lrmi", tags)));
        return registrators;
    }

    private void reload() {
        synchronized (lock) {
            // Close all existing samplers (if any):
            if (samplers != null) {
                if (logger.isInfoEnabled())
                    logger.info("Reloading Metrics Configuration");
                for (MetricSampler sampler : samplers.values())
                    sampler.close();
            }

            // load config from xml:
            MetricManagerConfig config = MetricManagerConfig.loadFromXml(getConfigFilePath());
            config.loadDefaults();
            this.patternSet = config.getPatternSet();
            if (logger.isDebugEnabled())
                logger.debug("Patterns: {}", patternSet.toString());

            // load new samplers:
            final Map<String, MetricSampler> newSamplers = new HashMap<>();
            for (MetricSamplerConfig samplerConfig : config.getSamplersConfig().values()) {
                // create reporters for each sampler:
                List<MetricReporter> reporters = new ArrayList<>();
                for (Map.Entry<String, MetricReporterFactory> entry : config.getReportersFactories().entrySet())
                    reporters.add(createReporter(entry.getKey(), entry.getValue()));
                newSamplers.put(samplerConfig.getName(), new MetricSampler(samplerConfig, reporters));
            }
            // Copy metrics (if any) from old samplers to new samplers:
            if (samplers != null) {
                for (MetricSampler sampler : samplers.values()) {
                    for (Map.Entry<MetricTags, MetricGroup> groupEntry : sampler.getRegistry().getGroups().entrySet())
                        for (Map.Entry<String, Metric> metricEntry : groupEntry.getValue().getMetrics().entrySet())
                            register(metricEntry.getKey(), groupEntry.getKey(), metricEntry.getValue(), newSamplers);
                }
            }
            // flush changes:
            this.samplers = newSamplers;
        }
    }

    public static String getConfigFilePath() {
        String configResource = GsEnv.property("com.gigaspaces.metrics.config.resource").get();
        if (StringUtils.hasLength(configResource)) {
            URL systemResource = ClassLoader.getSystemResource(configResource);
            if (systemResource != null) {
                return systemResource.getFile();
            }
            if (logger.isDebugEnabled()) {
                logger.debug("metrics.xml was not found using configured resource path: " + configResource);
            }
        }

        String result = GsEnv.property("com.gigaspaces.metrics.config").get();
        if (!StringUtils.hasLength(result)) {
            result = SystemLocations.singleton().config("metrics").resolve("metrics.xml").toString();
        }
        return result;
    }

    private MetricTags initDefaultTags() {
        Map<String, Object> tags = new HashMap<>();
        tags.put("host", SystemInfo.singleton().network().getHost().getHostName());
        tags.put("ip", SystemInfo.singleton().network().getHost().getHostAddress());
        tags.put("pid", String.valueOf(SystemInfo.singleton().os().processId()));
        return new MetricTags(tags);
    }

    public void close() {
        if (!release())
            return;

        logger.debug("Closing Metric Manager...");
        synchronized (lock) {
            for (MetricSampler sampler : samplers.values())
                sampler.close();
        }
        logger.debug("Closed Metric Manager.");
    }

    public MetricRegistrator createRegistrator(String prefix) {
        return createRegistrator(prefix, Collections.EMPTY_MAP, Collections.EMPTY_MAP);
    }

    public MetricRegistrator createRegistrator(String prefix, Map<String, String> tags) {
        return createRegistrator(prefix, tags, Collections.EMPTY_MAP);
    }

    public MetricRegistrator createRegistrator(String prefix, Map<String, String> tags, Map<String, DynamicMetricTag> dynamicTags) {
        return new InternalMetricRegistrator(this, prefix, defaultTags.extend(tags, dynamicTags));
    }

    public String getSeparator() {
        return patternSet.getSeparator();
    }

    public String toPath(String ... names) {
        return toPath(names, names.length);
    }

    public String toPath(String[] names, int limit) {
        if (limit == 1)
            return names[0];
        StringJoiner sj = new StringJoiner(getSeparator());
        for (int i = 0; i < limit; i++) {
            sj.add(names[i]);
        }
        return sj.toString();
    }

    public boolean isEnabled(String ... patternTokens) {
        int length = patternTokens.length;
        MetricPattern metricPattern = null;
        while (length > 0 && metricPattern == null) {
            metricPattern = patternSet.getPattern(toPath(patternTokens, length));
            length--;
        }
        boolean result = metricPattern == null || !metricPattern.getValue().equals("off");
        if (logger.isDebugEnabled())
            logger.debug("isEnabled({}) = {}", toPath(patternTokens), result);
        return result;
    }


    void register(String metricName, MetricTags tags, Metric metric) {
        synchronized (lock) {
            register(metricName, tags, metric, samplers);
        }
    }

    private void register(String metricName, MetricTags tags, Metric metric, Map<String, MetricSampler> samplers) {
        String samplerName = patternSet.findBestMatch(metricName);
        logger.debug("Registering metric {} to sampler {}", metricName, samplerName);
        samplers.get(samplerName).register(metricName, tags, metric);
    }

    void unregister(String metricName, MetricTags tags) {
        synchronized (lock) {
            for (MetricSampler sampler : samplers.values())
                sampler.remove(metricName, tags);
        }
    }

    void unregisterByPrefix(String prefix, MetricTags tags) {
        synchronized (lock) {
            for (MetricSampler sampler : samplers.values())
                sampler.removeByPrefix(prefix, tags);
        }
    }

    public Map<String,Object> getSnapshotsByPrefix( Collection<String> prefixes ) {
        synchronized (lock) {
            Map<String, Object> resultsMap = new HashMap<>();
            for (String prefix : prefixes) {
                for (MetricSampler sampler : samplers.values()) {
                    resultsMap.putAll(sampler.getSnapshotsByPrefix(prefix));
                }
            }

            return resultsMap;
        }
    }

    private MetricReporter createReporter(String name, MetricReporterFactory reporterFactory) {
        if (logger.isDebugEnabled())
            logger.debug("Loading metric reporter factory " + name);

        try {
            return reporterFactory.create();
        } catch (Exception e) {
            logger.warn("Failed to create reporter " + name, e);
            return null;
        }
    }

    private void registerOperatingSystemMetricsWithOshi(MetricRegistrator registrator) {

        registrator.register(registrator.toPath("cpu", "used-percent"),  OshiGaugeUtils.getCpuPercGauge());

        registrator.register(registrator.toPath("memory", "free-bytes"), OshiGaugeUtils.getFreeMemoryInBytesGauge());
        registrator.register(registrator.toPath("memory", "actual-free-bytes"), OshiGaugeUtils.getActualFreeMemoryInBytesGauge());
        registrator.register(registrator.toPath("memory", "used-bytes"), OshiGaugeUtils.getUsedMemoryInBytesGauge());
        registrator.register(registrator.toPath("memory", "actual-used-bytes"), OshiGaugeUtils.getActualUsedMemoryInBytesGauge());
        registrator.register(registrator.toPath("memory", "used-percent"), OshiGaugeUtils.getUsedMemoryInPercentGauge());

        registrator.register(registrator.toPath("swap", "free-bytes"), OshiGaugeUtils.getFreeSwapInBytesGauge());
        registrator.register(registrator.toPath("swap", "used-bytes"), OshiGaugeUtils.getUsedSwapInBytesGauge());
        registrator.register(registrator.toPath("swap", "used-percent"), OshiGaugeUtils.getUsedSwapInPercentGauge());

        for (NetworkIF networkIF : OshiUtils.getNetworkIFs()){
            Map<String, String> newTags = Collections.singletonMap("nic", networkIF.getName());
            MetricRegistrator networkRegistrator = ((InternalMetricRegistrator) registrator).extend("network", newTags, Collections.emptyMap());
            OshiGaugeUtils.registerNetworkMetrics(networkIF, networkRegistrator);
        }
    }


    private MetricRegistrator registerProcessMetricsInternal(Map<String, String> tags) {
        MetricRegistrator registrator = createRegistrator("process", tags);
        ProcessMetricFactory factory;
        ProcessCpuSampler cpuSampler = ProcessCpuSamplerFactory.create();
        if (cpuSampler.sampleTotalCpuTime() != cpuSampler.NA) {
            factory = new DefaultProcessMetricFactory(cpuSampler);
        } else {
            factory = null;
            logger.info("Skipping process metrics registration - Oshi is not available");
        }
        if (factory !=  null) {
            registrator.register(registrator.toPath("cpu", "time-total"), factory.createProcessCpuTotalTimeGauge());
            registrator.register(registrator.toPath("cpu", "used-percent"), factory.createProcessUsedCpuInPercentGauge());
        }
        return registrator;
    }

    private MetricRegistrator registerJvmMetrics(Map<String, String> tags) {
        final MetricRegistrator registrator = createRegistrator("jvm", tags);
        final JvmRuntimeMetricFactory runtimeFactory = new JvmRuntimeMetricFactory();
        registrator.register("uptime", runtimeFactory.createUptimeGauge());

        final JvmMemoryMetricFactory memoryFactory = new JvmMemoryMetricFactory();
        registrator.register(registrator.toPath("memory", "heap", "used-bytes"), memoryFactory.createHeapUsedInBytesGauge());
        registrator.register(registrator.toPath("memory", "heap", "used-percent"), memoryFactory.createHeapUsedInPercentGauge());
        registrator.register(registrator.toPath("memory", "heap", "committed-bytes"), memoryFactory.createHeapCommittedInBytesGauge());
        registrator.register(registrator.toPath("memory", "non-heap", "used-bytes"), memoryFactory.createNonHeapUsedInBytesGauge());
        registrator.register(registrator.toPath("memory", "non-heap", "committed-bytes"), memoryFactory.createNonHeapCommittedInBytesGauge());
        registrator.register(registrator.toPath("memory", "gc", "count"), memoryFactory.createCGCountGauge());
        registrator.register(registrator.toPath("memory", "gc", "time"), memoryFactory.createGCCollectionTimeGauge());

        final JvmThreadMetricFactory threadFactory = new JvmThreadMetricFactory();
        registrator.register(registrator.toPath("threads", "count"), threadFactory.createThreadCountGauge());
        registrator.register(registrator.toPath("threads", "daemon"), threadFactory.createDaemonThreadCountGauge());
        registrator.register(registrator.toPath("threads", "peak"), threadFactory.createPeakThreadCountGauge());
        registrator.register(registrator.toPath("threads", "total-started"), threadFactory.createTotalStartedThreadCountGauge());
        return registrator;
    }

    private MetricRegistrator registerLrmiMetrics(MetricRegistrator registrator) {
        LRMIRuntime lrmiRuntime = LRMIRuntime.getRuntime();
        registrator.register("received-traffic", new LongCounter(Reader.getReceivedTrafficCounter()));
        registrator.register("generated-traffic", new LongCounter(Writer.getGeneratedTrafficCounter()));
        registrator.register("pending-writes", new LongCounter(Writer.getPendingWritesCounter()));
        registrator.register(MetricConstants.CONNECTIONS_METRIC_NAME, new LongCounter(CPeer.getConnectionsCounter()));
        registrator.register(MetricConstants.ACTIVE_CONNECTIONS_METRIC_NAME, new LongCounter(ConnectionPool.getActiveConnectionsCounter()));
        registerThreadPoolMetrics(registrator.extend("connection-pool"), lrmiRuntime.getThreadPool());
        registerThreadPoolMetrics(registrator.extend("liveness-pool"), lrmiRuntime.getLivenessPriorityThreadPool());
        registerThreadPoolMetrics(registrator.extend("monitoring-pool"), lrmiRuntime.getMonitoringPriorityThreadPool());
        registerThreadPoolMetrics(registrator.extend("custom-pool"), lrmiRuntime.getCustomThreadPool());
        return registrator;
    }

    public static void registerThreadPoolMetrics(MetricRegistrator registrator, final DynamicThreadPoolExecutor dynamicThreadPoolExecutor) {
        registrator.register("active-threads", new Gauge<Integer>() {
            @Override
            public Integer getValue() throws Exception {
                return dynamicThreadPoolExecutor.getActiveCount();
            }
        });
        final BlockingQueue<Runnable> q = dynamicThreadPoolExecutor.getQueue();
        registrator.register("queueSize", new Gauge<Integer>() {
            @Override
            public Integer getValue() throws Exception {
                return q.size();
            }
        });
    }

    public static void registerTaskManagerMetrics(MetricRegistrator registrator, final TaskManager taskManager) {
        registrator.register("threads-count", new Gauge<Integer>() {
            @Override
            public Integer getValue() throws Exception {
                return taskManager.getThreadCount();
            }
        });
        registrator.register("total-tasks", new Gauge<Integer>() {
            @Override
            public Integer getValue() throws Exception {
                return taskManager.getTotalTasks();
            }
        });
    }

    public class MetricFlagsState {
        private final boolean dataIndexHitsMetricEnabled = isEnabled("space", "data", "index-hits-total");
        private final boolean dataReadCountsMetricEnabled = isEnabled("space", "data", "read-count");
        private final boolean dataTypesMetricEnabled = isEnabled("space", "data", "data-types");
        private final boolean tieredRamReadCountDataTypesMetricEnabled = isEnabled("space", "data", "read-count-ram");

        public boolean isDataIndexHitsMetricEnabled() {
            return dataIndexHitsMetricEnabled;
        }

        public boolean isTieredRamReadCountDataTypesMetricEnabled() {
            return tieredRamReadCountDataTypesMetricEnabled;
        }

        public boolean isDataTypesMetricEnabled() {
            return dataTypesMetricEnabled;
        }

        public boolean isDataReadCountsMetricEnabled() {
            return dataReadCountsMetricEnabled;
        }
    }
}
