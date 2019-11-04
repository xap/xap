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

import com.gigaspaces.internal.os.OSStatistics;
import com.gigaspaces.internal.os.OshiOSDetailsProbe;
import com.gigaspaces.internal.os.OshiOSStatisticsProbe;
import com.gigaspaces.internal.os.sigar.SigarOSDetailsProbe;
import com.gigaspaces.internal.os.sigar.SigarOSStatisticsProbe;
import com.gigaspaces.internal.oshi.OshiChecker;
import com.gigaspaces.internal.oshi.OshiGaugeUtils;
import com.gigaspaces.internal.oshi.OshiUtils;
import com.gigaspaces.internal.sigar.SigarChecker;
import com.gigaspaces.internal.sigar.SigarHolder;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.ConnectionPool;
import com.gigaspaces.lrmi.LRMIRuntime;
import com.gigaspaces.lrmi.nio.CPeer;
import com.gigaspaces.lrmi.nio.Reader;
import com.gigaspaces.lrmi.nio.Writer;
import com.gigaspaces.metrics.factories.JvmMemoryMetricFactory;
import com.gigaspaces.metrics.factories.JvmRuntimeMetricFactory;
import com.gigaspaces.metrics.factories.JvmThreadMetricFactory;
import com.gigaspaces.metrics.factories.SigarCpuMetricFactory;
import com.gigaspaces.metrics.factories.SigarMemoryMetricFactory;
import com.gigaspaces.metrics.factories.SigarNetworkMetricFactory;
import com.gigaspaces.metrics.factories.SigarProcessMetricFactory;
import com.gigaspaces.metrics.factories.SigarSwapMetricFactory;
import com.gigaspaces.start.SystemBoot;
import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.start.SystemLocations;
import com.j_spaces.kernel.threadpool.DynamicThreadPoolExecutor;
import com.sun.jini.thread.TaskManager;
import org.hyperic.sigar.Sigar;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.util.Util;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class MetricManager implements Closeable {

    private static final Logger logger = Logger.getLogger(Constants.LOGGER_METRICS_MANAGER);
    private static MetricManager instance;
    private static int refCount;

    private final Object lock = new Object();
    private final MetricTags defaultTags;
    private MetricPatternSet patternSet;
    private Map<String, MetricSampler> samplers;

    public static synchronized MetricManager acquire() {
        if (instance == null)
            instance = new MetricManager();
        refCount++;
        return instance;
    }

    private static synchronized boolean release() {
        refCount--;
        if (refCount == 0) {
            instance = null;
            return true;
        }
        return false;
    }

    public boolean isMetricReporterDefined( String metricReporterClassName ){
        boolean retValue = false;
        for( MetricSampler metricSampler : samplers.values() ){
            Collection<MetricReporter> reporters = metricSampler.getReporters();
            for( MetricReporter metricReporter : reporters ){
                if( metricReporter.getClass().getName().equals( metricReporterClassName ) ){
                    retValue = true;
                    break;
                }
            }
        }

        return retValue;
    }

    public static void reloadIfStarted() {
        MetricManager currInstance = instance;
        if (currInstance != null)
            currInstance.reload();
    }

    private MetricManager() {
        logger.log(Level.FINE, "Starting Metric Manager...");
        this.defaultTags = initDefaultTags();
        reload();
        logger.log(Level.FINE, "Started Metric Manager.");

        final String processName = SystemBoot.getProcessRole();
        if (processName != null) {
            if (processName.equals("gsa")){
                if(OshiChecker.isAvailable()){
                    registerOperatingSystemMetricsWithOshi(createRegistrator("os"));
                }else{
                    registerOperatingSystemMetricsWithSigar(createRegistrator("os"));
                }
            }

            Map<String, String> processTags = new HashMap<String, String>();
            processTags.put("process_name", processName);
            registerProcessMetrics(processTags);
        }
    }

    public List<MetricRegistrator> registerProcessMetrics(Map<String, String> tags) {
        List<MetricRegistrator> registrators = new ArrayList<MetricRegistrator>();
        registrators.add(registerProcessMetricsInternal(tags));
        registrators.add(registerJvmMetrics(tags));
        registrators.add(registerLrmiMetrics(createRegistrator("lrmi", tags)));
        return registrators;
    }

    private void reload() {
        synchronized (lock) {
            // Close all existing samplers (if any):
            if (samplers != null) {
                if (logger.isLoggable(Level.INFO))
                    logger.info("Reloading Metrics Configuration");
                for (MetricSampler sampler : samplers.values())
                    sampler.close();
            }

            // load config from xml:
            MetricManagerConfig config = MetricManagerConfig.loadFromXml(getConfigFilePath());
            config.loadDefaults();
            this.patternSet = config.getPatternSet();

            // load new samplers:
            final Map<String, MetricSampler> newSamplers = new HashMap<String, MetricSampler>();
            for (MetricSamplerConfig samplerConfig : config.getSamplersConfig().values()) {
                // create reporters for each sampler:
                List<MetricReporter> reporters = new ArrayList<MetricReporter>();
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
        String result = System.getProperty("com.gigaspaces.metrics.config");
        if (!StringUtils.hasLength(result)) {
            result = SystemLocations.singleton().config("metrics").resolve("metrics.xml").toString();
        }
        return result;
    }

    private MetricTags initDefaultTags() {
        Map<String, Object> tags = new HashMap<String, Object>();
        tags.put("host", SystemInfo.singleton().network().getHost().getHostName());
        tags.put("ip", SystemInfo.singleton().network().getHost().getHostAddress());
        tags.put("pid", String.valueOf(SystemInfo.singleton().os().processId()));
        return new MetricTags(tags);
    }

    public void close() {
        if (!release())
            return;

        logger.log(Level.FINE, "Closing Metric Manager...");
        synchronized (lock) {
            for (MetricSampler sampler : samplers.values())
                sampler.close();
        }
        logger.log(Level.FINE, "Closed Metric Manager.");
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

    void register(String metricName, MetricTags tags, Metric metric) {
        synchronized (lock) {
            register(metricName, tags, metric, samplers);
        }
    }

    private void register(String metricName, MetricTags tags, Metric metric, Map<String, MetricSampler> samplers) {
        String samplerName = patternSet.findBestMatch(metricName);
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
            Map<String, Object> resultsMap = new HashMap<String, Object>();
            for (String prefix : prefixes) {
                for (MetricSampler sampler : samplers.values()) {
                    resultsMap.putAll(sampler.getSnapshotsByPrefix(prefix));
                }
            }

            return resultsMap;
        }
    }

    private MetricReporter createReporter(String name, MetricReporterFactory reporterFactory) {
        if (logger.isLoggable(Level.CONFIG))
            logger.log(Level.CONFIG, "Loading metric reporter factory " + name);

        try {
            return reporterFactory.create();
        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed to create reporter " + name, e);
            return null;
        }
    }

    private void registerOperatingSystemMetricsWithSigar(MetricRegistrator registrator) {
        if (!SigarChecker.isAvailable()) {
            logger.info("Skipping operating system metrics registration - Sigar is not available");
            return;
        }

        final SigarCpuMetricFactory cpuFactory = new SigarCpuMetricFactory();
        registrator.register(registrator.toPath("cpu", "used-percent"), cpuFactory.createUsedCpuInPercentGauge());

        final SigarMemoryMetricFactory memoryFactory = new SigarMemoryMetricFactory();
        registrator.register(registrator.toPath("memory", "free-bytes"), memoryFactory.createFreeMemoryInBytesGauge());
        registrator.register(registrator.toPath("memory", "actual-free-bytes"), memoryFactory.createActualFreeMemoryInBytesGauge());
        registrator.register(registrator.toPath("memory", "used-bytes"), memoryFactory.createUsedMemoryInBytesGauge());
        registrator.register(registrator.toPath("memory", "actual-used-bytes"), memoryFactory.createActualUsedMemoryInBytesGauge());
        registrator.register(registrator.toPath("memory", "used-percent"), memoryFactory.createUsedMemoryInPercentGauge());

        final SigarSwapMetricFactory swapFactory = new SigarSwapMetricFactory();
        registrator.register(registrator.toPath("swap", "free-bytes"), swapFactory.createFreeSwapInBytesGauge());
        registrator.register(registrator.toPath("swap", "used-bytes"), swapFactory.createUsedSwapInBytesGauge());
        registrator.register(registrator.toPath("swap", "used-percent"), swapFactory.createUsedSwapInPercentGauge());

        final SigarNetworkMetricFactory networkFactory = new SigarNetworkMetricFactory();
        Collection<String> netInterfacesNames = getNetworkNames(networkFactory);
        for (String name : netInterfacesNames) {
            Map<String, String> newTags = new HashMap<String, String>();
            newTags.put("nic", name);
            MetricRegistrator networkRegistrator = ((InternalMetricRegistrator) registrator).extend("network", newTags, Collections.EMPTY_MAP);
            networkRegistrator.register("rx-bytes", networkFactory.createRxBytesGauge(name));
            networkRegistrator.register("tx-bytes", networkFactory.createTxBytesGauge(name));
            networkRegistrator.register("rx-packets", networkFactory.createRxPacketsGauge(name));
            networkRegistrator.register("tx-packets", networkFactory.createTxPacketsGauge(name));
            networkRegistrator.register("rx-errors", networkFactory.createRxErrorsGauge(name));
            networkRegistrator.register("tx-errors", networkFactory.createTxErrorsGauge(name));
            networkRegistrator.register("rx-dropped", networkFactory.createRxDroppedGauge(name));
            networkRegistrator.register("tx-dropped", networkFactory.createTxDroppedGauge(name));
        }
    }

    private static Collection<String> getNetworkNames(SigarNetworkMetricFactory factory) {
        try {
            return factory.getNetInterfacesNames();
        } catch (RuntimeException e) {
            logger.log(Level.WARNING, "Failed to retrieve network interfaces names", e);
            return Collections.emptyList();
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

        OSStatistics.OSNetInterfaceStats[] netInterfaceStats = OshiUtils.calcNetStats();
        for(OSStatistics.OSNetInterfaceStats interfaceStats : netInterfaceStats){
            Map<String, String> newTags = new HashMap<String, String>();
            newTags.put("nic", interfaceStats.getName());
            MetricRegistrator networkRegistrator = ((InternalMetricRegistrator) registrator).extend("network", newTags, Collections.EMPTY_MAP);
            networkRegistrator.register("rx-bytes", OshiGaugeUtils.createRxBytesGauge(interfaceStats));
            networkRegistrator.register("tx-bytes", OshiGaugeUtils.createTxBytesGauge(interfaceStats));
            networkRegistrator.register("rx-packets", OshiGaugeUtils.createRxPacketsGauge(interfaceStats));
            networkRegistrator.register("tx-packets", OshiGaugeUtils.createTxPacketsGauge(interfaceStats));
            networkRegistrator.register("rx-errors", OshiGaugeUtils.createRxErrorsGauge(interfaceStats));
            networkRegistrator.register("tx-errors", OshiGaugeUtils.createTxErrorsGauge(interfaceStats));
            networkRegistrator.register("rx-dropped", OshiGaugeUtils.createRxDroppedGauge(interfaceStats));
            networkRegistrator.register("tx-dropped", OshiGaugeUtils.createTxDroppedGauge(interfaceStats));
        }
    }


    private MetricRegistrator registerProcessMetricsInternal(Map<String, String> tags) {
        MetricRegistrator registrator = createRegistrator("process", tags);
        if(OshiChecker.isAvailable()){
            registrator.register(registrator.toPath("cpu", "time-total"), OshiGaugeUtils.createProcessCpuTotalTimeGauge());
            registrator.register(registrator.toPath("cpu", "used-percent"), OshiGaugeUtils.createProcessUsedCpuInPercentGauge());
        } else if (!SigarChecker.isAvailable()) {
            logger.info("Skipping process metrics registration - Sigar is not available");
        } else {
            final SigarProcessMetricFactory factory = new SigarProcessMetricFactory();
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
}
