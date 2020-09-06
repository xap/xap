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

package com.gigaspaces.internal.jvm;

import com.gigaspaces.internal.jvm.jmx.JMXJVMDetailsProbe;
import com.gigaspaces.internal.jvm.jmx.JMXJVMStatisticsProbe;
import com.gigaspaces.internal.jvm.sigar.SigarJVMStatisticsProbe;
import com.gigaspaces.internal.os.ProcessCpuSampler;
import com.gigaspaces.internal.os.ProcessCpuSamplerFactory;
import com.gigaspaces.internal.sigar.SigarChecker;
import com.gigaspaces.logger.LogHelper;

import java.util.logging.Level;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class JVMHelper {
    private static final String _loggerName = "com.gigaspaces.jvm";

    private static final JVMStatistics NA_STATISTICS = new JVMStatistics();
    private static final JVMStatisticsProbe _statisticsProbe = initJVMStatisticsProbe();

    // we cache the details, since they never change
    private static final JVMDetails details = initDetails();

    private static JVMDetails initDetails() {
        String detailsProbeClass = System.getProperty("gs.admin.jvm.probe.details");
        JVMDetailsProbe probe = detailsProbeClass != null ? tryCreateInstance(detailsProbeClass) : new JMXJVMDetailsProbe();
        if (probe != null) {
            try {
                return probe.probeDetails();
            } catch (RuntimeException e) {
                LogHelper.log(_loggerName, Level.FINE, "Failed to get JVM details from " + probe.getClass(), e);
            }
        }

        return new JVMDetails(); // N/A
    }

    private static JVMStatisticsProbe initJVMStatisticsProbe() {

        String statisticsProbeClass = System.getProperty("gs.admin.jvm.probe.statistics");
        if (statisticsProbeClass != null)
            return tryCreateInstance(statisticsProbeClass);

        ProcessCpuSampler cpuSampler = ProcessCpuSamplerFactory.create();
        if (cpuSampler.sampleTotalCpuTime() == cpuSampler.NA && SigarChecker.isAvailable()) {
            try {
                JVMStatisticsProbe result = new SigarJVMStatisticsProbe();
                result.probeStatistics();
                return result;
            } catch (Throwable t) {
                LogHelper.log(_loggerName, Level.FINE, "Trying to load sigar failed", t);
                // ignore, no sigar
            }
        }

        try {
            JVMStatisticsProbe result = new JMXJVMStatisticsProbe(cpuSampler);
            result.probeStatistics();
            return result;
        } catch (Throwable t) {
            LogHelper.log(_loggerName, Level.FINE, "Trying to load JMX failed", t);
        }

        return null;
    }

    private static <T> T tryCreateInstance(String className) {
        try {
            Class<T> clazz = (Class<T>) JVMHelper.class.getClassLoader().loadClass(className);
            return clazz.newInstance();
        } catch (Exception e) {
            return null;
        }
    }

    public static JVMDetails getDetails() {
        return details;
    }

    public static JVMStatistics getStatistics() {
        try {
            if (_statisticsProbe != null)
                return _statisticsProbe.probeStatistics();
        } catch (Exception e) {
            LogHelper.log(_loggerName, Level.FINE, "Failed to get stats", e);
        }
        return NA_STATISTICS;
    }

    public static void initStaticCotr() {
        // does nothing except invoking the static constructor that initializes Sigar
        // This must be called outside of the LogManager lock (meaning before RollingFileHandler() is invoked)
    }
}
