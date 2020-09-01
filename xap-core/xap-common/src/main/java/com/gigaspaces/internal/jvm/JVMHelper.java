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

import com.gigaspaces.internal.jmx.OshiJVMStatisticsProbe;
import com.gigaspaces.internal.jvm.jmx.JMXJVMDetailsProbe;
import com.gigaspaces.internal.jvm.jmx.JMXJVMStatisticsProbe;
import com.gigaspaces.internal.jvm.sigar.SigarJVMStatisticsProbe;
import com.gigaspaces.internal.oshi.OshiChecker;
import com.gigaspaces.internal.sigar.SigarChecker;
import com.gigaspaces.logger.LogHelper;

import java.util.logging.Level;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class JVMHelper {
    private static final String _loggerName = "com.gigaspaces.jvm";

    private static final JVMDetailsProbe _detailsProbe;
    private static final JVMStatisticsProbe _statisticsProbe;
    private static final JVMDetails NA_DETAILS = new JVMDetails();
    private static final JVMStatistics NA_STATISTICS = new JVMStatistics();

    // we cache the details, since they never change
    private static JVMDetails details;

    static {
        _statisticsProbe = initJVMStatisticsProbe();
        _detailsProbe = initJVMDetailsProbe();
    }

    private static JVMDetailsProbe initJVMDetailsProbe() {
        String detailsProbeClass = System.getProperty("gs.admin.jvm.probe.details");
        if (detailsProbeClass != null)
            return tryCreateInstance(detailsProbeClass);

        try {
            JVMDetailsProbe result = new JMXJVMDetailsProbe();
            result.probeDetails();
            return result;
        } catch (Throwable t) {
            LogHelper.log(_loggerName, Level.FINE, "Trying to load JMXJVMDetailsProbe failed", t);
        }

        return null;
    }

    private static JVMStatisticsProbe initJVMStatisticsProbe() {

        String statisticsProbeClass = System.getProperty("gs.admin.jvm.probe.statistics");
        if (statisticsProbeClass != null)
            return tryCreateInstance(statisticsProbeClass);

        if(OshiChecker.isAvailable()){
            OshiJVMStatisticsProbe oshiJVMStatisticsProbe = new OshiJVMStatisticsProbe();
            oshiJVMStatisticsProbe.probeStatistics();
            return oshiJVMStatisticsProbe;
        } else if (SigarChecker.isAvailable()) {
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
            JVMStatisticsProbe result = new JMXJVMStatisticsProbe();
            result.probeStatistics();
            return result;
        } catch (Throwable t) {
            LogHelper.log(_loggerName, Level.FINE, "Trying to load JMX failed", t);
            // ignore, no sigar
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
        if (_detailsProbe == null)
            return NA_DETAILS;

        try {
            if (details != null)
                return details;

            details = _detailsProbe.probeDetails();
            return details;
        } catch (Exception e) {
            LogHelper.log(_loggerName, Level.FINE, "Failed to get configuration", e);
            return NA_DETAILS;
        }
    }

    public static JVMStatistics getStatistics() {
        if (_statisticsProbe == null)
            return NA_STATISTICS;

        try {
            return _statisticsProbe.probeStatistics();
        } catch (Exception e) {
            LogHelper.log(_loggerName, Level.FINE, "Failed to get stats", e);
            return NA_STATISTICS;
        }
    }

    public static void initStaticCotr() {
        // does nothing except invoking the static constructor that initializes Sigar
        // This must be called outside of the LogManager lock (meaning before RollingFileHandler() is invoked)
    }
}
