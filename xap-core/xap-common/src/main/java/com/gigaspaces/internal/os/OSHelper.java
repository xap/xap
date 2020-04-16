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

package com.gigaspaces.internal.os;

import com.gigaspaces.internal.os.jmx.JMXOSDetailsProbe;
import com.gigaspaces.internal.os.jmx.JMXOSStatisticsProbe;
import com.gigaspaces.internal.os.sigar.SigarOSDetailsProbe;
import com.gigaspaces.internal.os.sigar.SigarOSStatisticsProbe;
import com.gigaspaces.internal.oshi.OshiChecker;
import com.gigaspaces.internal.sigar.SigarChecker;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class OSHelper {

    private static final Logger _logger = LoggerFactory.getLogger("com.gigaspaces.os");

    private static final OSStatisticsProbe statisticsProbe;

    private static final OSDetailsProbe detailsProbe;

    private static final OSStatistics NA_STATISTICS = new OSStatistics();

    private static final OSDetails NA_DETAILS = new OSDetails();

    static {
        OSStatisticsProbe statisticsProbeX = null;
        String statisticsProbeClass = System.getProperty("gs.admin.os.probe.statistics");
        if (statisticsProbeClass == null) {
            if(OshiChecker.isAvailable()){

                statisticsProbeX = new OshiOSStatisticsProbe();
            } else if (SigarChecker.isAvailable()) {
                try {
                    statisticsProbeX = new SigarOSStatisticsProbe();
                    statisticsProbeX.probeStatistics();
                } catch (Throwable t) {
                    statisticsProbeX = null;
                    _logger.debug("Trying to load sigar failed", t);
                    // ignore, no sigar
                }
            }
            if (statisticsProbeX == null) {
                // try JMX
                try {
                    statisticsProbeX = new JMXOSStatisticsProbe();
                    statisticsProbeX.probeStatistics();
                } catch (Throwable t) {
                    statisticsProbeX = null;
                    _logger.debug("Trying to load JMX failed", t);
                    // ignore, no sigar
                }
            }
        } else {
            try {
                statisticsProbeX = (OSStatisticsProbe) OSHelper.class.getClassLoader().loadClass(statisticsProbeClass).newInstance();
            } catch (Exception e) {
                statisticsProbeX = null;
            }
        }
        statisticsProbe = statisticsProbeX;


        OSDetailsProbe detailsProbeX = null;
        String detailsProbeClass = System.getProperty("gs.admin.os.probe.details");
        if (detailsProbeClass == null) {
            if(OshiChecker.isAvailable()){
                detailsProbeX = new OshiOSDetailsProbe();

            } else if (SigarChecker.isAvailable()) {
                try {
                    detailsProbeX = new SigarOSDetailsProbe();
                    detailsProbeX.probeDetails();
                } catch (Throwable t) {
                    detailsProbeX = null;
                    _logger.debug("Trying to load sigar failed", t);
                    // ignore, no sigar
                }
            }
            if (detailsProbeX == null) {
                // than JMX
                try {
                    detailsProbeX = new JMXOSDetailsProbe();
                    detailsProbeX.probeDetails();
                } catch (Throwable t) {
                    detailsProbeX = null;
                    _logger.debug("Trying to load JMX failed", t);
                    // ignore, no sigar
                }
            }
        } else {
            try {
                detailsProbeX = (OSDetailsProbe) OSHelper.class.getClassLoader().loadClass(detailsProbeClass).newInstance();
            } catch (Exception e) {
                detailsProbeX = null;
            }
        }
        detailsProbe = detailsProbeX;
    }

    // we cache the os details since they never change
    private static OSDetails details;

    public static OSDetails getDetails() {
        if (detailsProbe == null) {
            return NA_DETAILS;
        }
        try {
            if (details != null) {
                return details;
            }
            details = detailsProbe.probeDetails();
            return details;
        } catch (Exception e) {
            _logger.debug("Failed to get configuration", e);
            e.printStackTrace();
            return NA_DETAILS;
        }
    }

    public static OSStatistics getStatistics() {
        if (statisticsProbe == null) {
            return NA_STATISTICS;
        }
        try {
            return statisticsProbe.probeStatistics();
        } catch (Exception e) {
            _logger.debug("Failed to get stats", e);
            return NA_STATISTICS;
        }
    }
}
