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


package com.gigaspaces.metrics.factories;

import com.gigaspaces.internal.sigar.SigarHolder;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.metrics.Gauge;

import org.hyperic.sigar.NetFlags;
import org.hyperic.sigar.NetInterfaceConfig;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Barak Bar Orion
 * @since 10.1
 */

public class SigarNetworkMetricFactory {

    private static final Logger logger = LoggerFactory.getLogger(Constants.LOGGER_METRICS_MANAGER);
    private final Sigar sigar = SigarHolder.getSigar();

    public Collection<String> getNetInterfacesNames() {
        try {
            HashSet<String> result = new HashSet<String>();
            String[] names = sigar.getNetInterfaceList();
            if (logger.isDebugEnabled())
                logger.debug("The following network interface cards were detected by Sigar: " + Arrays.toString(names));
            for (String name : names) {
                NetInterfaceConfig config = sigar.getNetInterfaceConfig(name);
                if (config.getAddress().equals(NetFlags.ANY_ADDR)) {
                    if (logger.isDebugEnabled())
                        logger.debug("Skipping " + nameWithAddress(name, config) + " - invalid address");
                } else {
                    boolean added = result.add(name);
                    if (added) {
                        if (logger.isDebugEnabled())
                            logger.debug(nameWithAddress(name, config) + " will be registered for metrics");
                    } else {
                        if (logger.isWarnEnabled())
                            logger.warn("Skipping " + nameWithAddress(name, config) + " - a NIC by the same name has already been registered for metrics");
                    }
                }
            }
            return result;
        } catch (SigarException e) {
            throw new RuntimeException("Failed to get network interfaces names", e);
        }
    }

    private static String nameWithAddress(String name, NetInterfaceConfig config) {
        return "NIC [" + name + "] with address [" + config.getAddress() + "]";
    }

    public Gauge<Long> createRxBytesGauge(final String interfaceName) {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return sigar.getNetInterfaceStat(interfaceName).getRxBytes();
            }
        };
    }

    public Gauge<Long> createTxBytesGauge(final String interfaceName) {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return sigar.getNetInterfaceStat(interfaceName).getTxBytes();
            }
        };
    }

    public Gauge<Long> createRxPacketsGauge(final String interfaceName) {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return sigar.getNetInterfaceStat(interfaceName).getRxPackets();
            }
        };
    }

    public Gauge<Long> createTxPacketsGauge(final String interfaceName) {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return sigar.getNetInterfaceStat(interfaceName).getTxPackets();
            }
        };
    }

    public Gauge<Long> createRxErrorsGauge(final String interfaceName) {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return sigar.getNetInterfaceStat(interfaceName).getRxErrors();
            }
        };
    }

    public Gauge<Long> createTxErrorsGauge(final String interfaceName) {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return sigar.getNetInterfaceStat(interfaceName).getTxErrors();
            }
        };
    }

    public Gauge<Long> createRxDroppedGauge(final String interfaceName) {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return sigar.getNetInterfaceStat(interfaceName).getRxDropped();
            }
        };
    }

    public Gauge<Long> createTxDroppedGauge(final String interfaceName) {
        return new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return sigar.getNetInterfaceStat(interfaceName).getTxDropped();
            }
        };
    }
}
