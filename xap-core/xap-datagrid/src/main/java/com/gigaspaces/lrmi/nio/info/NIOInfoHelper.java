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

package com.gigaspaces.lrmi.nio.info;

import com.gigaspaces.lrmi.LRMIMonitoringDetails;
import com.gigaspaces.lrmi.LRMIRuntime;
import com.gigaspaces.lrmi.nio.filters.SSLFilterFactory;
import com.gigaspaces.start.SystemInfo;
import com.j_spaces.core.service.ServiceConfigLoader;
import com.j_spaces.kernel.SystemProperties;
import com.j_spaces.kernel.threadpool.GsPoolExecutorService;

/**
 * @author kimchy
 */
public abstract class NIOInfoHelper {

    private static NIODetails details;
    private static final String localHostAddress = SystemInfo.singleton().network().getHost().getHostAddress();
    private static final String localHostName = SystemInfo.singleton().network().getHost().getHostName();

    public static String getLocalHostAddress() {
        return localHostAddress;
    }

    public static String getLocalHostName() {
        return localHostName;
    }

    //TODO Switch to generic configuration details, not just NIO (This will actually throw cast class exception if used with new lrmi protocol)
    public static NIODetails getDetails() {
        if (details == null) {
            com.gigaspaces.config.lrmi.nio.NIOConfiguration nioConfiguration = (com.gigaspaces.config.lrmi.nio.NIOConfiguration) ServiceConfigLoader.getTransportConfiguration();
            details = new NIODetails(LRMIRuntime.getRuntime().getID(),
                    localHostAddress, localHostName,
                    nioConfiguration.getBindHostName(),
                    LRMIRuntime.getRuntime().getPort(nioConfiguration),
                    nioConfiguration.getMinThreads(), nioConfiguration.getMaxThreads(),
                    SSLFilterFactory.class.getName().equals(System.getProperty(SystemProperties.LRMI_NETWORK_FILTER_FACTORY)));
        }
        return details;
    }

    public static NIOStatistics getNIOStatistics() {
        LRMIRuntime lrmiRuntime = LRMIRuntime.getRuntime();
        GsPoolExecutorService threadPoolExecutor = lrmiRuntime.getThreadPool();
        return new NIOStatistics(System.currentTimeMillis(),
                threadPoolExecutor.getCompletedTaskCount(), threadPoolExecutor.getActiveCount(),
                threadPoolExecutor.getQueueSize());
    }

    public static LRMIMonitoringDetails fetchMonitoringDetails() {
        com.gigaspaces.config.lrmi.nio.NIOConfiguration nioConfiguration = (com.gigaspaces.config.lrmi.nio.NIOConfiguration) ServiceConfigLoader.getTransportConfiguration();
        return LRMIRuntime.getRuntime().getMonitoringDetails(nioConfiguration);
    }

    public static void enableMonitoring() {
        LRMIRuntime.getRuntime().setMonitorActivity(true);
    }

    public static void disableMonitoring() {
        LRMIRuntime.getRuntime().setMonitorActivity(false);
    }
}
