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

package com.gigaspaces;

/**
 * @author Niv Ingberg
 * @since 12.0
 */
@com.gigaspaces.api.InternalApi
public class CommonSystemProperties {

    public static final String GS_HOME = "com.gs.home";

    public final static String SYSTEM_TIME_PROVIDER = "com.gs.time-provider";

    public static final String HOST_ADDRESS = "java.rmi.server.hostname";

    public static final String MULTICAST_ENABLED_PROPERTY = "com.gs.multicast.enabled";

    /**
     * If true the default xap_logging.properties file will not be loaded and none of the GS log
     * handlers will be set to the LogManager.
     */
    public final static String GS_LOGGING_DISABLED = "com.gs.logging.disabled";

    /**
     * System property that determines JMX supporting
     */
    public final static String JMX_ENABLED_PROP = "com.gs.jmx.enabled";

    /**
     * Enable dynamic discovery of locators
     */
    public static final String ENABLE_DYNAMIC_LOCATORS = "com.gs.jini_lus.locators.dynamic.enabled";

    public final static String JMX_REMOTE_PORT = "com.sun.management.jmxremote.port";
    public final static String CREATE_JMX_CONNECTOR_PROP = "com.gs.jmx.createJmxConnetor";

    /**
     * If true the process of loading the default xap_logging.properties file will be printed out for
     * troubleshooting purpose.
     */
    public final static String GS_LOGGING_DEBUG = "com.gs.logging.debug";

    public final static String JAVA_LOGGING_CONFIG_FILE = "java.util.logging.config.file";
    public final static String GS_LOGGING_CONFIG_FILE_PATH = "config/log/xap_logging.properties";
    public final static String GS_EXT_LOGGING_CONFIG_FILE_PATH = "config/log/xap_logging_ext.properties";

    /**
     * System property set when SystemConfig starts an RMI Registry
     */
    public static final String REGISTRY_PORT = "com.gigaspaces.system.registryPort";
    public static final String REGISTRY_RETRIES = "com.gigaspaces.system.registryRetries";
    /**
     * System property set when SystemConfig binds the JMX PlatformMBeanServer to the RMI Registry
     */
    public static final String JMX_SERVICE_URL = "com.gigaspaces.system.jmxServiceURL";
    public static final String JMX_PUBLIC_SERVICE_URL = "com.gigaspaces.system.jmxDockerServicePublicURL";

    public static final String MANAGER_REST_PORT = "com.gs.manager.rest.port";
    public static final String MANAGER_REST_SSL_ENABLED = "com.gs.manager.rest.ssl.enabled";
    public static final String MANAGER_REST_URL = "com.gs.manager.rest.url";
    public static final String ZOOKEEPER_CONFIG_FILE = "com.gs.zookeeper.config-file";

    public static final String SERVICE_GRID_KILL_TIMEOUT = "com.gs.service-grid.process-kill-timeout";

    public final static String START_EMBEDDED_LOOKUP = "com.gs.start-embedded-lus";
}
