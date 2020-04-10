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

/*
 * @(#)ServiceConfigLoader.java 1.0 8/15/2007 16:21PM
 */
package com.j_spaces.core.service;

import com.gigaspaces.config.lrmi.ITransportConfig;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.GenericExporter;
import com.j_spaces.kernel.ResourceLoader;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationEntryFactory;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;
import net.jini.config.PlainConfiguration;
import net.jini.export.Exporter;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ServiceConfigLoader
 *
 * Utility class for loading services.config configuration and exporter.
 *
 * @author anna
 * @since 6.0
 */
@com.gigaspaces.api.InternalApi
public class ServiceConfigLoader {
    private final static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_CONFIG);
    private final static String SERVICES_CONFIG = "config/services/services.config";

    /**
     * Thread-safe Singleton instance of a Configuration object. This singleton implementation is
     * thread-safe because static member variables created when declared are guaranteed to be
     * created the first time they are accessed. We get a thread-safe implementation that
     * automatically employs lazy instantiation; <p> We keep a singleton to avoid the penalty of
     * parsing the "services.config" file on each request. See calls to {@link #getConfiguration()}
     * and {@link #getExporter()}.
     */
    private final static ConfigurationHolder configHolder = new ConfigurationHolder();

    /**
     * An in memory configuration for space. Uses system properties to be configured.
     */
    private final static Configuration advancedSpaceConfig;

    static {
        PlainConfiguration advancedSpaceConfigX = new PlainConfiguration();
        advancedSpaceConfigX.setEntry("net.jini.lease.LeaseRenewalManager", "roundTripTime", Long.parseLong(System.getProperty("com.gs.jini.config.roundTripTime", "4000")));
        advancedSpaceConfigX.setEntry("net.jini.lookup.JoinManager", "maxLeaseDuration", Long.parseLong(System.getProperty("com.gs.jini.config.maxLeaseDuration", "8000")));
        advancedSpaceConfig = advancedSpaceConfigX;
    }

    /**
     * Singleton instantiation of a Configuration object. Holder of the service configuration, or an
     * exception if occurred during it's initialization.
     *
     * @see ServiceConfigLoader#getConfiguration()
     */
    private static class ConfigurationHolder {
        private final Configuration serviceConfiguration;
        private final ConfigurationException configurationException;

        ConfigurationHolder() {
            Configuration config = null;
            ConfigurationException exception = null;

            try {
                ConfigurationProvider.disableServicesConfig = true;
                URL servicesConfig = ResourceLoader.getResourceURL(SERVICES_CONFIG);
                InputStream is = null;
                if (servicesConfig != null) {
                    if (_logger.isWarnEnabled())
                        _logger.warn("The use of services.config file is deprecated. Please use system properties instead.");
                    try {
                        is = servicesConfig.openStream();
                    } catch (IOException e) {
                        // assume no configuration
                    }
                }
                if (is == null) {
                    // no services config, create plain one
                    PlainConfiguration plainConfiguration = new PlainConfiguration();
                    // just initialize with exporter
                    ITransportConfig transportConfig = ProtocolAdapterConigurationFactory.create();
                    //The name is kept nioConfig due to legacy code
                    plainConfiguration.setEntry("com.gigaspaces.transport", "nioConfig", transportConfig);
                    GenericExporterConfigurationEntryFactory exporter = new GenericExporterConfigurationEntryFactory();
                    // TODO for some reason, when sharing the Generic exporter between all instances, failover does not work
                    // because of connection refused exception
//                    GenericExporter exporter = new GenericExporter(nioConfig);
                    plainConfiguration.setEntry("com.gigaspaces.transport", "defaultExporter", exporter);
                    plainConfiguration.setEntry("com.sun.jini.reggie", "serverExporter", exporter);
                    plainConfiguration.setEntry("com.sun.jini.mahalo", "serverExporter", exporter);
                    plainConfiguration.setEntry("org.jini.rio", "defaultExporter", exporter);
                    plainConfiguration.setEntry("org.jini.rio.watch", "watchDataSourceExporter", exporter);
                    plainConfiguration.setEntry("net.jini.lookup.ServiceDiscoveryManager", "eventListenerExporter", exporter);
                    config = plainConfiguration;
                } else {
                    try {
                        is.close();
                    } catch (IOException e) {
                        // do nothing
                    }
                    config = ConfigurationProvider.getInstance(new String[]{servicesConfig.toExternalForm()},
                            ServiceConfigLoader.class.getClassLoader());
                }
            } catch (ConfigurationException ce) {
                exception = ce;
            } catch (UnknownHostException e) {
                exception = new ConfigurationException("Failed to find host", e);
            } finally {
                ConfigurationProvider.disableServicesConfig = false;
                serviceConfiguration = config;
                configurationException = exception;
            }
        }
    }

    public static Configuration getAdvancedSpaceConfig() {
        return advancedSpaceConfig;
    }

    /**
     * Get services.config configuration object
     *
     * @return Configuration
     */
    public static Configuration getConfiguration()
            throws ConfigurationException {
        if (configHolder.configurationException != null)
            throw configHolder.configurationException;

        return configHolder.serviceConfiguration;
    }

    /**
     * Load from services.config the defined exporter.
     *
     * @return Exporter
     */
    public static Exporter getExporter() {
        try {
            // TODO for some reason, when sharing the Generic exporter between all instances, failover does not work
            // because of connection refused exception
            return (Exporter) getConfiguration().getEntry("com.gigaspaces.transport",
                    "defaultExporter",
                    Exporter.class,
                    null);
        } catch (Exception ex) {
            if (_logger.isWarnEnabled()) {
                _logger.warn("Failed to create Exporter using " + SERVICES_CONFIG + " config file. Going to use a default GenericExporter instance.", ex);
            }
            throw new IllegalStateException("Failed to find nio configuration", ex);
        }
    }

    /**
     * Create a NIOConfiguration	according to the settings loaded from services.config
     * com.gigaspaces.transport block section.
     *
     * @return NIOConfiguration
     */
    public static ITransportConfig getTransportConfiguration() {
        try {
            return (ITransportConfig) getConfiguration().getEntry("com.gigaspaces.transport",
                    "nioConfig",
                    ITransportConfig.class,
                    null);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to find transport configuration", ex);
        }
    }

    private static class GenericExporterConfigurationEntryFactory implements ConfigurationEntryFactory {


        public Object create() {
            return new GenericExporter(getTransportConfiguration());
        }
    }
}
