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

package com.gigaspaces.logger;

import com.gigaspaces.CommonSystemProperties;
import com.gigaspaces.internal.io.BootIOUtils;
import com.gigaspaces.internal.utils.GsEnv;
import com.gigaspaces.start.SystemLocations;

import java.io.*;
import java.net.URL;
import java.util.Enumeration;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * This class is a singletone that is responsible for reading and loading GS Log configuration. The
 * configuration is added to existing one and doesn't overwrite it.
 *
 * Usage:
 *
 * GSLogConfigLoader loader = GSLogConfigLoader.getLoader();
 *
 * no need for additional method invocations.
 *
 * @author Alexander Beresnev
 * @version 5.1
 */
@com.gigaspaces.api.InternalApi
public class GSLogConfigLoader {
    // properties that are read from configuration file
    final private Properties _props = new Properties();

    private static final String COM_GS_LOGGING = "com.gigaspaces.logging";
    private static final boolean _loggerConfig = Boolean.getBoolean("com.gigaspaces.logging.level.config");

    @Deprecated //since 9.0.0
    public static final String GIGASPACES_PATTERN_PREFIX = "gigaspaces";
    @Deprecated //since 9.0.0
    public static final String PATTERN_SUFFIX = "_%g_%u.log";
    @Deprecated //since 9.0.0
    public static final String SPACE_FILE_PATTERN_NAME = GIGASPACES_PATTERN_PREFIX + PATTERN_SUFFIX;
    @Deprecated //since 9.0.0
    private static String _filePatternName = SPACE_FILE_PATTERN_NAME;

    private static GSLogConfigLoader _loader = null;

    private GSLogConfigLoader(Properties overridedProperties) {
        loadGsLoggingPropertiesFile();
        overridePropertiesWithSystemProperties();
        overridePropertiesWithProvidedProperties(overridedProperties);
        boolean needToLoadHandlers = removeNonSystemClassLoaderHandlers();
        applyLoadedProperties();
        if (needToLoadHandlers) {
            loadHandlers();
        }
    }

    public static synchronized boolean isInitialized() {
        return _loader != null;
    }

    public static synchronized void reset() {
        _loader = null;
    }

    /**
     * Find in classpath the GS logging config file. This file is used for the setting GS logging
     * configuration. This file is searched under /config/log/xap_logging.properties
     *
     * User can set system property: -Djava.util.logging.config.file to use his own java logging
     * configuration. GS logger will not overwrite it.
     *
     * @return GSLogConfigLoader instance
     */
    public static synchronized GSLogConfigLoader getLoader() {
        return getLoader((Properties) null);
    }

    /**
     * Find in classpath the GS logging config file. This file is used for the setting GS logging
     * configuration. This file is searched under /config/log/xap_logging.properties
     *
     * User can set system property: -Djava.util.logging.config.file to use his own java logging
     * configuration. GS logger will not overwrite it.
     *
     * @param overridedProperties If this method is called for the first time, these properties will
     *                            override configuration file properties, and add new ones.
     * @return GSLogConfigLoader instance
     */
    public static synchronized GSLogConfigLoader getLoader(Properties overridedProperties) {
        if (_loader == null) {
            if (LogHelper.ENABLED)
                _loader = new GSLogConfigLoader(overridedProperties);
        }
        return _loader;
    }

    /**
     * Find in classpath the GS logging config file. This file is used for the setting GS logging
     * configuration. This file is searched under /config/log/xap_logging.properties
     *
     * User can set system property: -Djava.util.logging.config.file to use his own java logging
     * configuration. GS logger will not overwrite it.
     *
     * @param fileHanderPattern the file handler pattern name of the file that the loggers will
     *                          redirect to. In case of UI logger it will have a different log name
     *                          than the other spaces logger.
     * @return GSLogConfigLoader instance
     */
    public static synchronized GSLogConfigLoader getLoader(String fileHanderPattern) {
        RollingFileHandlerConfigurer.setServiceProperty(fileHanderPattern, false /*override if exists*/);
        _filePatternName = fileHanderPattern;
        return getLoader();
    }

    private void loadGsLoggingPropertiesFile() {

        boolean loaded = loadGsLoggingPropertiesFileFromJavaLoggingConfiguration();

        if (!loaded) {
            loaded = loadGsLoggingPropertiesFileFromClasspath();
        }

        if (!loaded) {
            loaded = loadGsLoggingPropertiesFileFromComGsHomePath();
        }

        boolean loadedExt = loadGsExtLoggingPropertiesFileFromClasspath();

        if (!loaded && !loadedExt && _loggerConfig) {
            LogHelper.println(COM_GS_LOGGING, Level.FINE, "Not using any logging configuration file");
        }
    }

    private boolean loadGsLoggingPropertiesFileFromJavaLoggingConfiguration() {
        String javaUtilLoggingConfigFile = System.getProperty(CommonSystemProperties.JAVA_LOGGING_CONFIG_FILE);
        if (javaUtilLoggingConfigFile == null) {
            return false; //not loaded, continue trying other options
        }

        return loadGsLoggingPropertiesFileFromFullPath(javaUtilLoggingConfigFile);
    }

    // we might have got a full path to the logging config file, e.g. through
    // -Djava.util.logging.config.file
    private boolean loadGsLoggingPropertiesFileFromFullPath(String fileName) {
        InputStream in = null;
        // try loading using url
        try {
            if(BootIOUtils.isURL(fileName)) {
                in = new URL(fileName).openStream();
                _props.load(in);

                if (_loggerConfig) {
                    LogHelper.println(COM_GS_LOGGING, Level.FINE, "Loaded logging configuration file from: " + fileName);
                }
                return true; //stop trying, configuration loaded
            }
        } catch (IOException e) {
            if (_loggerConfig) {
                LogHelper.println(COM_GS_LOGGING, Level.FINE, "Failed to load logging configuration file from: " + fileName, e);
            }
            throw new RuntimeException("Failed to load logging configuration file from: " + fileName, e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    //ignore
                }
            }
        }

        //try loading using direct path
        File logConfigFile = new File(fileName);
        try {
            if (logConfigFile.exists() && logConfigFile.canRead()) {
                in = logConfigFile.toURI().toURL().openStream();
                _props.load(in);

                if (_loggerConfig) {
                    LogHelper.println(COM_GS_LOGGING, Level.FINE, "Loaded logging configuration file from: " + logConfigFile.getAbsolutePath());
                }
                return true; //stop trying, configuration loaded
            } else {
                if (_loggerConfig) {
                    LogHelper.println(COM_GS_LOGGING, Level.FINE, "Failed to load logging configuration file from: " + logConfigFile.getAbsolutePath());
                }
                return false; //not a full path, try next option
            }
        } catch (Exception e) {
            if (_loggerConfig) {
                LogHelper.println(COM_GS_LOGGING, Level.FINE, "Failed to load logging configuration file from: " + logConfigFile.getAbsolutePath(), e);
            }
            return true; //stop trying, configuration failed
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    //ignore
                }
            }
        }
    }

    /**
     * look for config/log/xap_logging.properties in classpath
     */
    private boolean loadGsLoggingPropertiesFileFromClasspath() {
        return loadLoggingFileFromClasspath(CommonSystemProperties.GS_LOGGING_CONFIG_FILE_PATH);
    }

    /**
     * look for config/log/xap_logging_ext.properties in classpath
     */
    private boolean loadGsExtLoggingPropertiesFileFromClasspath() {
        return loadLoggingFileFromClasspath(CommonSystemProperties.GS_EXT_LOGGING_CONFIG_FILE_PATH);
    }

    private boolean loadLoggingFileFromClasspath(String fileName) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = GSLogConfigLoader.class.getClassLoader();
        }

        InputStream in = null;
        try {
            in = classLoader.getResourceAsStream(fileName);
            if (in != null) {
                _props.load(in);

                if (_loggerConfig) {
                    File f = new File(classLoader.getResource(fileName).getFile());
                    LogHelper.println(COM_GS_LOGGING, Level.FINE, "Loaded logging configuration file from: " + f.getAbsolutePath());
                }
                return true; //stop trying, configuration loaded
            } else {
                return false; //not loaded, continue trying other options
            }
        } catch (IOException e) {
            if (_loggerConfig) {
                LogHelper.println(COM_GS_LOGGING, Level.FINE, "Failed to load configuration file [" + fileName + "] from classpath", e);
            }

            return true; //stop trying, failed to read configuration
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    //ignore
                }
            }
        }
    }

    //locate logging config file under GigaSpaces home/config/log/
    private boolean loadGsLoggingPropertiesFileFromComGsHomePath() {
        return loadGsLoggingPropertiesFileFromFullPath(SystemLocations.singleton().home(CommonSystemProperties.GS_LOGGING_CONFIG_FILE_PATH).toString());
    }

    /*
     * Apply any system property override
     */
    private void overridePropertiesWithSystemProperties() {
        Enumeration<Object> keys = _props.keys();
        while (keys.hasMoreElements()) {
            String key = String.valueOf(keys.nextElement());
            String newValue = System.getProperty(key);
            if (newValue != null) {
                Object prevVal = _props.put(key, newValue);
                if (_loggerConfig) {
                    LogHelper.println(COM_GS_LOGGING, Level.FINE, "override: " + key + "=" + newValue
                            + (prevVal != null ? " (was " + prevVal + ")" : ""));
                }
            }
        }
        GsEnv.getPropertiesWithPrefix("com.gs.log.").forEach(_props::setProperty);
    }

    /*
     * Override current properties with provided properties
     */
    private void overridePropertiesWithProvidedProperties(
            Properties overridedProperties) {
        if (overridedProperties == null) {
            return;
        }

        for (Entry<Object, Object> entry : overridedProperties.entrySet()) {
            _props.put(entry.getKey(), entry.getValue());
        }

    }

    /*
     * set LogManager with the properties (as InputStream)
     */
    private void applyLoadedProperties() {
        try {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            _props.store(os, "");
            ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
            LogManager.getLogManager().readConfiguration(is);

        } catch (Exception e) {
            LogHelper.println(COM_GS_LOGGING, Level.SEVERE, "Failed to configure java.util.logging", e);
        }
    }

    private boolean removeNonSystemClassLoaderHandlers() {
        /*
         * Remove RollingFileHandler from handlers since we need to instantiate it using context classloader and not system classloader.
		 */
        if (RollingFileHandler.class.getClassLoader() != ClassLoader.getSystemClassLoader()) {
            String handlers = _props.getProperty("handlers");
            if (handlers != null) {
                String onlyNonCustomHandlers = handlers.replace(RollingFileHandler.class.getName(), "");
                _props.setProperty("handlers", onlyNonCustomHandlers);
                return true;
            }
        }
        return false;
    }

    private void loadHandlers() {
        try {
            Class<? extends Handler> handlerClass = RollingFileHandler.class.asSubclass(Handler.class);
            try {
                LogManager logManager = LogManager.getLogManager();
                Logger rootLogger = logManager.getLogger(""); //get root logger
                Handler handler = handlerClass.newInstance();
                rootLogger.addHandler(handler);
            } catch (Exception e) {
                LogHelper.println(COM_GS_LOGGING, Level.WARNING, "Configuration error using handler class: " + handlerClass, e);
            }
        } catch (ClassCastException e) {
            LogHelper.println(COM_GS_LOGGING, Level.WARNING, "Configuration error", e);
        }
    }
}
