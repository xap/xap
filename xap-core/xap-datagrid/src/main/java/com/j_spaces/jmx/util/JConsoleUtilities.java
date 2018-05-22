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

package com.j_spaces.jmx.util;

import com.gigaspaces.start.SystemInfo;
import com.j_spaces.kernel.SystemProperties;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

@com.gigaspaces.api.InternalApi
public class JConsoleUtilities {
    private static final Logger _logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_COMMON);

    private static final String JCONSOLE_PLUGIN_TOPTHREADS_FILE_PATH;

    static {
        JCONSOLE_PLUGIN_TOPTHREADS_FILE_PATH = SystemInfo.singleton().locations().lib() + File.separator + "platform" + File.separator + "ui" + File.separator + "topthreads-1.0.0.jar";
    }

    /**
     * This method creates jconosle command, JDK 1.6 used or later version then plugin parameter
     * will be used with topthreads or JTop last tab
     *
     * @return command as array of String
     */
    public static String[] createJConsoleCommand(String jmxServiceUrl,
                                                 String javaHomeDir,
                                                 boolean usePlugin,
                                                 boolean useParameters) {

        List<String> commandArray = new ArrayList<String>(5);

        if (javaHomeDir == null) {
            javaHomeDir = calculateJConsoleCommandJavaHome();
        }

        if (javaHomeDir != null) {
            commandArray.add(javaHomeDir + File.separator + "bin" + File.separator + "jconsole");
        } else {
            commandArray.add("jconsole");
        }

        if (useParameters) {

            //add interval parameter
            String property = System.getProperty(SystemProperties.JCONSOLE_INTERVAL);
            if (property != null) {
                try {
                    int interval = Integer.parseInt(property);
                    commandArray.add("-interval=" + interval);
                    if (_logger.isLoggable(Level.FINE)) {
                        _logger.log(Level.FINE, "Passed by system property [" +
                                SystemProperties.JCONSOLE_INTERVAL +
                                "] jconsole interval parameter:" + interval);
                    }
                } catch (NumberFormatException nfe) {
                    if (_logger.isLoggable(Level.SEVERE)) {
                        _logger.severe("jconsole interval parameter passed by system property [" +
                                SystemProperties.JCONSOLE_INTERVAL + "] is not a number, " +
                                "jconsole will be run with its default interval parameter");
                    }
                }
            }

            //pluginpath option is relevant only for jdk 1.6 or later
            if (usePlugin) {
                //if topthreads.jar file exists so use it as jconsole plugin
                //otherwise use JTop that comes with JDK
                if ((new File(JCONSOLE_PLUGIN_TOPTHREADS_FILE_PATH)).exists()) {
                    commandArray.add("-pluginpath");
                    commandArray.add(JCONSOLE_PLUGIN_TOPTHREADS_FILE_PATH);
                    if (jmxServiceUrl != null) {
                        commandArray.add(jmxServiceUrl);
                    }
                } else {
                    if (javaHomeDir != null) {
                        commandArray.add("-pluginpath");
                        commandArray.add(javaHomeDir + File.separator + "demo" +
                                File.separator + "management" + File.separator + "JTop" +
                                File.separator + "JTop.jar");
                        if (jmxServiceUrl != null) {
                            commandArray.add(jmxServiceUrl);
                        }
                    }
                }
            } else {
                if (jmxServiceUrl != null) {
                    commandArray.add(jmxServiceUrl);
                }
            }
        }

        if (_logger.isLoggable(Level.FINE)) {
            _logger.log(Level.FINE, "all jconsole parameters:" + commandArray);
        }

        return commandArray.toArray(new String[commandArray.size()]);
    }


    /**
     * This method creates jconosle command, JDK 1.6 used or later version then plugin parameter
     * will be used with topthreads or JTop last tab
     *
     * @return command as array of String
     * @since 10.2
     */
    public static String[] createJVisualVmCommand(String jmxServiceUrl, String javaHomeDir, File visualVmFileLaunchFilePath ) {

        List<String> commandArray = new ArrayList<String>(5);

        //GS-13424 java 9 support: VisualVM is not part of JDK 9
        if( visualVmFileLaunchFilePath != null && visualVmFileLaunchFilePath.exists() ){
            commandArray.add( visualVmFileLaunchFilePath.getPath() );
        }
        else {
            if (javaHomeDir == null) {
                javaHomeDir = calculateJVisualVmCommandJavaHome();
            }

            if (javaHomeDir != null) {
                commandArray.add(javaHomeDir + File.separator + "bin" + File.separator + "jvisualvm");
            } else {
                commandArray.add("jvisualvm");
            }
        }

        commandArray.add("--openjmx");
        commandArray.add(jmxServiceUrl);
        commandArray.add("--nosplash");
//        commandArray.add( "--console" );
//        commandArray.add( "suppress" );

        if (_logger.isLoggable(Level.FINE)) {
            _logger.log(Level.FINE, "all jvisualvm parameters:" + commandArray);
        }

        return commandArray.toArray(new String[commandArray.size()]);
    }


    private static String calculateJConsoleCommandJavaHome() {
        return calculateAppJavaHome("jconsole" );
    }

    private static String calculateJVisualVmCommandJavaHome() {
        return calculateAppJavaHome("jvisualvm" );
    }

    private static String calculateAppJavaHome( String appName ) {

        String opSys = System.getProperty("os.name");
        String ext = (opSys.startsWith("Win") ? ".exe" : "");

        String javaHome = System.getProperty("java.home");
        String applPath = javaHome + File.separator + "bin" + File.separator + appName + ext;

        File applFile = new File( applPath );

        //if jvisualvm/jconsole is not defined try to to [JDK_HOME] directory
        //because probably we are currently received [JDK_HOME]/jre by calling to
        //System.getProperty("java.home")
        if (!applFile.exists()) {
            File javaHomeDir = new File(javaHome);
            javaHome = javaHomeDir.getParent();
            applPath = javaHome + File.separator + "bin" + File.separator + appName + ext;
            applFile = new File( applPath );
        }

        if (applFile.exists()) {
            return javaHome;
        }

        return null;
    }
}