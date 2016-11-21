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

package org.openspaces.launcher;

import com.gigaspaces.admin.cli.RuntimeInfo;
import com.gigaspaces.admin.security.SecurityConstants;
import com.gigaspaces.internal.io.FileUtils;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.logger.GSLogConfigLoader;
import com.j_spaces.kernel.ClassLoaderHelper;

import org.openspaces.pu.container.support.CommandLineParser;

import java.awt.*;
import java.io.File;
import java.net.URI;
import java.util.logging.Logger;

/**
 * @author Guy Korland
 * @since 8.0.4
 */
public class Launcher {

    public static void main(String[] args) throws Exception {

        WebLauncherConfig config = new WebLauncherConfig();
        String name = System.getProperty("org.openspaces.launcher.name", "launcher");
        String loggerName = System.getProperty("org.openspaces.launcher.logger", "org.openspaces.launcher");
        String webLauncherClass = System.getProperty("org.openspaces.launcher.class", "org.openspaces.launcher.JettyLauncher");
        String bindAddress = null;

        CommandLineParser.Parameter[] params = CommandLineParser.parse(args);
        for (CommandLineParser.Parameter param : params) {
            String paramName = param.getName();
            if ("port".equals(paramName))
                config.setPort(Integer.parseInt(param.getArguments()[0]));
            else if ("path".equals(paramName))
                config.setWarFilePath(param.getArguments()[0]);
            else if ("work".equals(paramName))
                config.setTempDirPath(param.getArguments()[0]);
            else if ("name".equals(paramName))
                name = param.getArguments()[0];
            else if ("logger".equals(paramName))
                loggerName = param.getArguments()[0];
            else if ("bind-address".equals(paramName)) {
                bindAddress = param.getArguments()[0];
                config.setHostAddress(bindAddress);
            }
            else if(SecurityConstants.KEY_USER_PROVIDER.equals( paramName ) ){
                String credentialsProvider = param.getArguments()[0];
                System.setProperty( SecurityConstants.KEY_USER_PROVIDER, credentialsProvider );
            }
            else if(SecurityConstants.KEY_USER_PROPERTIES.equals( paramName ) ){
                String credentialsProviderProperties = param.getArguments()[0];
                System.setProperty( SecurityConstants.KEY_USER_PROPERTIES, credentialsProviderProperties );
            }

            else if ("help".equals(paramName) || "h".equals(paramName)) {
                printHelpMessage();
                return;
            }
        }

        GSLogConfigLoader.getLoader(name);
        GSLogConfigLoader.getLoader();
        if (!validate(config)) {
            printHelpMessage();
            return;
        }

        final Logger logger = Logger.getLogger(loggerName);
        logger.info(RuntimeInfo.getEnvironmentInfoIfFirstTime());
        WebLauncher webLauncher = ClassLoaderHelper.newInstance(webLauncherClass);
        webLauncher.launch(config);
        logger.info("Starting the " + name + " server, bind address: " + config.getHostAddress() + ", port: " + config.getPort());
        launchBrowser(logger, config);
    }

    private static void launchBrowser(Logger logger, WebLauncherConfig config) {
        final String url = "http://localhost:" + config.getPort();
        logger.info("Browsing to " + url);
        try {
            Desktop.getDesktop().browse(URI.create(url));
        } catch (Exception e) {
            logger.warning("Failed to browse to XAP web-ui: " + e.getMessage());
        }
    }

    private static void printHelpMessage() {
        System.out.println("Launcher -path <path> [-work <work>] [-port <port>] [-name <name>] [-logger <logger>]");
    }

    private static boolean validate(WebLauncherConfig config) {
        // Verify path is not empty:
        if (!StringUtils.hasLength(config.getWarFilePath()))
            return false;

        // Verify path exists:
        final File file = new File(config.getWarFilePath());
        if (!file.exists()) {
            System.out.println("Path does not exist: " + config.getWarFilePath());
            return false;
        }
        // If File is an actual file, return it:
        if (file.isFile())
            return true;

        // If file is a directory, Get the 1st war file (if any):
        if (file.isDirectory()) {
            File[] warFiles = FileUtils.findFiles(file, null, ".war");
            if (warFiles.length == 0) {
                System.out.println("Path does not contain any war files: " + config.getWarFilePath());
                return false;
            }
            if (warFiles.length > 1)
                System.out.println("Found " + warFiles.length + " war files in " + config.getWarFilePath() + ", using " + warFiles[0].getPath());
            config.setWarFilePath(warFiles[0].getPath());
            return true;
        }

        System.out.println("Path is neither file nor folder: " + config.getWarFilePath());
        return false;
    }
}