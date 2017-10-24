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
import com.gigaspaces.security.service.SecurityResolver;
import com.j_spaces.kernel.ClassLoaderHelper;

import org.openspaces.pu.container.support.CommandLineParser;

import java.awt.Desktop;
import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
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

        String helpArg1 = "help";
        String helpArg2 = "h";

        CommandLineParser.Parameter[] params = CommandLineParser.parse(args, new HashSet<String>(Arrays.asList("-" +helpArg1, "-" + helpArg2)));
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
                System.setProperty(SecurityConstants.KEY_USER_PROVIDER, credentialsProvider);
            }
            else if(SecurityConstants.KEY_USER_PROPERTIES.equals( paramName ) ){
                String credentialsProviderProperties = param.getArguments()[0];
                System.setProperty(SecurityConstants.KEY_USER_PROPERTIES, credentialsProviderProperties);
            }
            else if(SecurityConstants.KEY_SSL_KEY_MANAGER_PASSWORD.equals(paramName) ){
                config.setSslKeyManagerPassword(param.getArguments()[0]);
                config.setSslEnabled(true);
            }
            else if(SecurityConstants.KEY_SSL_KEY_STORE_PASSWORD.equals(paramName) ){
                config.setSslKeyStorePassword(param.getArguments()[0]);
                config.setSslEnabled(true);
            }
            else if(SecurityConstants.KEY_SSL_KEY_STORE_PATH.equals(paramName) ){
                config.setSslKeyStorePath(param.getArguments()[0]);
                config.setSslEnabled(true);
            }
            else if(SecurityConstants.KEY_SSL_TRUST_STORE_PATH.equals(paramName) ){
                config.setSslTrustStorePath(param.getArguments()[0]);
                config.setSslEnabled(true);
            }
            else if(SecurityConstants.KEY_SSL_TRUST_STORE_PASSWORD.equals(paramName) ){
                config.setSslTrustStorePassword(param.getArguments()[0]);
                config.setSslEnabled(true);
            }
            else if (helpArg1.equals(paramName) || helpArg2.equals(paramName)) {
                printHelpMessage();
                return;
            }
        }

        GSLogConfigLoader.getLoader(name);
        GSLogConfigLoader.getLoader();
        if (!validateWar(config) || !validateSslParameters(config)) {
            printHelpMessage();
            return;
        }

        final Logger logger = Logger.getLogger(loggerName);
        RuntimeInfo.logRuntimeInfo(logger, "Starting the " + name + " server, security enabled:" + SecurityResolver
            .isSecurityEnabled() + ", bind address: " + config.getHostAddress() + ", port: " + config.getPort());
        WebLauncher webLauncher = ClassLoaderHelper.newInstance(webLauncherClass);
        webLauncher.launch(config);
        launchBrowser(logger, config);
    }

    private static void launchBrowser(Logger logger, WebLauncherConfig config) {
        String protocol = config.isSslEnabled() ? "https" : "http";
        final String url = protocol + "://localhost:" + config.getPort();
        logger.info("Browsing to " + url);
        try {
            Desktop.getDesktop().browse(URI.create(url));
        } catch (Exception e) {
            logger.warning("Failed to browse to XAP web-ui: " + e.getMessage());
        }
    }

    private static void printHelpMessage() {
        System.out.println("Launcher -path <path> [-work <work>] [-port <port>] [-name <name>] [-logger <logger>] " +
                "[-" + SecurityConstants.KEY_USER_PROVIDER + " <provider>] " +
                "[-" + SecurityConstants.KEY_USER_PROPERTIES + " <properties>] " +
                "[-" + SecurityConstants.KEY_SSL_KEY_MANAGER_PASSWORD + " <key-manager-password>] " +
                "[-" + SecurityConstants.KEY_SSL_KEY_STORE_PASSWORD + " <key-store-password>] " +
                "[-" + SecurityConstants.KEY_SSL_KEY_STORE_PATH + " <key-store-path>] " +
                "[-" + SecurityConstants.KEY_SSL_TRUST_STORE_PATH + " <trust-store-path>] " +
                "[-" + SecurityConstants.KEY_SSL_TRUST_STORE_PASSWORD + " <trust-store-password>]");
    }

    private static boolean validateWar(WebLauncherConfig config) {
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

    private static boolean validateSslParameters(WebLauncherConfig config) {

        if( config.isSslEnabled() ) {
            String sslKeyManagerPassword = config.getSslKeyManagerPassword();
            String sslKeyStorePassword = config.getSslKeyStorePassword();
            String sslKeyStorePath = config.getSslKeyStorePath();
            String sslTrustStorePath = config.getSslTrustStorePath();
            String sslTrustStorePassword = config.getSslTrustStorePassword();

            StringBuilder stringBuilder = new StringBuilder();
            if (sslKeyManagerPassword == null || sslKeyManagerPassword.trim().isEmpty()) {
                stringBuilder.append(SecurityConstants.KEY_SSL_KEY_MANAGER_PASSWORD);
                stringBuilder.append('\n');
            }
            if (sslKeyStorePassword == null || sslKeyStorePassword.trim().isEmpty()) {
                stringBuilder.append(SecurityConstants.KEY_SSL_KEY_STORE_PASSWORD);
                stringBuilder.append('\n');
            }
            if (sslKeyStorePath == null || sslKeyStorePath.trim().isEmpty()) {
                stringBuilder.append(SecurityConstants.KEY_SSL_KEY_STORE_PATH);
                stringBuilder.append('\n');
            }
            if (sslTrustStorePath == null || sslTrustStorePath.trim().isEmpty()) {
                stringBuilder.append(SecurityConstants.KEY_SSL_TRUST_STORE_PATH);
                stringBuilder.append('\n');
            }
            if (sslTrustStorePassword == null || sslTrustStorePassword.trim().isEmpty()) {
                stringBuilder.append(SecurityConstants.KEY_SSL_TRUST_STORE_PASSWORD);
                stringBuilder.append('\n');
            }

            if (stringBuilder.length() > 0) {
                stringBuilder.insert(0, "Following ssl parameters or their values are missing:\n");
                System.out.println(stringBuilder.toString());
            }

            return stringBuilder.length() == 0;
        }

        return true;
    }
}