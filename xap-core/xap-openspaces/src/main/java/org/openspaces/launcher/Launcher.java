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
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Guy Korland
 * @since 8.0.4
 */
public class Launcher implements Closeable {

    private final WebLauncherConfig config;
    private final WebLauncher webLauncher;
    private final Logger logger;

    public static void main(String[] args) throws Exception {
        Collection<String> helpArgs = Arrays.asList("help", "h");
        Properties props = parseCommandLineArgs(args, helpArgs);
        for (String helpArg : helpArgs) {
            if (props.containsKey(helpArg)) {
                printHelpMessage();
                System.exit(0);
            }
        }

        WebLauncherConfig config = new WebLauncherConfig(props);
        new Launcher(config);
    }

    public Launcher() {
        this(new WebLauncherConfig(new Properties()));
    }

    public Launcher(WebLauncherConfig config) {
        this.config = config;
        if (!validateWar(config) || !validateSslParameters(config)) {
            printHelpMessage();
            System.exit(1);
        }

        initIfNotDefined("org.openspaces.launcher.jetty.session.manager",null, "org.openspaces.pu.container.jee.jetty.GSSessionManager");
        initIfNotDefined("com.gigaspaces.logger.RollingFileHandler.time-rolling-policy", null, "monthly");
        initIfNotDefined("com.gigaspaces.webui.username.mandatory", "USER_NAME_MANDATORY", "false");

        if (!GSLogConfigLoader.isInitialized()) {
            GSLogConfigLoader.getLoader(config.getName());
            GSLogConfigLoader.getLoader();
        }

        this.logger = Logger.getLogger(config.getLoggerName());
        RuntimeInfo.logRuntimeInfo(logger, "Starting " + config.getName() + ", security enabled:" +
                SecurityResolver.isSecurityEnabled() + ", host: " + config.getHostAddress() + ", port: " + config.getPort());
        this.webLauncher = initWebLauncher();
        if (webLauncher == null)
            System.exit(1);
        launchBrowser();
    }

    @Override
    public void close() throws IOException {
        logger.info("Closing " + config.getName());
        if (webLauncher != null)
            webLauncher.close();
    }

    private WebLauncher initWebLauncher() {
        String webLauncherClass = System.getProperty("org.openspaces.launcher.class", "org.openspaces.launcher.JettyLauncher");
        try {
            WebLauncher webLauncher = ClassLoaderHelper.newInstance(webLauncherClass);
            webLauncher.launch(config);
            return webLauncher;
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to start " + webLauncherClass, e);
            return null;
        }
    }

    private void launchBrowser() {
        String protocol = config.isSslEnabled() ? "https" : "http";
        final String url = protocol + "://localhost:" + config.getPort();
        logger.info("Browsing to " + url);
        try {
            Desktop.getDesktop().browse(URI.create(url));
        } catch (Exception e) {
            logger.warning("Failed to browse to XAP web-ui: " + e.getMessage());
        }
    }

    private static Properties parseCommandLineArgs(String[] args, Collection<String> helpArgs) {
        Set<String> parametersWithoutValues = new HashSet<String>();
        for (String helpArg : helpArgs)
            parametersWithoutValues.add("-" + helpArg);

        CommandLineParser.Parameter[] params = CommandLineParser.parse(args, parametersWithoutValues);

        Properties result = new Properties();
        for (CommandLineParser.Parameter param : params)
            result.setProperty(param.getName(), param.getArguments()[0]);
        return result;
    }

    private static void initIfNotDefined(String sysProp, String envVar, String defaultValue) {
        if (!System.getProperties().containsKey(sysProp)) {
            String value = envVar != null && System.getenv().containsKey(envVar) ? System.getenv(envVar) : defaultValue;
            System.setProperty(sysProp, value);
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