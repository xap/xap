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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.SessionManager;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.util.security.Password;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.webapp.WebAppContext;

import java.io.File;
import java.io.IOException;

/**
 * @author Niv Ingberg
 * @since 10.0.0
 */
public class JettyLauncher extends WebLauncher {

    private static final Log logger = LogFactory.getLog(JettyLauncher.class);

    private Server server;

    @Override
    public void launch(WebLauncherConfig config) throws Exception {
        this.server = new Server();

        SelectChannelConnector connector;

        if( config.isSslEnabled() ){
            connector = createSslConnector(server,config);
        }
        else{
            connector = new SelectChannelConnector();
        }
        connector.setPort(config.getPort());

        //GS-12102, fix for 10.1, added possibility to define host address
        if (config.getHostAddress() != null) {
            connector.setHost(config.getHostAddress());
        }
        connector.setReuseAddress(false);

        server.setConnectors(new Connector[]{connector});

        WebAppContext webAppContext = new WebAppContext();
        webAppContext.setContextPath("/");
        webAppContext.setWar(config.getWarFilePath());
        File tempDir = new File(config.getTempDirPath());
        boolean createdDirs = tempDir.mkdirs();

        if (logger.isDebugEnabled()) {
            boolean canRead = tempDir.canRead();
            boolean canWrite = tempDir.canWrite();
            boolean canExecute = tempDir.canExecute();

            logger.debug("Temp dir:" + tempDir.getName() + ", canRead=" + canRead + ", canWrite=" + canWrite +
                    ", canExecute=" + canExecute + ", exists=" + tempDir.exists() +
                    ", createdDirs=" + createdDirs + ", path=" + config.getTempDirPath());
        }

        webAppContext.setTempDirectory(tempDir);
        webAppContext.setCopyWebDir(false);
        webAppContext.setParentLoaderPriority(true);

        String sessionManager = System.getProperty("org.openspaces.launcher.jetty.session.manager");
        if (sessionManager != null) {
            //change default session manager implementation ( in order to change "JSESSIONID" )
            //GS-10830
            try {
                Class sessionManagerClass = Class.forName(sessionManager);
                SessionManager sessionManagerImpl = (SessionManager) sessionManagerClass.newInstance();
                webAppContext.getSessionHandler().setSessionManager(sessionManagerImpl);
            } catch (Throwable t) {
                System.out.println("Session Manager [" + sessionManager + "] was not set cause following exception:" + t.toString());
                t.printStackTrace();
            }
        } else {
            System.out.println("Session Manager was not provided");
        }

        server.setHandler(webAppContext);

        server.start();
        webAppContext.start();
    }

    @Override
    public void close() throws IOException {
        if (server != null) {
            try {
                server.stop();
            } catch (Exception e) {
                throw new IOException("Failed to stop jetty server", e);
            }
        }
    }

    private SelectChannelConnector createSslConnector(Server server, WebLauncherConfig config ) throws IOException {

        SslSelectChannelConnector ssl_connector = new SslSelectChannelConnector();

        SslContextFactory cf = ssl_connector.getSslContextFactory();

        cf.setKeyStorePath(config.getSslKeyStorePath());
        cf.setTrustStore(config.getSslTrustStorePath());
        cf.setKeyStorePassword(Password.obfuscate(config.getSslKeyStorePassword()));
        cf.setKeyManagerPassword(Password.obfuscate(config.getSslKeyManagerPassword()));
        cf.setTrustStorePassword(Password.obfuscate(config.getSslTrustStorePassword()));

        cf.setExcludeCipherSuites(
                new String[] {
                        "SSL_RSA_WITH_DES_CBC_SHA",
                        "SSL_DHE_RSA_WITH_DES_CBC_SHA",
                        "SSL_DHE_DSS_WITH_DES_CBC_SHA",
                        "SSL_RSA_EXPORT_WITH_RC4_40_MD5",
                        "SSL_RSA_EXPORT_WITH_DES40_CBC_SHA",
                        "SSL_DHE_RSA_EXPORT_WITH_DES40_CBC_SHA",
                        "SSL_DHE_DSS_EXPORT_WITH_DES40_CBC_SHA"
                });
        ssl_connector.setStatsOn(false);
        server.addConnector(ssl_connector);

        return ssl_connector;
    }
}
