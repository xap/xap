package org.openspaces.launcher;

import com.gigaspaces.internal.version.PlatformVersion;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.nio.filters.SelfSignedCertificate;
import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.start.SystemLocations;
import com.gigaspaces.start.manager.XapManagerConfig;
import com.j_spaces.kernel.SystemProperties;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.webapp.WebAppContext;
import org.eclipse.jetty.webapp.WebInfConfiguration;
import org.openspaces.core.util.FileUtils;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import java.io.Closeable;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Yohana Khoury
 * @since 12.1
 */
public class JettyManagerRestLauncher implements Closeable {
    private static final Logger logger = Logger.getLogger(Constants.LOGGER_MANAGER);

    private AbstractXmlApplicationContext application;
    private Server server;

    private final static File workLocation = SystemLocations.singleton().work("rest-jetty").toFile();

    public static void main(String[] args) {
        final JettyManagerRestLauncher starter = new JettyManagerRestLauncher();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                starter.close();
            }
        });
    }

    /**
     * NOTE: This ctor is also called via reflection from SystemConfig
     */
    @SuppressWarnings("WeakerAccess")
    public JettyManagerRestLauncher() {
        try {
            final XapManagerConfig config = SystemInfo.singleton().getManagerClusterInfo().getCurrServer();
            if (config == null) {
                logger.severe("Cannot start server  - this host is not part of the xap managers configuration");
                System.exit(1);
            }
            String customJettyPath = System.getProperty(SystemProperties.MANAGER_REST_JETTY_CONFIG);
            if (customJettyPath != null) {
                logger.info("Loading jetty configuration from " + customJettyPath);
                this.application = new FileSystemXmlApplicationContext(customJettyPath);
                this.server = this.application.getBean(Server.class);
            } else {
                this.server = new Server();
            }
            if (!server.isStarted()) {
                if (server.getConnectors() == null || server.getConnectors().length == 0) {
                    initConnectors(server, config);
                }
                if (server.getHandler() == null) {
                    initWebApps(server);
                }
                //fix GS-13595, 17.12.2018
                clearOldTempWarFiles();

                server.start();
            }
            if (logger.isLoggable(Level.INFO)) {
                String connectors = "";
                for (Connector connector : server.getConnectors()) {
                    if (connector instanceof ServerConnector) {
                        String connectorDesc = JettyUtils.toUrlPrefix((ServerConnector) connector);
                        connectors = connectors.isEmpty() ? connectorDesc : connectors + ", " + connectorDesc;
                    }
                }
                logger.info("Started at " + connectors);
            }
        }catch(Exception e){
            logger.log(Level.SEVERE, e.toString(), e);
            System.exit(-1);
        }
    }

    //fix GS-13595, 17.12.2018
    private void clearOldTempWarFiles() {

        String clearRestJettyFiles =
            System.getProperty(SystemProperties.CLEAR_REST_JETTY_FILES, Boolean.FALSE.toString());
        if( Boolean.parseBoolean( clearRestJettyFiles ) ) {

            File tempDirectory = workLocation;//new File( tempDirPath );

            File[] filteredFiles = tempDirectory.listFiles();

            logger.info( filteredFiles.length + " rest jetty files are deleting from [" + tempDirectory.getPath() + "]");

            for (File file : filteredFiles) {
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("File name:" + file.getName() + ", exists:" + file.exists());
                }
                try {
                    FileUtils.deleteFileOrDirectory(file);
                    logger.info("Deleted temp file :" + file.getName() );
                } catch (Throwable t) {
                    if (logger.isLoggable(Level.WARNING)) {
                        logger.log(Level.WARNING,
                                   "Failed to delete jetty temp file, " + t.toString());
                    }
                }
            }
        }
    }

    private void initConnectors(Server server, XapManagerConfig config)
            throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
        // Bind to the private host, unless local-manager is used.
        final String host = config.getHost().equals("localhost") ? config.getHost()  :SystemInfo.singleton().network().getHostId();
        final int port = Integer.parseInt(config.getAdminRest());
        SslContextFactory sslContextFactory = createSslContextFactoryIfNeeded();
        JettyUtils.createConnector(server, host, port, sslContextFactory);
    }

    private void sortDesc(File[] files) {
        Arrays.sort(files, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                return -1 * o1.getName().compareTo(o2.getName());
            }
        });
    }

    private void initWebApps(Server server) {
        ContextHandlerCollection handler = new ContextHandlerCollection();
        File webApps = SystemLocations.singleton().libPlatform("manager").resolve("webapps").toFile();
        FilenameFilter warFilesFilter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".war");
            }
        };

        File[] warFiles = webApps.listFiles(warFilesFilter);

        boolean isWorkLocationExist = workLocation.exists();
        if( !isWorkLocationExist ){
            workLocation.mkdirs();
        }

        for (File file : warFiles) {
            WebAppContext webApp = new WebAppContext();
            webApp.setContextPath(getContextPath(file));
            webApp.setWar(file.getAbsolutePath());
            webApp.setThrowUnavailableOnStartupException(true);
            handler.addHandler(webApp);

            String webAppTmpDir = WebInfConfiguration.getCanonicalNameForWebAppTmpDir(webApp);
            try {
                File tmpDir = File.createTempFile( webAppTmpDir, ".dir", workLocation );
                webApp.setTempDirectory( tmpDir );
            } catch (IOException e) {
                if( logger.isLoggable( Level.SEVERE ) ) {
                    logger.log(Level.SEVERE, e.toString(), e);
                }
            }
        }

        server.setHandler(handler);
    }

    private String getContextPath(File file) {
        return file.getName().equals("ui.war")
                ? "/"
                : "/" + file.getName().replace(".war", "");
    }

    private SslContextFactory createSslContextFactoryIfNeeded()
            throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
        boolean sslEnabled = Boolean.getBoolean(SystemProperties.MANAGER_REST_SSL_ENABLED);
        if (!sslEnabled) {
            boolean isSecured = Boolean.getBoolean(SystemProperties.SECURITY_ENABLED);
            if (isSecured) {
                if (System.getProperty(SystemProperties.MANAGER_REST_SSL_ENABLED) == null)
                    throw new SecurityException(
                            "Security is enabled, but SSL is not configured. Please configure SSL using the system property '"
                                    +SystemProperties.MANAGER_REST_SSL_ENABLED+"'. " +
                                    "For more information: '" + PlatformVersion.getProductHelpUrl() + "/admin/xap-manager-rest.html#security'");

                logger.warning("Security is enabled, but SSL was explicitly disabled - passwords will be sent over the network without encryption");
            }
            return null;
        }
        SslContextFactory sslContextFactory = new SslContextFactory();
        String keyStorePath = System.getProperty(SystemProperties.MANAGER_REST_SSL_KEYSTORE_PATH);
        String password = System.getProperty(SystemProperties.MANAGER_REST_SSL_KEYSTORE_PASSWORD);

        if (keyStorePath != null && new File(keyStorePath).exists()) {
            sslContextFactory.setKeyStorePath(keyStorePath);
            sslContextFactory.setKeyStorePassword(password);
        } else {
            sslContextFactory.setKeyStore(SelfSignedCertificate.keystore());
            sslContextFactory.setKeyStorePassword("foo");
            logger.info("SSL Keystore was not provided - Self-signed certificate was generated");
        }

        return sslContextFactory;
    }

    @Override
    public void close() {
        if (server != null) {
            try {
                server.stop();
            } catch (Exception e) {
                logger.warning("Failed to stop server: " + e);
            }
        }
        if (this.application != null)
            this.application.destroy();
    }
}
