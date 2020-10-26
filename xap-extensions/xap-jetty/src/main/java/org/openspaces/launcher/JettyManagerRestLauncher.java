package org.openspaces.launcher;

import com.gigaspaces.internal.version.PlatformVersion;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.nio.filters.SelfSignedCertificate;
import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.start.SystemLocations;
import com.gigaspaces.start.manager.XapManagerConfig;
import com.j_spaces.kernel.SystemProperties;
import org.eclipse.jetty.annotations.ServletContainerInitializersStarter;
import org.eclipse.jetty.apache.jsp.JettyJasperInitializer;
import org.eclipse.jetty.plus.annotation.ContainerInitializer;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.webapp.WebAppContext;
import org.eclipse.jetty.webapp.WebInfConfiguration;
import org.openspaces.core.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import java.io.Closeable;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * @author Yohana Khoury
 * @since 12.1
 */
public class JettyManagerRestLauncher implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(Constants.LOGGER_MANAGER);

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
        System.setProperty("org.apache.jasper.compiler.disablejsr199","true");

        try {
            final XapManagerConfig config = SystemInfo.singleton().getManagerClusterInfo().getCurrServer();
            if (config == null) {
                logger.error("Cannot start server  - this host is not part of the xap managers configuration");
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
            if (logger.isInfoEnabled()) {
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
            logger.error(e.toString(), e);
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
                if (logger.isDebugEnabled()) {
                    logger.debug("File name:" + file.getName() + ", exists:" + file.exists());
                }
                try {
                    FileUtils.deleteFileOrDirectory(file);
                    logger.info("Deleted temp file :" + file.getName() );
                } catch (Throwable t) {
                    if (logger.isWarnEnabled()) {
                        logger.warn(
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

    private String toSessionId(File file) {
        String fileName = file.getName();
        if (fileName.indexOf(".") > 0) {
            fileName = fileName.substring(0, fileName.lastIndexOf("."));
        }

        boolean sslEnabled = Boolean.getBoolean(SystemProperties.MANAGER_REST_SSL_ENABLED);
        String ssl = sslEnabled ? "_Secured" : "";
        return "GigaSpaces_" + fileName.replace(" ","_") + ssl;
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
            String contextPath = getContextPath(file);
            webApp.setContextPath(contextPath);
            if (contextPath.equals("/") && Boolean.getBoolean("com.gs.security.enabled")) {
                webApp.setInitParameter("spring.profiles.active", "gs-ops-manager-secured");
            }
            webApp.getSessionHandler().setSessionCookie(toSessionId(file));

            //Enable JSP support
            webApp.setAttribute("org.eclipse.jetty.server.webapp.ContainerIncludeJarPattern",
                    ".*/[^/]*servlet-api-[^/]*\\.jar$|.*/javax.servlet.jsp.jstl-.*\\.jar$|.*/.*taglibs*\\.jar$");
            webApp.setAttribute("org.eclipse.jetty.containerInitializers", jspInitializers());
            webApp.addBean(new ServletContainerInitializersStarter(webApp), true);

            webApp.setWar(file.getAbsolutePath());
            webApp.setThrowUnavailableOnStartupException(true);
            handler.addHandler(webApp);

            String webAppTmpDir = WebInfConfiguration.getCanonicalNameForWebAppTmpDir(webApp);
            try {
                File tmpDir = File.createTempFile( webAppTmpDir, ".dir", workLocation );
                webApp.setTempDirectory( tmpDir );
            } catch (IOException e) {
                if( logger.isErrorEnabled() ) {
                    logger.error(e.toString(), e);
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

                logger.warn("Security is enabled, but SSL was explicitly disabled - passwords will be sent over the network without encryption");
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
                logger.warn("Failed to stop server: " + e);
            }
        }
        if (this.application != null)
            this.application.destroy();
    }

    private List<ContainerInitializer> jspInitializers()
    {
        JettyJasperInitializer sci = new JettyJasperInitializer();
        ContainerInitializer initializer = new ContainerInitializer(sci, null);
        List<ContainerInitializer> initializers = new ArrayList<ContainerInitializer>();
        initializers.add(initializer);
        return initializers;
    }
}
