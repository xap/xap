package org.openspaces.launcher;

import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.util.ssl.SslContextFactory;

public class JettyUtils {

    public static ServerConnector createConnector(Server server, String host, int port, SslContextFactory sslContextFactory) {
        ServerConnector connector = sslContextFactory == null
                ? new ServerConnector(server)
                : new ServerConnector(server, toConnectionFactories(sslContextFactory, port));
        if (host != null) {
            connector.setHost(host);
        }
        connector.setPort(port);
        server.addConnector(connector);
        return connector;
    }

    private static ConnectionFactory[] toConnectionFactories(SslContextFactory sslContextFactory, int port) {
        sslContextFactory.setExcludeCipherSuites(
                new String[]{
                        "SSL_RSA_WITH_DES_CBC_SHA",
                        "SSL_DHE_RSA_WITH_DES_CBC_SHA",
                        "SSL_DHE_DSS_WITH_DES_CBC_SHA",
                        "SSL_RSA_EXPORT_WITH_RC4_40_MD5",
                        "SSL_RSA_EXPORT_WITH_DES40_CBC_SHA",
                        "SSL_DHE_RSA_EXPORT_WITH_DES40_CBC_SHA",
                        "SSL_DHE_DSS_EXPORT_WITH_DES40_CBC_SHA"
                });

        // HTTP Configuration
        HttpConfiguration http_config = new HttpConfiguration();
        http_config.setSecureScheme("https");
        http_config.setSecurePort(port);
        http_config.setOutputBufferSize(32768);
//        http_config.setRequestHeaderSize(8192);
//        http_config.setResponseHeaderSize(8192);
        http_config.setSendServerVersion(true);
        http_config.setSendDateHeader(false);
        // SSL HTTP Configuration
        HttpConfiguration https_config = new HttpConfiguration(http_config);
        https_config.addCustomizer(new SecureRequestCustomizer());
        return new ConnectionFactory[] {
                new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()),
                new HttpConnectionFactory(https_config)
        };
    }

    public static boolean isSsl(ServerConnector connector) {
        return connector.getConnectionFactory(SslConnectionFactory.class) != null;
    }

    public static String toUrlPrefix(ServerConnector connector) {
        String protocol = JettyUtils.isSsl(connector) ? "https" : "http";
        return protocol + "://" + connector.getHost() + ":" + connector.getLocalPort();
    }
}
