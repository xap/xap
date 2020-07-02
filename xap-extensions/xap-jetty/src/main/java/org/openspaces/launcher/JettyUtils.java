package org.openspaces.launcher;

import com.gigaspaces.internal.utils.GsEnv;
import com.gigaspaces.start.SystemLocations;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class JettyUtils {

    public static ServerConnector createConnector(Server server, String host, int port, SslContextFactory sslContextFactory) {

        ServerConnector connector = sslContextFactory == null
                ? new ServerConnector(server, toConnectionFactories(port))
                : new ServerConnector(server, toHTTPSConnectionFactories(sslContextFactory, port));
        if (host != null) {
            connector.setHost(host);
        }
        connector.setPort(port);
        server.addConnector(connector);

        server.setRequestLog(new MyReqLog());


        return connector;
    }

    private static class MyReqLog implements RequestLog {
        private PrintWriter out;
        private Integer maxTillNow = 0;
        private final Object lock = new Object();

        public MyReqLog() {
            try {
                out = new PrintWriter(SystemLocations.singleton().logs().resolve("jetty-requests.log").toFile().getAbsolutePath());
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }


        private void print(String str) {
            out.print(str);
//            System.out.println(str);
        }

        private void flush(HashMap<String, Object> json) {
            print("{ ");
            Iterator<String> iter = json.keySet().iterator();

            while (iter.hasNext()) {
                String key = iter.next();
                print(String.format("\"%s\": ", key));
                flushJson(json.get(key));
                if (iter.hasNext()) {
                    print(",");
                }
            }
            print("}");
        }

        private void flush(ArrayList<Object> list) {
            print("[");

            Iterator<Object> iter = list.iterator();

            while (iter.hasNext()) {
                flushJson(iter.next());
                if (iter.hasNext()) {
                    print(",");
                }
            }
            print("]");

        }

        private synchronized void flushJson(Object o) {
            if (o instanceof HashMap) {
                flush((HashMap<String, Object>) o);
            } else if (o instanceof ArrayList) {
                flush((ArrayList<Object>) o);
            } else {
                print("\"" + o.toString() + "\"");
            }
            out.flush();

        }

        private boolean isKnownHeader(String name) {
            for (HttpHeader header : HttpHeader.values()) {
                if (header.asString().equals(name)) return true;
            }
            return false;
        }
        private boolean shouldCalculateHeader(String name) {
            switch (name) {
                case "Connection":
//                case "User-Agent":
//                case "Host":
//                case "Accept":
                case "Accept-Encoding":
                case "Cache-Control":
                    return false;
                default:
                    return true;
            }
        }

        @Override
        public void log(Request request, Response response) {
            JsonObject rootJson = create();

            List<String> headerNames = new ArrayList<>();
            Enumeration<String> headers = request.getHeaderNames();
            while (headers.hasMoreElements()) {
                headerNames.add(headers.nextElement());
            }
            AtomicInteger totalBytes = new AtomicInteger();
            AtomicInteger totalBytesWithoutCached = new AtomicInteger();

            rootJson.put("url", request.getRequestURL().toString());
            rootJson.put("totalHeaders", headerNames.size());
            ArrayList<Object> allHeadersJson = new ArrayList<>();
            rootJson.put("headers", allHeadersJson);

            List<String> cachedHeaders = Arrays.asList(HttpHeader.ACCEPT_LANGUAGE.asString(), HttpHeader.ACCEPT.asString(), HttpHeader.HOST.asString(), HttpHeader.USER_AGENT.asString(), HttpHeader.COOKIE.asString());
            headerNames.forEach((name) -> {
                        Enumeration<String> values = request.getHeaders(name);
                        List<String> vvv = new ArrayList<>();
                        int sum = 0;

                        if (shouldCalculateHeader(name)) {
                            sum += 2;
                            if (isKnownHeader(name)) {
                                sum+=1;
                            } else {
                                sum+=name.length() + 2;
                            }

                            boolean headerIsCached = cachedHeaders.contains(name);

                            while (values.hasMoreElements()) {
                                String v = values.nextElement();
                                if (name.equals("Accept")) {
                                    int index = v.lastIndexOf(",");
                                    if (index != -1) {
                                        v = v.substring(index);
                                    }
                                }
                                sum += v.length();
                                vvv.add(v);

                                if (headerIsCached) {
                                    totalBytesWithoutCached.addAndGet(-1 * v.length());
                                }

                            }
                            if (headerIsCached) {
                                totalBytesWithoutCached.addAndGet(-1 );
                            }
                        } else {
                            sum+=2;
                        }

                        JsonObject header = create();
                        header.put("header", name);
                        header.put("values", vvv);
                        header.put("size", sum);
                        allHeadersJson.add(header);
                        totalBytes.addAndGet(sum);
                        totalBytesWithoutCached.addAndGet(sum);
                    }
            );


            totalBytes.addAndGet(5 + 2);
            rootJson.put("totalBytes", totalBytes.get());
            rootJson.put("totalBytesIfCached", totalBytesWithoutCached.get());

//            JsonObject reqChan = create();
//            reqChan.put("timestamp", new Date(request.getHttpChannel().getConnection().getCreatedTimeStamp()));
//            reqChan.put("bytesIn", request.getHttpChannel().getConnection().getBytesIn());
//            reqChan.put("bytesOut", request.getHttpChannel().getConnection().getBytesOut());
//            reqChan.put("messagesIn", request.getHttpChannel().getConnection().getMessagesIn());
//            reqChan.put("messagesOut", request.getHttpChannel().getConnection().getMessagesOut());
//            rootJson.put("request", reqChan);


            flushJson(rootJson);
            print(",");

            synchronized (lock) {
                if (maxTillNow < totalBytes.get()) {
                    maxTillNow = totalBytes.get();
                }
            }

            out.println();
            out.println("Max headers size till now is: " + maxTillNow);
            out.println();
            out.flush();
        }
    }

    private static ConnectionFactory[] toConnectionFactories(int port) {
        // HTTP Configuration
        HttpConfiguration http_config = new HttpConfiguration();
        http_config.setOutputBufferSize(32768);
        http_config.setRequestHeaderSize(GsEnv.propertyInt("com.gs.14127.size").get(8192));
//        http_config.setResponseHeaderSize(30);
        http_config.setSendServerVersion(true);
        http_config.setSendDateHeader(false);
        return new ConnectionFactory[]{
                new HttpConnectionFactory(http_config)
        };
    }

    private static ConnectionFactory[] toHTTPSConnectionFactories(SslContextFactory sslContextFactory, int port) {
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
        return new ConnectionFactory[]{
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

    private static class JsonObject extends HashMap<String, Object> {
    }

    private static JsonObject create() {
        return new JsonObject();
    }
}
