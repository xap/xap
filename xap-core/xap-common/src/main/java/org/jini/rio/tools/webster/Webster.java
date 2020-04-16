/*
 * Copyright 2005 Sun Microsystems, Inc.
 * Copyright 2005 GigaSpaces, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jini.rio.tools.webster;

import com.gigaspaces.internal.io.BootIOUtils;
import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.gigaspaces.lrmi.nio.filters.BouncyCastleSelfSignedCertificate;
import com.gigaspaces.lrmi.nio.filters.SelfSignedCertificate;
import com.gigaspaces.start.SystemInfo;

import com.gigaspaces.start.SystemLocations;
import org.jini.rio.boot.PUZipUtils;
import org.jini.rio.resources.resource.ThreadPool;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.net.URLEncoder;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ServerSocketFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;

/**
 * Webster is a HTTP server which can serve code from multiple codebases. Environment variables used
 * to control Webster are as follows: <p/> <table BORDER COLS=3 WIDTH="100%" > <tr>
 * <td>org.jini.rio.tools.webster.port</td> <td>Sets the port for webster to use</td> <td>0</td>
 * </tr> <tr> <td>org.jini.rio.tools.webster.restrictAccess</td> <td>Restricts access only to files
 * under root. Disallows any "../" notation.</td> <td>true</td> </tr>
 * <td>org.jini.rio.tools.webster.root</td> <td>Root directory to serve code from. Webster supports
 * multiple root directories which are separated by a <code>;</code></td>
 * <td>System.getProperty(user.home)</td> </tr> <p/> </table>
 */
@com.gigaspaces.api.InternalApi
public class Webster implements Runnable {

    static final int DEFAULT_MIN_THREADS = 0;
    static final int DEFAULT_MAX_THREADS = 10;
    private ServerSocket ss;
    private int port;
    private Thread runner = null;
    private volatile boolean run = true;
    private static Properties MimeTypes = new Properties();
    private String[] websterRoot;
    private ExecutorService pool;
    private int minThreads = DEFAULT_MIN_THREADS;
    private int maxThreads = DEFAULT_MAX_THREADS;
    private int soTimeout = 0;
    private static Logger logger = LoggerFactory.getLogger("org.jini.rio.tools.webster");
    private com.sun.jini.start.LifeCycle lifeCycle;
    private boolean debug = false;
    private static String SERVER_DESCRIPTION = Webster.class.getName();
    private boolean restrictAccess;
    /**
     * Either "http" or "https"
     */
    private String protocol = "http";

    private static final String WEBSTER_PORT = "org.jini.rio.tools.webster.port";


    private final Object[] putGetZipMutex = new Object[1000];

    /**
     * Create a new Webster. The port is determined by the org.jini.rio.tools.webster.port system
     * property. If the org.jini.rio.tools.webster.port system property does not exist, an anonynous
     * port will be allocated.
     */
    public Webster() throws BindException {
        this.port = Integer.getInteger(WEBSTER_PORT, 0);
        initialize();
    }

    /**
     * Create a new Webster
     *
     * @param port The port to use
     */
    public Webster(int port) throws BindException {
        this.port = Integer.getInteger(WEBSTER_PORT, 0);
        initialize();
    }

    /**
     * Create a new Webster
     *
     * @param roots The root(s) to serve code from. This is a semi-colin delimited list of
     *              directories
     */
    public Webster(String roots) throws BindException {
        this.port = Integer.getInteger(WEBSTER_PORT, 0);
        initialize(roots);
    }

    /**
     * Create a new Webster
     *
     * @param port  The port to use
     * @param roots The root(s) to serve code from. This is a semi-colin delimited list of
     *              directories
     */
    public Webster(int port, String roots) throws BindException {
        this.port = port;
        initialize(roots);
    }

    /**
     * Create a new Webster
     *
     * @param port        The port to use
     * @param roots       The root(s) to serve code from. This is a semi-colin delimited list of
     *                    directories
     * @param bindAddress TCP/IP address which Webster should bind to (null implies no specific
     *                    address)
     */
    public Webster(int port, String roots, String bindAddress)
            throws BindException {
        this.port = port;
        initialize(roots, bindAddress);
    }

    /**
     * Create a new Webster
     *
     * @param port        The port to use
     * @param roots       The root(s) to serve code from. This is a semi-colin delimited list of
     *                    directories
     * @param bindAddress TCP/IP address which Webster should bind to (null implies no specific
     *                    address)
     * @param minThreads  Minimum threads to use in the ThreadPool
     * @param maxThreads  Minimum threads to use in the ThreadPool
     */
    public Webster(int port,
                   String roots,
                   String bindAddress,
                   int minThreads,
                   int maxThreads) throws BindException {
        this.port = port;
        this.minThreads = minThreads;
        this.maxThreads = maxThreads;
        initialize(roots, bindAddress);
    }

    /**
     * Create a new Webster, compatible with the ServiceStarter mechanism in Jini 2.0
     *
     * @param options   String[] of options. Valid options are [-port port], [-roots list-of-roots],
     *                  [-bindAddress address], [-minThreads minThreads], [-maxThreads maxThreads]
     *                  [-soTimeout soTimeout]
     * @param lifeCycle The LifeCycle object, may be null
     */
    public Webster(String[] options, com.sun.jini.start.LifeCycle lifeCycle)
            throws BindException {
        if (options == null)
            throw new NullPointerException("options are null");
        this.lifeCycle = lifeCycle;
        String roots = null;
        String bindAddress = null;
        for (int i = 0; i < options.length; i++) {
            String option = options[i];
            if (option.equals("-port")) {
                i++;
                this.port = Integer.parseInt(options[i]);
            } else if (option.equals("-roots")) {
                i++;
                roots = options[i];
            } else if (option.equals("-bindAddress")) {
                i++;
                bindAddress = options[i];
            } else if (option.equals("-minThreads")) {
                i++;
                minThreads = Integer.parseInt(options[i]);
            } else if (option.equals("-maxThreads")) {
                i++;
                maxThreads = Integer.parseInt(options[i]);
            } else if (option.equals("-soTimeout")) {
                i++;
                soTimeout = Integer.parseInt(options[i]);
            } else {
                throw new IllegalArgumentException(option);
            }
        }
        initialize(roots, bindAddress);
    }

    /**
     * Initialize Webster, serving code as determined by the either the
     * org.jini.rio.tools.webster.root system property (if set) or defaulting to the user.dir system
     * property
     */
    private void initialize() throws BindException {
        String root = System.getProperty("org.jini.rio.tools.webster.root");
        if (root == null)
            root = System.getProperty("user.dir");
        initialize(root);
    }

    /**
     * Initialize Webster
     *
     * @param roots The root(s) to serve code from. This is a semi-colin delimited list of
     *              directories
     */
    private void initialize(String roots) throws BindException {
        initialize(roots, null);
    }

    /**
     * Initialize Webster
     *
     * @param roots The root(s) to serve code from. This is a semi-colin delimited list of
     *              directories
     */
    private void initialize(String roots, String bindAddress)
            throws BindException {
        for (int i = 0; i < putGetZipMutex.length; i++) {
            putGetZipMutex[i] = new Object();
        }

        restrictAccess = Boolean.parseBoolean(System.getProperty(
                "org.jini.rio.tools.webster.restrictAccess", "true"));

        String d = System.getProperty("org.jini.rio.tools.webster.debug");
        if (d != null)
            debug = true;
        d = System.getProperty("webster.debug");
        if (d != null)
            debug = true;
        setupRoots(roots);

        ServerSocketFactory serverSocketFactory = null;
        try {
            if (Boolean.getBoolean("org.jini.rio.tools.webster.tls")) {
                if (System.getProperty("javax.net.ssl.keyStore", null) != null) {
                    serverSocketFactory = SSLServerSocketFactory.getDefault();
                } else {
                    KeyStore ks = keystore();
                    if (ks != null) {
                        SSLContext sslContext = SSLContext.getInstance("TLS");
                        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                        kmf.init(ks, "foo".toCharArray());
                        sslContext.init(kmf.getKeyManagers(), null, null);
                        serverSocketFactory = sslContext.getServerSocketFactory();
                    }
                }
                protocol = "https";
            }
        } catch (Throwable e) {
            logger.error("Failed to create TLS connection", e);
        }
        if (serverSocketFactory == null) {
            serverSocketFactory = ServerSocketFactory.getDefault();
        }

        try {
            if (bindAddress == null) {
                ss = serverSocketFactory.createServerSocket(port);
            } else {
                InetAddress addr = InetAddress.getByName(bindAddress);
                ss = serverSocketFactory.createServerSocket(port, 0, addr);
            }

            port = ss.getLocalPort();

            if (logger.isDebugEnabled() || debug) {
                String msg = "Webster serving on : "
                        + ss.getInetAddress().getHostAddress() + ":" + ""
                        + port;

                if (debug) System.out.println(msg);
                logger.debug(msg);
            }
        } catch (BindException be) {
            logAndThrowBindExceptionException(bindAddress, be);
        } catch (IOException ioe) {
            BindException be = new BindException(
                    "Could not start listener. Port [" + port
                            + "] already taken");
            be.initCause(ioe);
            logAndThrowBindExceptionException(bindAddress, be);
        }

        try {
            pool = Executors.newCachedThreadPool(new ThreadFactory() {
                public Thread newThread(Runnable r) {
                    Thread t = Executors.defaultThreadFactory().newThread(r);
                    t.setName("GS-Webster" + t.getName());
                    t.setDaemon(true);
                    return t;
                }
            });
            new ThreadPool("Webster", minThreads, maxThreads);
            if (debug)
                System.out.println("Webster minThreads [" + minThreads + "], " +
                        "maxThreads [" + maxThreads + "]");
            if (logger.isDebugEnabled())
                logger.debug("Webster minThreads [" + minThreads + "], " +
                        "maxThreads [" + maxThreads + "]");
        } catch (Exception e) {
            logger.error("Could not create ThreadPool", e);
            throw new RuntimeException("Could not create Thread Pool");
        }
        if (soTimeout > 0) {
            if (debug)
                System.out.println("Webster Socket SO_TIMEOUT set to [" + soTimeout + "] millis");
            if (logger.isDebugEnabled())
                logger.debug("Webster Socket SO_TIMEOUT set to [" + soTimeout + "] millis");
        }
        runner = new GSThread(this, "Webster Runner");
        runner.setDaemon(true);
        runner.start();
    }

    private KeyStore keystore() {
        try {
            return SelfSignedCertificate.keystore();
        } catch (Throwable e) {
            e.printStackTrace();
            if (logger.isTraceEnabled()) {
                logger.trace("Failed to create self signed certificate using sun classes will try Bouncy Castle.", e);
            } else if (logger.isInfoEnabled()) {
                logger.info("Could not create self signed certificate using sun classes - trying Bouncy Castle");
            }
            try {
                return BouncyCastleSelfSignedCertificate.keystore();
            } catch (Throwable t) {
                logger.warn("Failed to create self signed certificate using Bouncy Castle classes.\n" +
                        " please add Bouncy Castle jars to classpath (or add the artifact org.bouncycastle.bcpkix-jdk15on to maven)", t);

            }
        }
        return null;
    }

    private void logAndThrowBindExceptionException(String bindAddress,
                                                   BindException be) throws BindException {
        String hostAddress = SystemInfo.singleton().network().getHostId();

        if (bindAddress == null) {
            logger.warn("Failed to bind socket on [" + hostAddress + "], port [" + port + "], please check your network configuration", be);
        } else {
            logger.warn("Failed to bind socket on [" + hostAddress + "], bind address [" + bindAddress + "], port [" + port + "], please check your network configuration", be);
        }
        throw be;
    }

    /**
     * Get the roots Webster is serving as a semicolon delimited String
     */
    public String getRoots() {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < websterRoot.length; i++) {
            if (i > 0)
                buffer.append(";");
            buffer.append(websterRoot[i]);
        }
        return (buffer.toString());
    }

    public String getURL() {
        return getProtocol() + "://" + getAddress() + ":" + getPort() + "/";
    }

    /**
     * Get address that Webster is bound to
     *
     * @return The host address the server socket Webster is using is bound to. If the socket is
     * null, return null.
     */
    public String getAddress() {
        if (ss == null)
            return (null);

        final InetAddress inetAddress = SystemInfo.singleton().network().isPublicHostConfigured() ?
                SystemInfo.singleton().network().getPublicHost() :
                ss.getInetAddress();

        return BootIOUtils.wrapIpv6HostAddressIfNeeded(inetAddress);
    }

    public String getHostName() {
        return ss == null ? null : ss.getInetAddress().getHostName();
    }

    public String getProtocol() {
        return protocol;
    }

    /**
     * Setup the websterRoot property
     */
    private void setupRoots(String roots) {
        if (roots == null)
            throw new NullPointerException("roots is null");
        StringTokenizer tok = new StringTokenizer(roots, ";");
        websterRoot = new String[tok.countTokens()];
        if (websterRoot.length > 1) {
            for (int j = 0; j < websterRoot.length; j++) {
                websterRoot[j] = tok.nextToken();
                if (debug)
                    System.out.println("Root " + j + " = " + websterRoot[j]);
                if (logger.isDebugEnabled())
                    logger.debug("Root " + j + " = " + websterRoot[j]);
            }
        } else {
            websterRoot[0] = roots;
            if (debug)
                System.out.println("Root  = " + websterRoot[0]);
            if (logger.isDebugEnabled())
                logger.debug("Root  = " + websterRoot[0]);
        }
    }

    /**
     * Terminate a running Webster instance
     */
    public void terminate() {
        run = false;
        if (ss != null) {
            try {
                ss.close();
            } catch (Exception e) {
                logger.warn("Exception closing Webster ServerSocket", e);
            }
        }
        if (lifeCycle != null) {
            lifeCycle.unregister(this);
        }
        if (pool != null) {
            pool.shutdown();
        }
    }

    /**
     * Get the port Webster is bound to
     */
    public int getPort() {
        return (port);
    }

    /**
     * Read up to CRLF, return false if EOF
     */
    private boolean readLine(InputStream in, StringBuffer buf)
            throws IOException {
        while (true) {
            int c = in.read();
            if (c < 0)
                return (buf.length() > 0);
            if (c == '\r') {
                in.mark(1);
                c = in.read();
                if (c != '\n')
                    in.reset();
                return (true);
            }
            if (c == '\n')
                return (true);
            buf.append((char) c);
        }
    }

    /**
     * Read the request and return the initial request line.
     */
    private String getRequest(BufferedInputStream in) throws IOException {
        StringBuffer buf = new StringBuffer(80);
        do {
            if (!readLine(in, buf))
                return (null);
        } while (buf.length() == 0);
        String req = buf.toString();
        do {
            buf.setLength(0);
        } while (readLine(in, buf) && buf.length() > 0);
        return (req);
    }

    private String getRequestLine(BufferedInputStream in) throws IOException {
        StringBuffer buf = new StringBuffer(80);
        do {
            if (!readLine(in, buf))
                return (null);
        } while (buf.length() == 0);
        return buf.toString();
    }

    private String getNextRequestLine(BufferedInputStream in) throws IOException {
        StringBuffer buf = new StringBuffer(80);
        if (!readLine(in, buf))
            return (null);
        if (buf.length() == 0) {
            return null;
        }
        return buf.toString();
    }

    public void run() {
        Socket s = null;
        try {
            loadMimes();
            String fileName = null;
            while (run) {
                s = ss.accept(); // accept incoming requests
                if (soTimeout > 0) {
                    s.setSoTimeout(soTimeout);
                }
                BufferedInputStream in = new BufferedInputStream(s.getInputStream());
                String line = null;
                Properties header = new Properties();
                try {
                    line = getRequestLine(in);
                    int port = s.getPort();
                    String from = s.getInetAddress().getHostAddress() + ":" + port;
                    if (debug) {
                        StringBuffer buff = new StringBuffer();
                        buff.append("From: " + from + ", ");
                        if (soTimeout > 0)
                            buff.append("SO_TIMEOUT: " + soTimeout + ", ");
                        buff.append("Request: " + line);
                        System.out.println(buff.toString());
                    }
                    if (logger.isDebugEnabled()) {
                        StringBuffer buff = new StringBuffer();
                        buff.append("From: " + from + ", ");
                        if (soTimeout > 0)
                            buff.append("SO_TIMEOUT: " + soTimeout + ", ");
                        buff.append("Request: " + line);
                        logger.debug(buff.toString());
                    }
                    if (line != null) {
                        StringTokenizer tokenizer =
                                new StringTokenizer(line, " ");
                        if (!tokenizer.hasMoreTokens())
                            break;
                        String token = tokenizer.nextToken();
                        fileName = tokenizer.nextToken();
                        if (fileName.startsWith("/"))
                            fileName = fileName.substring(1);
                        if (token.equals("GET")) {
                            header.setProperty("GET", fileName);
                        } else if (token.equals("PUT")) {
                            header.setProperty("PUT", fileName);
                        } else if (token.equals("DELETE")) {
                            header.setProperty("DELETE", fileName);
                        } else if (token.equals("HEAD")) {
                            header.setProperty("HEAD", fileName);
                        }
                        while ((line = getNextRequestLine(in)) != null) {
                            // read the header parameters
                            int colIdx = line.indexOf(':');
                            if (colIdx > 0) {
                                String key = line.substring(0, colIdx);
                                String value = line.substring(colIdx + 1);
                                header.setProperty(key.trim(), value.trim());
                            }
                        }
                        if (restrictAccess) {
                            if (fileName.indexOf("..") != -1) {
                                // trying to navigate out, and we restrict it, bail
                                if (logger.isDebugEnabled())
                                    logger.debug(
                                            "forbidden [" + line + "] " +
                                                    "from " + from);
                                DataOutputStream clientStream =
                                        new DataOutputStream(
                                                new BufferedOutputStream(
                                                        s.getOutputStream()));
                                clientStream.writeBytes(
                                        "HTTP/1.0 403 Forbidden\r\n\r\n");
                                clientStream.flush();
                                clientStream.close();
                                try {
                                    s.close();
                                } catch (Exception e) {
                                    // do nothing
                                }
                                continue;
                            }
                        }
                        if (header.getProperty("GET") != null) {
                            pool.execute(new GetFile(s, fileName, header));
                        } else if (header.getProperty("PUT") != null) {
                            pool.execute(new PutFile(in, s, fileName, header));
                        } else if (header.getProperty("DELETE") != null) {
                            throw new IOException("DELETE not allowed");
                        } else if (header.getProperty("HEAD") != null) {
                            pool.execute(new Head(s, fileName, header));
                        } else {
                            if (debug)
                                System.out.println(
                                        "bad request [" + line + "] from " + from);
                            if (logger.isDebugEnabled())
                                logger.debug(
                                        "bad request [" + line + "] " +
                                                "from " + from);
                            DataOutputStream clientStream =
                                    new DataOutputStream(
                                            new BufferedOutputStream(
                                                    s.getOutputStream()));
                            clientStream.writeBytes(
                                    "HTTP/1.0 400 Bad Request\r\n\r\n");
                            clientStream.flush();
                            clientStream.close();
                        }
                    } /* if line != null */
                } catch (Exception e) {
                    logger.warn("Getting Request from " + s.getInetAddress() + ":" + s.getPort(), e);
                    DataOutputStream clientStream = null;
                    try {
                        clientStream =
                                new DataOutputStream(
                                        new BufferedOutputStream(
                                                s.getOutputStream()));
                        clientStream.writeBytes(
                                "HTTP/1.0 500 Internal Server Error\n" +
                                        "MIME-Version: 1.0\n" +
                                        "Server: " + SERVER_DESCRIPTION + "\n" +
                                        "\n\n<H1>500 Internal Server Error</H1>\n"
                                        + e);
                        clientStream.flush();
                    } catch (Exception e1) {
                        logger.warn("Failed to send 500 response", e);
                    } finally {
                        try {
                            if (clientStream != null) {
                                clientStream.close();
                            }
                        } catch (Exception e2) {
                            // do nothing here, we already reported enough errors
                        }
                    }
                }
            }
        } catch (Exception e) {
            if (run) {
                e.printStackTrace();
                logger.warn("Processing HTTP Request", e);
            }
        }
    }

    // load the properties file
    void loadMimes() throws IOException {
        boolean loadDefaults = true;
        if (debug)
            System.out.println("Loading mimetypes ... ");
        if (logger.isDebugEnabled())
            logger.debug("Loading mimetypes ... ");
        ClassLoader ccl = Thread.currentThread().getContextClassLoader();
        URL fileURL = ccl != null ?
                ccl.getResource("org/jini/rio/tools/webster/mimetypes.properties") : null;
        if (fileURL != null) {
            try {
                InputStream is = fileURL.openStream();
                MimeTypes.load(is);
                is.close();
                loadDefaults = false;
                if (debug)
                    System.out.println("Mimetypes loaded");
                if (logger.isDebugEnabled())
                    logger.debug("Mimetypes loaded");
            } catch (IOException ioe) {
                logger.error("Loading Mimetypes", ioe);
            }
        } else {
            if (loadDefaults) {
                if (debug)
                    System.out.println("mimetypes.properties not found, " +
                            "loading defaults");
                if (logger.isDebugEnabled())
                    logger.debug("mimetypes.properties not found, loading defaults");
            }
            MimeTypes.put("jpg", "image/jpg");
            MimeTypes.put("jpeg", "image/jpg");
            MimeTypes.put("jpe", "image/jpg");
            MimeTypes.put("gif", "image/gif");
            MimeTypes.put("htm", "text/html");
            MimeTypes.put("html", "text/html");
            MimeTypes.put("txt", "text/plain");
            MimeTypes.put("qt", "video/quicktime");
            MimeTypes.put("mov", "video/quicktime");
            MimeTypes.put("class", "application/octet-stream");
            MimeTypes.put("mpg", "video/mpeg");
            MimeTypes.put("mpeg", "video/mpeg");
            MimeTypes.put("mpe", "video/mpeg");
            MimeTypes.put("au", "audio/basic");
            MimeTypes.put("snd", "audio/basic");
            MimeTypes.put("wav", "audio/x-wave");
            MimeTypes.put("JNLP", "application/x-java-jnlp-file");
            MimeTypes.put("jnlp", "application/x-java-jnlp-file");
            MimeTypes.put("java", "application/java");
            MimeTypes.put("jar", "application/java");
            MimeTypes.put("JAR", "application/java");
        }
    } // end of loadMimes

    protected File parseFileName(String filename) {
        StringBuffer fn = new StringBuffer(filename);
        for (int i = 0; i < fn.length(); i++) {
            if (fn.charAt(i) == '/')
                fn.replace(i, i + 1, File.separator);
        }
        File f = null;
        String[] roots = expandRoots();
        for (int i = 0; i < roots.length; i++) {
            f = new File(roots[i], fn.toString());
            if (f.exists()) {
                return (f);
            }
        }
        return (f);
    }

    protected String[] expandRoots() {
        List expandedRoots = new LinkedList();
        if (hasWildcard()) {
            String[] rawRoots = websterRoot;
            for (int i = 0; i < rawRoots.length; i++) {
                String root = rawRoots[i];
                int wildcard;
                if ((wildcard = root.indexOf('*')) != -1) {
                    String prefix = root.substring(0, wildcard);
                    File prefixFile = new File(prefix);
                    if (prefixFile.exists()) {
                        String suffix =
                                (wildcard < (root.length() - 1)) ?
                                        root.substring(wildcard + 1) : "";
                        String[] children = prefixFile.list();
                        for (int j = 0; j < children.length; j++) {
                            expandedRoots.add(prefix + children[j] + suffix);
                        }
                    } else {
                        // Eat the root entry if it's wildcarded and doesn't
                        // exist
                    }
                } else {
                    expandedRoots.add(rawRoots[i]);
                }
            }
        }
        String[] roots = null;
        if (expandedRoots.size() > 0) {
            roots = (String[]) expandedRoots.toArray(
                    new String[expandedRoots.size()]);
        } else {
            roots = websterRoot;
        }
        return (roots);
    }

    /**
     * See if the root is using a wildcard
     */
    boolean hasWildcard() {
        boolean wildcarded = false;
        for (int i = 0; i < websterRoot.length; i++) {
            String root = websterRoot[i];
            if ((root.indexOf('*')) != -1) {
                wildcarded = true;
                break;
            }
        }
        return (wildcarded);
    }

    class Head implements Runnable {
        private final Socket client;
        private final String fileName;
        private String header;
        private DataInputStream requestedFile;
        private int fileLength;

        Head(Socket s, String fileName, Properties header) {
            client = s;
            this.fileName = fileName;
        }

        public void run() {
            StringBuffer dirData = new StringBuffer();
            StringBuffer logData = new StringBuffer();
            try {
                File getFile = parseFileName(fileName);
                logData.append("Do HEAD: input=" + fileName + ", " +
                        "parsed=" + getFile + ", ");
                if (getFile.isDirectory()) {
                    logData.append("directory located");
                    String files[] = getFile.list();
                    for (int i = 0; i < files.length; i++) {
                        File f = new File(getFile, files[i]);
                        dirData.append(f.toString().substring(
                                getFile.getParent().length()));
                        dirData.append("\t");
                        if (f.isDirectory())
                            dirData.append("d");
                        else
                            dirData.append("f");
                        dirData.append("\t");
                        dirData.append(f.length());
                        dirData.append("\t");
                        dirData.append(f.lastModified());
                        dirData.append("\n");
                    }
                    fileLength = dirData.length();
                    String fileType = MimeTypes.getProperty("txt");
                    if (fileType == null)
                        fileType = "application/java";
                    header = "HTTP/1.0 200 OK\n" +
                            "Allow: GET\nMIME-Version: 1.0\n" +
                            "Server: " + SERVER_DESCRIPTION + "\n" +
                            "Content-Type: " + fileType + "\n" +
                            "Content-Length: " + fileLength + "\r\n\r\n";
                } else if (getFile.exists()) {
                    requestedFile =
                            new DataInputStream(
                                    new BufferedInputStream(new FileInputStream(getFile)));
                    fileLength = requestedFile.available();
                    String fileType =
                            fileName.substring(fileName.lastIndexOf(".") + 1,
                                    fileName.length());
                    fileType = MimeTypes.getProperty(fileType);
                    logData.append("file size: [" + fileLength + "]");
                    header = "HTTP/1.0 200 OK\n"
                            + "Allow: GET\nMIME-Version: 1.0\n"
                            + "Server: " + SERVER_DESCRIPTION + "\n"
                            + "Content-Type: "
                            + fileType
                            + "\n"
                            + "Content-Length: "
                            + fileLength
                            + "\r\n\r\n";
                } else {
                    header = "HTTP/1.1 404 Not Found\r\n\r\n";
                    logData.append("not found");
                }

                if (debug)
                    System.out.println(logData.toString());
                if (logger.isDebugEnabled())
                    logger.debug(logData.toString());

                DataOutputStream clientStream =
                        new DataOutputStream(
                                new BufferedOutputStream(client.getOutputStream()));
                clientStream.writeBytes(header);
                clientStream.flush();
                clientStream.close();
            } catch (Exception e) {
                logger.warn("Error closing Socket", e);
            } finally {
                try {
                    client.close();
                } catch (IOException e2) {
                    logger.warn(
                            "Closing incoming socket",
                            e2);
                }
            }
        } // end of Head
    }

    private static Random zipFileRandom = new Random();

    class GetFile implements Runnable {
        private final Socket client;
        private final String fileName;
        private String header;
        private DataInputStream requestedFile;
        private long fileLength;
        private final Properties rheader;

        GetFile(Socket s, String fileName, Properties header) {
            client = s;
            this.fileName = fileName;
            this.rheader = header;
        }

        public String ignoreCaseProperty(Properties props, String field) {
            Enumeration names = props.propertyNames();
            while (names.hasMoreElements()) {
                String propName = (String) names.nextElement();
                if (field.equalsIgnoreCase(propName)) {
                    return (props.getProperty(propName));
                }
            }
            return (null);
        }

        public void run() {
            StringBuilder dirData = new StringBuilder();
            StringBuilder logData = new StringBuilder();
            try {
                File[] files = null;
                if (fileName.equals("") || fileName.equals("/")) {
                    logData.append(
                            "Do GET: input=" + fileName + ", " +
                                    "list roots" + ", "
                    );
                    List<File> filesList = new ArrayList<File>();
                    for (int i = 0; i < websterRoot.length; i++) {
                        String root = websterRoot[i];
                        File rootFile = new File(root);
                        filesList.addAll(Arrays.<File>asList(BootIOUtils.listFiles(rootFile)));
                    }
                    files = (File[]) filesList.toArray(new File[filesList.size()]);
                } else if (fileName.equals("list-pu")) {
                    List<File> filesList = new ArrayList<File>();
                    for (int i = 0; i < websterRoot.length; i++) {
                        String root = websterRoot[i];
                        File rootFile = new File(root);
                        File[] pus = BootIOUtils.listFiles(rootFile, new FileFilter() {
                            public boolean accept(File pathname) {
                                if (!pathname.isDirectory()) {
                                    return false;
                                }
                                return new File(pathname, "META-INF/spring/pu.xml").exists()
                                        || new File(pathname, "WEB-INF/web.xml").exists()
                                        || new File(pathname, "pu.config").exists()
                                        || new File(pathname, "pu.interop.config").exists();
                            }
                        });
                        filesList.addAll(Arrays.<File>asList(pus));
                    }
                    files = (File[]) filesList.toArray(new File[filesList.size()]);
                }
                File getFile = null;
                if (files == null) {

                    getFile = parseFileName(fileName);
                    logData.append(
                            "Do GET: input=" + fileName + ", " +
                                    "parsed=" + getFile + ", "
                    );
                    if (getFile.isDirectory()) {
                        files = BootIOUtils.listFiles(getFile);
                    }
                }

                String packageDir = ignoreCaseProperty(rheader, "Package");
                if (packageDir != null && "true".equalsIgnoreCase(packageDir)) {
                    synchronized (putGetZipMutex[Math.abs(getFile.getName().hashCode() % putGetZipMutex.length)]) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Received GET operation for packaged processing unit [" + getFile.getName() + "]");
                        }
                        if (getFile.exists()) {
                            if (!getFile.isDirectory()) {
                                throw new IOException("Trying to package a file [" + getFile.getAbsolutePath() + "] that is not a directory");
                            }
                            File tempLocation = new File(System.getProperty("com.gs.work", SystemLocations.singleton().work("webster").toString()));
                            tempLocation.mkdirs();
                            File packagedZip;
                            try {
                                String name = getFile.getName();
                                packagedZip = new File(tempLocation, name + zipFileRandom.nextLong() + ".zip");
                            } catch (Exception e) {
                                IOException ioe = new IOException("Failed to create temporary zip file at [" + tempLocation.getAbsolutePath() + "] from [" + getFile.getAbsolutePath() + "]");
                                ioe.initCause(e);
                                throw ioe;
                            }
                            try {
                                PUZipUtils.zip(getFile, packagedZip);
                            } catch (Exception e) {
                                IOException ioe = new IOException("Failed to build temporary zip file at [" + packagedZip.getAbsolutePath() + "] from [" + getFile.getAbsolutePath() + "]");
                                ioe.initCause(e);
                                throw ioe;
                            }
                            getFile = packagedZip;

                            RandomAccessFile ras = new RandomAccessFile(packagedZip, "rw");
                            ras.getFD().sync();
                            ras.close();

                            if (logger.isDebugEnabled()) {
                                logger.debug("Created temporary packaged (zip) processing unit to send back at [" + getFile.getAbsolutePath() + "] with size [" + getFile.length() + "]");
                            }
                            files = null;
                        }
                    }
                }

                if (files != null) {
                    String list = ignoreCaseProperty(rheader, "list");
                    if (!fileName.equals("list-pu") && !"true".equals(list)) {
                        dirData.append("");
                    } else {
                        logData.append("directory located");
                        for (int i = 0; i < files.length; i++) {
                            File f = files[i];
                            dirData.append(URLEncoder.encode(f.getName().trim(), "UTF-8"));
                            dirData.append("\t");
                            if (f.isDirectory()) {
                                dirData.append("d");
                            } else {
                                dirData.append("f");
                            }
                            dirData.append("\t");
                            dirData.append(f.length());
                            dirData.append("\t");
                            dirData.append(f.lastModified());
                            dirData.append("\n");
                        }
                    }
                    fileLength = dirData.length();
                    String fileType = MimeTypes.getProperty("txt");
                    if (fileType == null) {
                        fileType = "application/java";
                    }
                    header = "HTTP/1.0 200 OK\n"
                            + "Allow: GET\nMIME-Version: 1.0\n"
                            + "Server: " + SERVER_DESCRIPTION + "\n"
                            + "Content-Type: "
                            + fileType
                            + "\n"
                            + "Content-Length: "
                            + fileLength
                            + "\r\n\r\n";
                } else if (getFile.exists()) {
                    requestedFile =
                            new DataInputStream(
                                    new BufferedInputStream(new FileInputStream(getFile))
                            );
                    fileLength = getFile.length();
                    String fileType =
                            fileName.substring(
                                    fileName.lastIndexOf(".") + 1,
                                    fileName.length()
                            );
                    fileType = MimeTypes.getProperty(fileType);
                    header = "HTTP/1.0 200 OK\n"
                            + "Allow: GET\nMIME-Version: 1.0\n"
                            + "Server: " + SERVER_DESCRIPTION + "\n"
                            + "Content-Type: "
                            + fileType
                            + "\n"
                            + "Content-Length: "
                            + fileLength
                            + "\r\n\r\n";
                } else {
                    header = "HTTP/1.0 404 Not Found\r\n\r\n";
                }
                DataOutputStream clientStream =
                        new DataOutputStream(
                                new BufferedOutputStream(client.getOutputStream())
                        );
                clientStream.writeBytes(header);

                if (files != null) {
                    clientStream.writeBytes(dirData.toString());
                } else if (getFile.exists()) {
                    byte[] buffer = new byte[4096];
                    int bytesRead = -1;
                    try {
                        while ((bytesRead = requestedFile.read(buffer)) != -1) {
                            clientStream.write(buffer, 0, bytesRead);
                        }
                    } catch (Exception e) {
                        String s = "Sending [" +
                                getFile.getAbsolutePath() + "], " +
                                "size [" + fileLength + "], " +
                                "to client at " +
                                "[" +
                                client.getInetAddress().getHostAddress() +
                                "]";
                        if (logger.isDebugEnabled()) {
                            logger.debug(s, e);
                        }
                        if (debug) {
                            System.out.println(s);
                            e.printStackTrace();
                        }
                    }
                    requestedFile.close();
                    if (packageDir != null && "true".equalsIgnoreCase(packageDir)) {
                        getFile.delete();
                    }
                } else {
                    logData.append("not found");
                }
                if (debug) {
                    System.out.println(logData.toString());
                }
                if (logger.isDebugEnabled()) {
                    logger.debug(logData.toString());
                }
                clientStream.flush();
                clientStream.close();
            } catch (Exception e) {
                logger.error("GET operation failed for [" + fileName + "]", e);
            } finally {
                try {
                    client.close();
                } catch (IOException e2) {
                    logger.warn(
                            "Closing incoming socket",
                            e2);
                }
            }
        } // end of GetFile
    }

    class PutFile implements Runnable {
        private final Socket client;

        private final BufferedInputStream in;

        private final String fileName;

        private String header;
        private final Properties rheader;

        PutFile(BufferedInputStream in, Socket s, String fileName, Properties header) {
            this.in = in;
            rheader = header;
            client = s;
            this.fileName = fileName;
        }

        public void run() {
            try {
                // check to see if the file exists if it does the return code
                // will be 200 if it dosent it will be 201
                File putFile = parseFileName(fileName);
                if (putFile.exists()) {
                    header = "HTTP/1.0 200 OK\r\n"
                            + "Allow: PUT\r\n"
                            + "MIME-Version: 1.0\r\n"
                            + "Server : Webster: a Java HTTP Server \r\n" + "\r\n\r\n"
                            + "\n\n <H1>200 File updated</H1>\n";
                } else {
                    header = "HTTP/1.0 201 Created\r\n"
                            + "Allow: PUT\r\n"
                            + "MIME-Version: 1.0\r\n"
                            + "Server : Webster: a Java HTTP Server \r\n" + "\r\n\r\n"
                            + "\n\n <H1>201 File Created</H1>\n";
                }

                String putFileName = putFile.getName();
                if (putFileName.length() >= 4 && putFileName.charAt(putFileName.length() - 4) == '.') {
                    putFileName = putFileName.substring(0, putFileName.length() - 4);
                }
                synchronized (putGetZipMutex[Math.abs(putFileName.hashCode() % putGetZipMutex.length)]) {
                    OutputStream requestedFile = new BufferedOutputStream(new FileOutputStream(putFile));
                    try {
                        String extract = ignoreCaseProperty(rheader, "Extract");
                        if (!"true".equalsIgnoreCase(extract)) {
                            throw new IOException("PUT is not allowed");
                        }

                        int length = Integer.parseInt(ignoreCaseProperty(rheader, "Content-Length"));
                        if (logger.isDebugEnabled()) {
                            logger.debug("Putting file [" + putFile.getAbsolutePath() + "] with length [" + length + "]");
                        }
                        for (int i = 0; i < length; i++) {
                            requestedFile.write(in.read());
                        }
                        requestedFile.flush();
                        requestedFile.close();

                        if (extract != null && extract.equalsIgnoreCase("true")) {

                            //1. unzip to tempDestDir
                            //2. swap destDir with tempDestDir
                            // 2.1. rename destDir -> oldDestDir 
                            // 2.2. rename tempDestDir -> destDir
                            // 2.3. delete oldDestDir

                            File directory = putFile.getParentFile();
                            String prefix = putFileName;
                            String suffix = "";
                            File tempDestDir = File.createTempFile(prefix, suffix, directory);
                            deleteFileWithRetry(tempDestDir);
                            tempDestDir.mkdirs();

                            if (logger.isDebugEnabled()) {
                                logger.debug("Extracting file [" + putFile.getAbsolutePath() + "] to [" + tempDestDir.getAbsolutePath() + "]");
                            }
                            PUZipUtils.unzip(putFile, tempDestDir);
                            deleteFileWithRetry(putFile);

                            File oldDestDir = File.createTempFile(prefix + "_old_", suffix, directory);
                            deleteFileWithRetry(oldDestDir);
                            File destDir = new File(putFile.getParentFile(), putFileName);
                            if (destDir.exists()) {
                                try {
                                    // This operation could fail on Windows if a file in destDir is locked by another process 
                                    // For example the client zipping this folder before deploying it.
                                    // So do not deploy from the same folder that Webster is going to use
                                    renameFile(destDir, oldDestDir);
                                } catch (IOException e) {
                                    if (logger.isWarnEnabled()) {
                                        logger.warn(
                                                "Failed to rename " + destDir + " will try to delete it instead. " +
                                                        "This could happen on Windows if " + destDir + " is still locked by another process.", e);
                                    }
                                    deleteFileWithRetry(destDir);
                                }
                            }
                            renameFile(tempDestDir, destDir);
                            try {
                                deleteFileWithRetry(oldDestDir);
                            } catch (IOException e) {
                                if (logger.isWarnEnabled()) {
                                    logger.warn(
                                            "Failed to delete temp folder " + oldDestDir + " Will try to delete it after this process exists.", e);
                                }
                                destDir.deleteOnExit();
                            }
                        }
                    } catch (IOException e) {
                        logger.error("Failed to write file [" + fileName + "]", e);
                        header = "HTTP/1.0 500 Internal Server Error\r\n"
                                + "Allow: PUT\r\n"
                                + "MIME-Version: 1.0\r\n"
                                + "Server: " + SERVER_DESCRIPTION + "\r\n" + "\r\n\r\n"
                                + "\n\n <H1>500 Internal Server Error</H1>\n"
                                + e;
                    } finally {
                        try {
                            requestedFile.close();
                        } catch (Exception e) {
                            // ignore
                        }
                    }
                }
                DataOutputStream clientStream =
                        new DataOutputStream(
                                new BufferedOutputStream(client.getOutputStream()));
                clientStream.writeBytes(header);
                clientStream.flush();
                clientStream.close();
            } catch (Exception e) {
                logger.warn("Closing Socket", e);
            } finally {
                try {
                    client.close();
                } catch (IOException e2) {
                    logger.warn(
                            "Closing incoming socket",
                            e2);
                }
            }
        }

        private void renameFile(File srcDir, File targetDir)
                throws IOException {
            if (logger.isDebugEnabled()) {
                logger.debug("Renaming " + srcDir + " to " + targetDir);
            }
            final boolean success = srcDir.renameTo(targetDir);
            if (!success) {
                throw new IOException("Renaming " + srcDir + " to " + targetDir + " failed.");
            }
        }

        private void deleteFileWithRetry(File file) throws IOException {
            if (logger.isDebugEnabled()) {
                logger.debug("Trying to delete " + file);
            }
            boolean deleted = false;
            for (int i = 0; i < 5; i++) {
                deleted = deleteFile(file);
                if (deleted) {
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // no matter
                }
            }
            if (!deleted) {
                throw new IOException("Failed to delete [" + file.getAbsolutePath() + "]");
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Deleted " + file);
            }
        }

        public String ignoreCaseProperty(Properties props, String field) {
            Enumeration names = props.propertyNames();
            while (names.hasMoreElements()) {
                String propName = (String) names.nextElement();
                if (field.equalsIgnoreCase(propName)) {
                    return (props.getProperty(propName));
                }
            }
            return (null);
        }
    } // end of PutFile

    class DelFile implements Runnable {
        private final Socket client;
        private final String fileName;
        private String header;

        DelFile(Socket s, String fileName, Properties header) {
            client = s;
            this.fileName = fileName;
        }

        public void run() {
            try {
                File putFile = parseFileName(fileName);
                if (!putFile.exists()) {
                    header = "HTTP/1.0 404 File not found\n"
                            + "Allow: GET\n"
                            + "MIME-Version: 1.0\n"
                            + "Server: " + SERVER_DESCRIPTION + "\n"
                            + "\n\n <H1>404 File not Found</H1>\n"
                            + "<BR>";
                } else if (putFile.delete()) {
                    header = "HTTP/1.0 200 OK\n"
                            + "Allow: PUT\n"
                            + "MIME-Version: 1.0\n"
                            + "Server: " + SERVER_DESCRIPTION + "\n"
                            + "\n\n <H1>200 File successfully deleted</H1>\n";
                } else {
                    header = "HTTP/1.0 500 Internal Server Error\n"
                            + "Allow: PUT\n"
                            + "MIME-Version: 1.0\n"
                            + "Server: " + SERVER_DESCRIPTION + "\n"
                            + "\n\n <H1>500 File could not be deleted</H1>\n";
                }
                DataOutputStream clientStream =
                        new DataOutputStream(
                                new BufferedOutputStream(client.getOutputStream()));
                clientStream.writeBytes(header);
                clientStream.flush();
                clientStream.close();
            } catch (Exception e) {
                logger.warn("Closing Socket", e);
            } finally {
                try {
                    client.close();
                } catch (IOException e2) {
                    logger.warn(
                            "Closing incoming socket",
                            e2);
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println("start webster");
        System.setProperty("org.jini.rio.tools.webster.tls", "true");
        try {
            Webster webster = new Webster(8080, "/home/barakbo/tmp/setups/gigaspaces-xap-premium-11.0.0-m7/bin");
            webster.debug = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        //noinspection ResultOfMethodCallIgnored
        System.in.read();
        System.out.println("webster exiting");
    }

    public static boolean deleteFile(File dir) {
        if (!dir.exists()) {
            return true;
        }
        boolean globalSuccess = true;
        if (dir.isDirectory()) {
            String[] children = dir.list();
            if (children != null) {
                for (int i = 0; i < children.length; i++) {
                    boolean success = deleteFile(new File(dir, children[i]));
                    if (!success) {
                        globalSuccess = false;
                    }
                }
            }
        }
        // The directory is now empty so delete it
        if (!dir.delete()) {
            globalSuccess = false;
        }
        return globalSuccess;
    }
}
