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

package com.gigaspaces.start;

import com.gigaspaces.CommonSystemProperties;
import com.gigaspaces.api.BootException;
import com.gigaspaces.grid.gsa.AgentHelper;
import com.gigaspaces.internal.jvm.JVMHelper;
import com.gigaspaces.internal.jvm.JVMStatistics;
import com.gigaspaces.internal.sigar.SigarChecker;
import com.gigaspaces.logger.GSLogConfigLoader;
import com.gigaspaces.logger.RollingFileHandler;
import com.sun.jini.start.ServiceDescriptor;
import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import org.jini.rio.boot.BootUtil;
import org.jini.rio.boot.CommonClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.Introspector;
import java.io.*;
import java.lang.annotation.Annotation;
import java.lang.ref.ReferenceQueue;
import java.lang.reflect.Field;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

/**
 * Provides bootstrapping support for the GigaSpaces Service Grid.
 */
@com.gigaspaces.api.InternalApi
public class SystemBoot {
    /**
     * Token indicating a Lookup Handler should be started
     */
    public static final String LH = "LH";
    /**
     * Token indicating a Mahalo TxnManager should be started
     */
    public static final String TM = "TM";
    /**
     * Token indicating JMX MBeanServer (and required infrastructure) should not be started
     */
    public static final String NO_JMX = "NO_JMX";
    /**
     * Token indicating a Grid Service Container should be started
     */
    public static final String GSC = "GSC";
    /**
     * Token indicating a Grid Service Agent should be started
     */
    public static final String GSA = "GSA";
    /**
     * Token indicating a Grid Service Monitor should be started
     */
    public static final String GSM = "GSM";
    /**
     * Token indicating a Elastic Service Manager should be started
     */
    public static final String ESM = "ESM";

    /**
     * Token indicating a GigaSpace instance should be started
     */
    public static final String SPACE = "GS";
    /**
     * Configuration and logger property
     */
    static final String COMPONENT = "com.gigaspaces.start";
    private static Logger logger;

    static final String SERVICES_COMPONENT = COMPONENT + ".services";

    private static final SystemBootShutdownHook systemBootShutdownHook = new SystemBootShutdownHook();

    private static class SystemBootShutdownHook extends Thread {

        //holds shutdown hooks registered with SystemBoot
        private final List<Thread> shutdownHooks = Collections.synchronizedList(new ArrayList<Thread>());

        public boolean isEmpty() {
            return shutdownHooks.isEmpty();
        }

        void addShutdownHook(Thread shutdownHook) {
            shutdownHooks.add(shutdownHook);
        }

        @Override
        public void run() {
            processShutdownHooks();
        }

        // invoked by Runtime shutdown and may also be invoked explicitly by GSA shutdown command
        private StringBuilder processShutdownHooks() {
            final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("> HH:mm:ss,SSS - ");
            final StringBuilder sb = new StringBuilder();
            final ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
            final long start = System.currentTimeMillis();
            synchronized (shutdownHooks) {
                //process shutdown hooks in reverse order (of registration)
                Collections.reverse(shutdownHooks);

                sb.append(dtf.format(LocalDateTime.now()))
                        .append("Started shutdown with [")
                        .append(shutdownHooks.size())
                        .append("] registered shutdown hooks\n");

                for (Thread shutdownHook : shutdownHooks) {
                    Future<?> future = null;
                    try {
                        sb.append(dtf.format(LocalDateTime.now()))
                                .append("> call shutdown hook [")
                                .append(shutdownHook.getName())
                                .append("]\n");
                        future = singleThreadExecutor.submit(shutdownHook);
                        future.get(10, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        if (future != null) {
                            future.cancel(true);
                        }
                        if (e instanceof TimeoutException) {
                            sb.append(dtf.format(LocalDateTime.now()))
                                    .append(">> Timeout waiting for shutdown hook to complete\n");
                        } else {
                            sb.append(dtf.format(LocalDateTime.now()))
                                    .append(">> Caught Exception: ").append(e)
                                    .append("\n")
                                    .append(BootUtil.getStackTrace(e))
                                    .append("\n");
                        }
                    }
                }
                //empty list after processing is done (still under lock)
                shutdownHooks.clear();
            }
            singleThreadExecutor.shutdownNow();
            final long elapsed = System.currentTimeMillis() - start;
            sb.append(dtf.format(LocalDateTime.now())).append("Completed shutdown in [").append(elapsed).append(" ms]\n");
            return sb;
        }
    }


    public static void addShutdownHook(Thread shutdownHook) {
        systemBootShutdownHook.addShutdownHook(shutdownHook);
        if (systemBootShutdownHook.isEmpty()) {
            // remove to avoid IllegalArgumentException If the specified hook has already been registered.
            Runtime.getRuntime().removeShutdownHook(systemBootShutdownHook);
            Runtime.getRuntime().addShutdownHook(systemBootShutdownHook);
        }
    }

    private static volatile boolean runningWithinGSC = false;

    public static boolean isRunningWithinGSC() {
        return runningWithinGSC;
    }

    public static void iAmTheGSC() {
        runningWithinGSC = true;
    }

    /**
     * Get the port number the RMI Registry has been created with
     */
    public static int getRegistryPort() {
        int registryPort = 0;
        String sPort = System.getProperty(CommonSystemProperties.REGISTRY_PORT);
        if (sPort != null) {
            try {
                registryPort = Integer.parseInt(sPort);
            } catch (NumberFormatException e) {
                if (logger.isDebugEnabled())
                    logger.trace("Bad value for " +
                            "RMI Registry Port [" + sPort + "]");
            }
        }
        return (registryPort);
    }

    /**
     * Get the JMX Service URL that can be used to connect to the Platform MBeanServer. This
     * property may be <null> if the Platform MBeanServer was not be created
     */
    public static String getJMXServiceURL() {
        return (System.getProperty(CommonSystemProperties.JMX_SERVICE_URL));
    }

    /**
     * Load the platformJARs and initialize any configured system properties
     */
    public static void loadPlatform()
            throws ConfigurationException, IOException {
        //ensureSecurityManager();        
        SystemConfig sysConfig = SystemConfig.getInstance();

        /* Load system properties, to check if a logging configuration file
         * has been defined */
        //noinspection UnusedAssignment
        Properties addSysProps = sysConfig.getSystemProperties();

        URL[] platformJARs = sysConfig.getPlatformJars();
        if (platformJARs.length == 0)
            throw new RuntimeException("No platformJARs have been defined");

        CommonClassLoader commonCL = CommonClassLoader.getInstance();
        commonCL.addCommonJARs(platformJARs);

        /* Refetch the system properties */
        addSysProps = sysConfig.getSystemProperties();
        if (logger.isDebugEnabled()) {
            StringBuilder buff = new StringBuilder();
            for (Enumeration<?> en = addSysProps.propertyNames();
                 en.hasMoreElements(); ) {
                String name = (String) en.nextElement();
                String value = addSysProps.getProperty(name);
                buff.append("    ").append(name).append("=").append(value);
                buff.append("\n");
            }
            logger.debug("Configured System Properties {\n" +
                    buff.toString() +
                    "}");
        }
        Properties sysProps = System.getProperties();
        sysProps.putAll(addSysProps);
        System.setProperties(sysProps);

        logger.trace("Full list of System Properties {\n" +
                System.getProperties() +
                "}");
    }

    /**
     * Convert comma-separated String to array of Strings
     */
    private static Set<String> toSet(String s) {
        if (s.endsWith("]")) {
            s = s.substring(s.indexOf("[")+1, s.length()-1);
        }

        final Set<String> result = new LinkedHashSet<String>();
        for (StringTokenizer tok = new StringTokenizer(s, " ,") ; tok.hasMoreTokens() ; ) {
            result.add(tok.nextToken());
        }
        return result;
    }

    private static ControllablePrintStream outStream;
    private static ControllablePrintStream errStream;

    private static String processRole;

    public static void main(String[] args) {
        outStream = new ControllablePrintStream(System.out);
        System.setOut(outStream);
        errStream = new ControllablePrintStream(System.err);
        System.setErr(errStream);
        // DEAR GOD!, the Introspector uses java.awt.AppContext which is static and keeps the context
        // class loader. Spring uses Introspector, and if its loaded as part of a processing unit, then
        // it will won't release the class loader. Call this dummy method here to force the state AppContext
        // to be created with the system as its context class loader
        Introspector.flushCaches();
        RollingFileHandler.monitorCreatedFiles();
        try {
            final String command = BootUtil.arrayToDelimitedString(args, " ");
            boolean isSilent = isSilent(command);
            preProcess(args);
            processRole = getLogFileName(args);
            logger = getLogger(processRole);

            if (!isSilent) {
                if (logger.isInfoEnabled()) {
                    logger.info("Starting ServiceGrid [user=" + System.getProperty("user.name") +
                            ", command=\"" + command + "\"]");
                }

                if (logger.isTraceEnabled()) {
                    logger.trace("Security policy=" + System.getProperty("java.security.policy"));
                }
            }

            // preload sigar in boot time under the app class loader
            SigarChecker.isAvailable();

            // print the pid if we are running with gs-agent, so that the agent will know the process id
            if (AgentHelper.hasAgentId()) {
                System.out.println("pid=" + SystemInfo.singleton().os().processId());
            }

            prepareRmiGC();

            final SystemConfig systemConfig = SystemConfig.getInstance(args);
            Configuration config = systemConfig.getConfiguration();
            loadPlatform();

            final Set<String> services = toSet((String) config.getEntry(COMPONENT, "services", String.class, GSC));
            if (!isSilent)
                initJmxIfNeeded(services, systemConfig, config);
            enableDynamicLocatorsIfNeeded();

            /* Boot the services */
            final Collection<Closeable> customServices = new ArrayList<Closeable>();
            for (String service : services) {
                ServiceDescriptor serviceDescriptor = systemConfig.getServiceDescriptor(service);
                if (logger.isDebugEnabled())
                    logger.debug("Creating service " + service + (serviceDescriptor == null ? "" :
                    " with serviceDescriptor " + serviceDescriptor));
                if (serviceDescriptor != null) {
                    serviceDescriptor.create(config);
                } else {
                    Closeable customService = systemConfig.getCustomService(service);
                    if (customService != null)
                        customServices.add(customService);
                }
            }

            final Thread scheduledSystemBootThread = createScheduledSystemBootThread();
            scheduledSystemBootThread.start();

            // Use the MAIN thread as the non daemon thread to keep it alive
            final Thread mainThread = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    for (Closeable customService : customServices) {
                        try {
                            customService.close();
                        } catch (IOException e) {
                            if (logger.isWarnEnabled())
                                logger.warn("Failed to close service " + customService.toString(), e);
                        }
                    }

                    scheduledSystemBootThread.interrupt();
                    mainThread.interrupt();
                }
            });

            if (AgentHelper.hasAgentId()) {
                mainAgent(mainThread, systemConfig);
            } else {
                while (!mainThread.isInterrupted()) {
                    try {
                        Thread.sleep(Long.MAX_VALUE);
                    } catch (InterruptedException e) {
                        // do nothing, simply exit
                    }
                }
            }
        } catch (Throwable t) {
            Throwable bootException = getCauseByAnnotation(t, BootException.class);
            if (bootException != null)
                reportError(bootException, true);
            else
                reportError(t, false);
            System.exit(1);
        }
    }

    private static void reportError(Throwable t, boolean bootError) {
        if (logger != null) {
            if (bootError) {
                logger.error("Error while booting system - " + t.getMessage());
            } else
                logger.error("Error while booting system - ", t);
        } else {
            if (bootError) {
                System.err.println("Error while booting system - " + t.getMessage());
            } else {
                System.err.println("Error while booting system - " + t);
                t.printStackTrace();
            }
        }
    }

    private static Throwable getCauseByAnnotation(Throwable root, Class<? extends Annotation> annotationClass) {
        for (Throwable e = root ; e != null ; e = e.getCause()) {
            if (e.getClass().getAnnotation(annotationClass) != null)
                return e;
        }
        return null;
    }

    /**
     * HACK: detect when running gs-agent -h to reduce clutter
     */
    private static boolean isSilent(String command) {
        return command.equals("services=GSA -h") || command.startsWith("services=GSA --help");
    }

    private static void mainAgent(Thread mainThread, SystemConfig systemConfig)
            throws InterruptedException {
        // if we are running under GS Agent, add a shutdown hook that will simply wait
        // this is since we might get SIG KILL, and we want to give a change to process any
        // gsa-exit command that the GSA might have send
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        });

        waitForStopCommand(mainThread, systemConfig);
        logger.info("Received stop command from GSA, exiting");
        exitGracefully();

        System.out.println("gsa-exit-done"); //this text is processed by GigaSpacesShutdownProcessHandler.isShutdownVerification
        System.out.flush();

        gracefulSleep(20);
        System.exit(0);
    }

    private static void gracefulSleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // graceful sleep
        }
    }

    private static void waitForStopCommand(Thread mainThread, SystemConfig systemConfig) throws InterruptedException {
        // Loop waiting for a connection and a valid command
        while (!mainThread.isInterrupted()) {
            File workLocation = SystemLocations.singleton().work("gsa").toFile();
            File file = new File(workLocation, "gsa-" + AgentHelper.getGSAServiceID() + "-" + AgentHelper.getAgentId() + "-stop");
            if (file.exists()) {
                file.deleteOnExit();
                // give it a few retries to delete the file
                for (int i = 0; i < 5; i++) {
                    if (file.delete()) {
                        break;
                    }
                    Thread.sleep(5);
                }
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private static Thread createScheduledSystemBootThread() {
        final long scheduledSystemBootTime = Long.parseLong(System.getProperty("gs.start.scheduledSystemBootTime", "10000"));
        final boolean loadCleanerEnabled = System.getProperty("gs.rmi.loaderHandlerCleaner", "true").equals("true");
        final long gcCollectionWarning = Long.parseLong(System.getProperty("gs.gc.collectionTimeThresholdWarning", "60000"));
        logger.debug("GC collection time warning set to [" + gcCollectionWarning + "ms]");
        final Thread scheduledSystemBootThread = new Thread("GS-Scheduled-System-Boot-Thread") {
            @Override
            public void run() {
                RmiLoaderHandlerCleaner loaderHandlerCleaner = new RmiLoaderHandlerCleaner();
                JVMStatistics jvmStats = JVMHelper.getStatistics();
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Thread.sleep(scheduledSystemBootTime);
                    } catch (InterruptedException e) {
                        break;
                    }

                    JVMStatistics newStats = JVMHelper.getStatistics();
                    long collectionTime = newStats.getGcCollectionTime() - jvmStats.getGcCollectionTime();
                    if (collectionTime > gcCollectionWarning) {
                        logger.warn("Long GC collection occurred, took [" + collectionTime + "ms], breached threshold [" + gcCollectionWarning + "]");
                    }
                    jvmStats = newStats;

                    if (loadCleanerEnabled) {
                        loaderHandlerCleaner.clean();
                    }

                    exitIfHasAgentAndAgentIsNotRunning();
                }
            }
        };
        scheduledSystemBootThread.setDaemon(true);
        return scheduledSystemBootThread;
    }

    private static void enableDynamicLocatorsIfNeeded() {
        if (AgentHelper.hasAgentId() && AgentHelper.enableDynamicLocators()) {
            if (logger.isInfoEnabled()) {
                logger.info("Dynamic locators discovery is enabled.");
            }
            System.setProperty(CommonSystemProperties.ENABLE_DYNAMIC_LOCATORS, Boolean.TRUE.toString());
            // TODO DYNAMIC : not sure if this is required. all my tests were made with this flag set
            System.setProperty(CommonSystemProperties.MULTICAST_ENABLED_PROPERTY, Boolean.FALSE.toString());
        }
    }

    private static void initJmxIfNeeded(Set<String> services, SystemConfig systemConfig, Configuration config) {
        /* If NO_JMX is not defined, start JMX and required infrastructure services */
        if (!services.contains(NO_JMX)) {
            try {
                systemConfig.getJMXServiceDescriptor().create(config);
            } catch (Exception e) {
                if (logger.isTraceEnabled())
                    logger.trace("Unable to create the MBeanServer", e);
                else
                    logger.warn("Unable to create the MBeanServer");
            }
        } else {
            if (System.getProperty(CommonSystemProperties.JMX_ENABLED_PROP) == null) {
                if (logger.isInfoEnabled()) {
                    logger.info("\n\nJMX is disabled \n\n");
                }
            }
        }

        if (System.getProperty(CommonSystemProperties.JMX_ENABLED_PROP) == null) {
            System.setProperty(CommonSystemProperties.JMX_ENABLED_PROP, String.valueOf(!services.contains(NO_JMX)));
        }
    }

    public static String getProcessRole() {
        return processRole;
    }

    private static void preProcess(String[] args) {
        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                if (args[i].startsWith("services="))
                    args[i] = args[i].replace("services=", "com.gigaspaces.start.services=");
                // HACK to add " around parameters that have the following format: xxx=yyy (will be transformed ot xxx="yyy")
                // we do that since within the IDE it does not pass that " character.
                int eqIdx = args[i].indexOf('=');
                if (eqIdx > 0) {
                    //if the format is xxx= we will transform it to xxx=""
                    if(args[i].length()==eqIdx+1){
                        args[i] += "\"\"";
                        continue;
                    }
                    if (args[i].charAt(eqIdx + 1) != '\"') {
                        args[i] = args[i].substring(0, eqIdx) + "=\"" + args[i].substring(eqIdx + 1);
                    }
                    if (args[i].charAt(args[i].length() - 1) != '\"') {
                        args[i] += "\"";
                    }
                }
            }
        }
    }

    private static String getLogFileName(String[] args) {
        String result = System.getenv(AgentHelper.ENV_GSA_SERVICE_TYPE);
        if (result != null)
            return result;
        if (args != null) {
            for (String arg : args) {
                int index = arg.indexOf(SERVICES_COMPONENT);
                if (index != -1) {
                    result = arg.substring(SERVICES_COMPONENT.length() + 1);
                    // Unquote
                    if (result.startsWith("\""))
                        result = result.substring(1);
                    if (result.endsWith("\""))
                        result = result.substring(0, result.length() - 1);

                    result = result.replace(',', '_');
                    result = result.replace(' ', '_');
                    result = result.toLowerCase();
                    // change "lh" to "lus" (maintain backward compatibility with scripts and not change the scripts from LH to LUS)
                    result = result.replace("lh", "lus");
                    return result;
                }
            }
        }
        return null;
    }

    private static Logger getLogger(String logFileName) {
        if (AgentHelper.hasAgentId())
            logFileName += "_" + AgentHelper.getAgentId();
        System.setProperty("gs.logFileName", logFileName);
        GSLogConfigLoader.getLoader(logFileName);
        return LoggerFactory.getLogger(COMPONENT);
    }

    /**
     * Check if the RMI GC system properties are been set, if not we set it to the recommended
     * values.
     */
    private static void prepareRmiGC() {
        try {
            if (System.getProperty("sun.rmi.dgc.client.gcInterval") == null)
                System.setProperty("sun.rmi.dgc.client.gcInterval", "36000000");
            if (System.getProperty("sun.rmi.dgc.server.gcInterval") == null)
                System.setProperty("sun.rmi.dgc.server.gcInterval", "36000000");
        } catch (Exception secExc) {
            if (logger.isWarnEnabled()) {
                logger.warn("Failed to set sun.rmi.dgc.xxx system properties. \n", secExc);
            }
        }
    }

    /**
     * An RMI Loader Handler cleaner that cleans weak referneces holding exported objects.
     *
     * In RMI, they only get cleaned on export operation, when we undeploy we don't export anything
     * so memory will not be released.
     */
    public static class RmiLoaderHandlerCleaner {

        private Class<?> loaderHandlerClass;

        private Field refQueueField;

        private Field loaderTableField;

        private int numberOfFailures = 0;

        public RmiLoaderHandlerCleaner() {
            try {
                loaderHandlerClass = this.getClass().getClassLoader().loadClass("sun.rmi.server.LoaderHandler");
                refQueueField = loaderHandlerClass.getDeclaredField("refQueue");
                refQueueField.setAccessible(true);
                loaderTableField = loaderHandlerClass.getDeclaredField("loaderTable");
                loaderTableField.setAccessible(true);
            } catch (Throwable e) {
                // do nothing
            }
        }

        public void clean() {
            if (refQueueField == null) {
                return;
            }
            if (numberOfFailures > 3) {
                return;
            }
            //simulating the code done within the LoaderHandler LRMI (lookupLoader)

//            synchronized (LoaderHandler.class) {
//                /*
//                 * Take this opportunity to remove from the table entries
//                 * whose weak references have been cleared.
//                 */
//                while ((entry = (LoaderEntry) refQueue.poll()) != null) {
//                if (!entry.removed) {	// ignore entries removed below
//                    loaderTable.remove(entry.key);
//                }
//                }
//
//                // ......
//            }

            //noinspection SynchronizeOnNonFinalField
            synchronized (loaderHandlerClass) {
                try {
                    ReferenceQueue<?> referenceQueue = (ReferenceQueue<?>) refQueueField.get(null);
                    Map<?, ?> loaderTable = (Map<?, ?>) loaderTableField.get(null);
                    Object entry;
                    while ((entry = referenceQueue.poll()) != null) {
                        Field removedField = entry.getClass().getDeclaredField("removed");
                        removedField.setAccessible(true);
                        Field keyField = entry.getClass().getDeclaredField("key");
                        keyField.setAccessible(true);
                        if (!(Boolean) removedField.get(entry)) {    // ignore entries removed below
                            loaderTable.remove(keyField.get(entry));
                        }
                    }
                } catch (Throwable e) {
                    numberOfFailures++;
                    // ignore
                }
            }
        }
    }

    private static class NullOutputStream extends OutputStream {

        @Override
        public void write(int i) throws IOException {
            // ignore
        }
    }

    @SuppressWarnings("NullableProblems")
    private static class ControllablePrintStream extends PrintStream {

        private final PrintStream stream;

        private boolean ignore = false;

        private ControllablePrintStream(PrintStream stream) {
            super(new NullOutputStream());
            this.stream = stream;
        }

        @Override
        public void flush() {
            if (ignore) {
                return;
            }
            stream.flush();
        }

        @Override
        public void close() {
            if (ignore) {
                return;
            }
            if (this.stream != System.out && this.stream != System.err) {
                this.stream.close();
            }
        }

        /**
         * ignore any data sent to this stream
         */
        public void setIgnore(boolean ignore) {
            this.ignore = ignore;
        }

        @Override
        public void write(int i) {
            if (ignore) {
                return;
            }
            stream.write(i);
        }

        @Override
        public void write(byte[] bytes, int i, int i1) {
            if (ignore) {
                return;
            }
            stream.write(bytes, i, i1);
        }

        @Override
        public void print(boolean b) {
            if (ignore) {
                return;
            }
            stream.print(b);
        }

        @Override
        public void print(char c) {
            if (ignore) {
                return;
            }
            stream.print(c);
        }

        @Override
        public void print(int i) {
            if (ignore) {
                return;
            }
            stream.print(i);
        }

        @Override
        public void print(long l) {
            if (ignore) {
                return;
            }
            stream.print(l);
        }

        @Override
        public void print(float v) {
            if (ignore) {
                return;
            }
            stream.print(v);
        }

        @Override
        public void print(double v) {
            if (ignore) {
                return;
            }
            stream.print(v);
        }

        @Override
        public void print(char[] chars) {
            if (ignore) {
                return;
            }
            stream.print(chars);
        }

        @Override
        public void print(String s) {
            if (ignore) {
                return;
            }
            stream.print(s);
        }

        @Override
        public void print(Object o) {
            if (ignore) {
                return;
            }
            stream.print(o);
        }

        @Override
        public void println() {
            if (ignore) {
                return;
            }
            stream.println();
        }

        @Override
        public void println(boolean b) {
            if (ignore) {
                return;
            }
            stream.println(b);
        }

        @Override
        public void println(char c) {
            if (ignore) {
                return;
            }
            stream.println(c);
        }

        @Override
        public void println(int i) {
            if (ignore) {
                return;
            }
            stream.println(i);
        }

        @Override
        public void println(long l) {
            if (ignore) {
                return;
            }
            stream.println(l);
        }

        @Override
        public void println(float v) {
            if (ignore) {
                return;
            }
            stream.println(v);
        }

        @Override
        public void println(double v) {
            if (ignore) {
                return;
            }
            stream.println(v);
        }

        @Override
        public void println(char[] chars) {
            if (ignore) {
                return;
            }
            stream.println(chars);
        }

        @Override
        public void println(String s) {
            if (ignore) {
                return;
            }
            stream.println(s);
        }

        @Override
        public void println(Object o) {
            if (ignore) {
                return;
            }
            stream.println(o);
        }

        @Override
        public PrintStream printf(String s, Object... objects) {
            if (ignore) {
                return this;
            }
            return stream.printf(s, objects);
        }

        @Override
        public PrintStream printf(Locale locale, String s, Object... objects) {
            if (ignore) {
                return this;
            }
            return stream.printf(locale, s, objects);
        }

        @Override
        public PrintStream format(String s, Object... objects) {
            if (ignore) {
                return this;
            }
            return stream.format(s, objects);
        }

        @Override
        public PrintStream format(Locale locale, String s, Object... objects) {
            if (ignore) {
                return this;
            }
            return stream.format(locale, s, objects);
        }

        @Override
        public PrintStream append(CharSequence charSequence) {
            if (ignore) {
                return this;
            }
            return stream.append(charSequence);
        }

        @Override
        public PrintStream append(CharSequence charSequence, int i, int i1) {
            if (ignore) {
                return this;
            }
            return stream.append(charSequence, i, i1);
        }

        @Override
        public PrintStream append(char c) {
            if (ignore) {
                return this;
            }
            return stream.append(c);
        }

        @Override
        public void write(byte[] bytes) throws IOException {
            if (ignore) {
                return;
            }
            stream.write(bytes);
        }
    }

    public static void exitIfHasAgentAndAgentIsNotRunning() {
        // check for ping if we are in agent
        if (AgentHelper.hasAgentId()) {
            File file = findAgentFile(AgentHelper.getGSAServiceID());
            boolean gsaIsOut;
            if (file != null && file.exists()) {
                gsaIsOut = isGsaOut( file );
            } else {
                gsaIsOut = true;
            }
            if (gsaIsOut) {
                logger.info("GSA parent missing, exiting");
                exitGracefully();

                gracefulSleep(20);
                System.exit(1);
            }
        }
    }

    private static void exitGracefully() {
        logger.info("Exit gracefully");

        // replace output stream, so the process won't get stuck when outputting to stream when processing shutdown hooks
        outStream.setIgnore(true);
        errStream.setIgnore(true);

        StringBuilder output = systemBootShutdownHook.processShutdownHooks();

        outStream.setIgnore(false);
        errStream.setIgnore(false);

        if (logger.isInfoEnabled()) {
            logger.info("Processed shutdown-hooks:\n" + output);
        }
    }

    private static File findAgentFile(String gsaServiceID) {
        File gsaLocation = SystemLocations.singleton().work("gsa").toFile();
        File[] files = gsaLocation.listFiles();
        if (files != null) {
            for (File file : files) {
                String name = file.getName();
                if (name.startsWith("gsa-") && name.endsWith("~" + gsaServiceID))
                    return file;
            }
        }
        return null;
    }

    public static boolean isGsaOut( File file ) {
        boolean gsaIsOut = false;

        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            try (FileChannel channel = raf.getChannel()) {
                FileLock lock = channel.tryLock();
                if (lock != null) {
                    // if we can get a lock on the file, the GSA was force killed, even *without releasing the lock*
                    // which in theory, should not happen
                    lock.release();
                    gsaIsOut = true;
                }
            } catch (Exception e) {
                gsaIsOut = false;
            }
        } catch (Exception e) {
            // gsa is still holding the file
            gsaIsOut = false;
        }

        return gsaIsOut;
    }
}
