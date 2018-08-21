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
import org.jini.rio.resources.util.SecurityPolicyLoader;

import java.beans.Introspector;
import java.io.*;
import java.lang.ref.ReferenceQueue;
import java.lang.reflect.Field;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.rmi.RMISecurityManager;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

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

    static final List<Thread> shutdownHooks = new ArrayList<Thread>();

    public static void addShutdownHook(Thread shutdownHook) {
        synchronized (shutdownHooks) {
            shutdownHooks.add(shutdownHook);
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        }
    }

    @SuppressWarnings("unused")
    public static void removeShutdownHook(Thread shutdownHook) {
        synchronized (shutdownHooks) {
            shutdownHooks.remove(shutdownHook);
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
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
     * Utility routine that sets a security manager (if one isn't already present) and the security
     * policy
     */
    public synchronized static void ensureSecurityManager() {
        SecurityPolicyLoader.load(SystemBoot.class, "policy.all");
        //noinspection deprecation
        System.setSecurityManager(new RMISecurityManager());
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
                if (logger.isLoggable(Level.FINE))
                    logger.finest("Bad value for " +
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
        if (logger.isLoggable(Level.FINE)) {
            StringBuilder buff = new StringBuilder();
            for (Enumeration<?> en = addSysProps.propertyNames();
                 en.hasMoreElements(); ) {
                String name = (String) en.nextElement();
                String value = addSysProps.getProperty(name);
                buff.append("    ").append(name).append("=").append(value);
                buff.append("\n");
            }
            logger.fine("Configured System Properties {\n" +
                    buff.toString() +
                    "}");
        }
        Properties sysProps = System.getProperties();
        sysProps.putAll(addSysProps);
        System.setProperties(sysProps);

        logger.finest("Full list of System Properties {\n" +
                System.getProperties() +
                "}");
    }

    /**
     * Convert comma-separated String to array of Strings
     */
    private static Set<String> toSet(String s) {
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
            ensureSecurityManager();
            processRole = getLogFileName(args);
            logger = getLogger(processRole);

            if (!isSilent) {
                if (logger.isLoggable(Level.INFO)) {
                    logger.info("Starting ServiceGrid [user=" + System.getProperty("user.name") +
                            ", command=\"" + command + "\"]");
                }

                if (logger.isLoggable(Level.FINEST)) {
                    logger.finest("Security policy=" + System.getProperty("java.security.policy"));
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
                if (logger.isLoggable(Level.CONFIG))
                    logger.log(Level.CONFIG, "Creating service " + service + (serviceDescriptor == null ? "" :
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
                            if (logger.isLoggable(Level.WARNING))
                                logger.log(Level.WARNING, "Failed to close service " + customService.toString(), e);
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
            if (logger != null) {
                logger.log(Level.SEVERE, "Error while booting system - ", t);
            } else {
                System.err.println("Error while booting system - " + t);
                t.printStackTrace();
            }
            System.exit(1);
        }
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

        outStream.ignore = true;
        errStream.ignore = true;
        StringBuilder sb = processShutdownHooks();
        outStream.ignore = false;
        errStream.ignore = false;
        System.out.println("Exiting... \n" + sb);
        System.out.println("gsa-exit-done");
        System.out.flush();
        try {
            Thread.sleep(20);
        } catch (InterruptedException e) {
            // graceful sleep
        }
        System.exit(0);
    }

    private static StringBuilder processShutdownHooks() {
        final StringBuilder sb = new StringBuilder();
        ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
        sb.append("Started shutdown at: ").append(new Date()).append("\n");
        synchronized (shutdownHooks) {
            sb.append("Calling [").append(shutdownHooks.size()).append("] ShutdownHooks...").append("\n");
            for (Thread shutdownHook : shutdownHooks) {
                Future<?> future = null;
                try {
                    sb.append("> ShutdownHook called for: ").append(shutdownHook.getName()).append("\n");
                    future = singleThreadExecutor.submit(shutdownHook);
                    future.get(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    if (future != null) {
                        future.cancel(true);
                    }
                    sb.append("> ShutdownHook.run() reported exception: ").append(e)
                            .append("\n")
                            .append(BootUtil.getStackTrace(e)).append("\n");
                    // ignore
                }
            }
            shutdownHooks.clear();
        }
        singleThreadExecutor.shutdownNow();
        sb.append("Completed shutdown at: ").append(new Date()).append("\n");
        return sb;
    }

    private static void waitForStopCommand(Thread mainThread, SystemConfig systemConfig) throws InterruptedException {
        // Loop waiting for a connection and a valid command
        while (!mainThread.isInterrupted()) {
            File workLocation = new File(System.getProperty("com.gs.work", systemConfig.getHomeDir() + "/work"));
            File file = new File(workLocation, "/gsa/gsa-" + AgentHelper.getGSAServiceID() + "-" + AgentHelper.getAgentId() + "-stop");
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
        logger.fine("GC collection time warning set to [" + gcCollectionWarning + "ms]");
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
                        logger.warning("Long GC collection occurred, took [" + collectionTime + "ms], breached threshold [" + gcCollectionWarning + "]");
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
            if (logger.isLoggable(Level.INFO)) {
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
                if (logger.isLoggable(Level.FINEST))
                    logger.log(Level.FINEST, "Unable to create the MBeanServer", e);
                else
                    logger.log(Level.WARNING, "Unable to create the MBeanServer");
            }
        } else {
            if (System.getProperty(CommonSystemProperties.JMX_ENABLED_PROP) == null) {
                if (logger.isLoggable(Level.INFO)) {
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
        return Logger.getLogger(COMPONENT);
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
            if (logger.isLoggable(Level.WARNING)) {
                logger.log(Level.WARNING, "Failed to set sun.rmi.dgc.xxx system properties. \n", secExc);
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
            stream.close();
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
            boolean gsaIsOut = false;
            if (file != null && file.exists()) {
                gsaIsOut = isGsaOut( file );
            } else {
                gsaIsOut = true;
            }
            if (gsaIsOut) {
                // no GSA, print on a different thread (so we won't lock when writing to console)
                // and wait a bit for the output
                Thread t = new Thread(new Runnable() {
                    public void run() {
                        logger.info("GSA parent missing, exiting");
                    }
                });
                t.start();
                try {
                    t.join(1000);
                } catch (InterruptedException e) {
                    // ignore
                }
                // replace output stream, so the process won't get stuck when outputting and not reading from it
                outStream.ignore = true;
                errStream.ignore = true;
                System.exit(1);
            }
        }
    }

    private static File findAgentFile(String gsaServiceID) {
        File gsaLocation = new File(SystemInfo.singleton().locations().work(), "gsa");
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
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "rw");
        } catch (Exception e) {
            // gsa is still holding the file
            gsaIsOut = false;
        }
        if (raf != null) {
            FileChannel channel = raf.getChannel();
            try {
                FileLock lock = channel.tryLock();
                if (lock != null) {
                    // if we can get a lock on the file, the GSA was force killed, even *without releasing the lock*
                    // which in theory, should not happen
                    lock.release();
                    gsaIsOut = true;
                }
            } catch (Exception e) {
                gsaIsOut = false;
            } finally {
                try {
                    channel.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }

        return gsaIsOut;
    }
}