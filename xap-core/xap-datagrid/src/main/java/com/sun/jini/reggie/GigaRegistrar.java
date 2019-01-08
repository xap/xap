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
package com.sun.jini.reggie;

import com.gigaspaces.admin.ManagerClusterInfo;
import com.gigaspaces.admin.cli.RuntimeInfo;
import com.gigaspaces.internal.backport.java.util.concurrent.FastConcurrentSkipListMap;
import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.jmx.JMXUtilities;
import com.gigaspaces.internal.jvm.JVMDetails;
import com.gigaspaces.internal.jvm.JVMHelper;
import com.gigaspaces.internal.jvm.JVMInfoProvider;
import com.gigaspaces.internal.jvm.JVMStatistics;
import com.gigaspaces.internal.log.InternalLogHelper;
import com.gigaspaces.internal.log.InternalLogProvider;
import com.gigaspaces.internal.os.OSDetails;
import com.gigaspaces.internal.os.OSHelper;
import com.gigaspaces.internal.os.OSInfoProvider;
import com.gigaspaces.internal.os.OSStatistics;
import com.gigaspaces.log.LogEntries;
import com.gigaspaces.log.LogEntryMatcher;
import com.gigaspaces.log.LogProcessType;
import com.gigaspaces.logger.LogLevel;
import com.gigaspaces.logger.LogUtils;
import com.gigaspaces.lrmi.ILRMIProxy;
import com.gigaspaces.lrmi.LRMIMonitoringDetails;
import com.gigaspaces.lrmi.nio.info.NIODetails;
import com.gigaspaces.lrmi.nio.info.NIOInfoHelper;
import com.gigaspaces.lrmi.nio.info.NIOInfoProvider;
import com.gigaspaces.lrmi.nio.info.NIOStatistics;
import com.gigaspaces.management.entry.JMXConnection;
import com.gigaspaces.metrics.Gauge;
import com.gigaspaces.metrics.LongCounter;
import com.gigaspaces.metrics.MetricManager;
import com.gigaspaces.metrics.MetricRegistrator;
import com.gigaspaces.metrics.MetricUtils;
import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.service.Service;
import com.j_spaces.kernel.SizeConcurrentHashMap;
import com.j_spaces.kernel.SystemProperties;
import com.j_spaces.kernel.threadpool.DynamicExecutors;
import com.j_spaces.kernel.threadpool.DynamicThreadPoolExecutor;
import com.sun.jini.config.Config;
import com.sun.jini.constants.ThrowableConstants;
import com.sun.jini.discovery.ClientSubjectChecker;
import com.sun.jini.discovery.Discovery;
import com.sun.jini.discovery.DiscoveryConstraints;
import com.sun.jini.discovery.DiscoveryProtocolException;
import com.sun.jini.discovery.EncodeIterator;
import com.sun.jini.discovery.MulticastAnnouncement;
import com.sun.jini.discovery.MulticastRequest;
import com.sun.jini.discovery.UnicastResponse;
import com.sun.jini.lookup.entry.BasicServiceType;
import com.sun.jini.proxy.MarshalledWrapper;
import com.sun.jini.start.LifeCycle;
import com.sun.jini.thread.InterruptedStatusThread;
import com.sun.jini.thread.ReadersWriter;
import com.sun.jini.thread.ReadersWriter.ConcurrentLockException;
import com.sun.jini.thread.ReadyState;
import com.sun.jini.thread.TaskManager;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;
import net.jini.config.NoSuchEntryException;
import net.jini.constraint.BasicMethodConstraints;
import net.jini.core.constraint.InvocationConstraints;
import net.jini.core.constraint.MethodConstraints;
import net.jini.core.constraint.RemoteMethodControl;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.entry.Entry;
import net.jini.core.event.RemoteEventListener;
import net.jini.core.lease.Lease;
import net.jini.core.lease.UnknownLeaseException;
import net.jini.core.lookup.RegistrarEventRegistration;
import net.jini.core.lookup.ServiceDetails;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.core.lookup.ServiceRegistration;
import net.jini.discovery.*;
import net.jini.discovery.dynamic.DynamicLookupLocatorDiscovery;
import net.jini.export.Exporter;
import net.jini.export.ProxyAccessor;
import net.jini.id.ReferentUuid;
import net.jini.id.Uuid;
import net.jini.id.UuidFactory;
import net.jini.io.MarshalOutputStream;
import net.jini.io.MarshalledInstance;
import net.jini.io.OptimizedByteArrayOutputStream;
import net.jini.io.UnsupportedConstraintException;
import net.jini.lookup.JoinManager;
import net.jini.lookup.entry.ServiceInfo;
import net.jini.security.BasicProxyPreparer;
import net.jini.security.ProxyPreparer;
import net.jini.security.TrustVerifier;
import net.jini.security.proxytrust.ServerProxyTrust;

import org.jini.rio.boot.BootUtil;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.rmi.MarshalException;
import java.rmi.MarshalledObject;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.activation.ActivationException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * Base server-side implementation of a lookup service, subclassed by TransientRegistrarImpl and
 * PersistentRegistrarImpl.  Multiple client-side proxy classes are used, for the ServiceRegistrar
 * interface as well as for leases and administration; their methods transform the parameters and
 * then make corresponding calls on the Registrar interface implemented on the server side.
 *
 * @author Sun Microsystems, Inc.
 */
@com.gigaspaces.api.InternalApi
public class GigaRegistrar implements Registrar, ProxyAccessor, ServerProxyTrust, NIOInfoProvider, OSInfoProvider, JVMInfoProvider, InternalLogProvider {

    /**
     * Maximum minMax lease duration for both services and events
     */
    private static final long MAX_LEASE = 1000L * 60 * 60 * 24 * 365 * 1000;
    /**
     * Maximum minimum renewal interval
     */
    private static final long MAX_RENEW = 1000L * 60 * 60 * 24 * 365;
    /**
     * Default maximum size of multicast packets to send and receive
     */
    private static final int DEFAULT_MAX_PACKET_SIZE = 512;
    /**
     * Default timeout to set on sockets used for unicast discovery
     */
    private static final int DEFAULT_SOCKET_TIMEOUT = 60 * 1000;
    /**
     * Logger and configuration component name
     */
    private static final String COMPONENT = "com.sun.jini.reggie";
    /**
     * Lease ID always assigned to self
     */
    private static final Uuid myLeaseID = UuidFactory.create(0L, 0L);

    private static final Logger logger = LoggerFactory.getLogger(GigaRegistrar.class.getName());
    private static final Logger loggerCache = LoggerFactory.getLogger(logger.getName() + ".cache");
    private static final Logger loggerStats = LoggerFactory.getLogger(logger.getName() + ".stats");

    private static final Logger loggerServiceExpire = LoggerFactory.getLogger(COMPONENT + ".expire.service");
    private static final Logger loggerEventExpire = LoggerFactory.getLogger(COMPONENT + ".expire.event");
    private static final Object _lock = new Object();
    private static boolean isActive;

    /**
     * Base set of initial attributes for self
     */
    private static final Entry[] baseAttrs = {
            new ServiceInfo(
                    "Lookup", "Sun Microsystems, Inc.", "Sun Microsystems, Inc.",
                    "2.1", "", ""),
            new BasicServiceType("Lookup")
    };
    /**
     * Empty attribute set
     */
    private static final EntryRep[] emptyAttrs = {};

    /**
     * Proxy for myself
     */
    private RegistrarProxy proxy;

    // cached marshalled object of the proxy
    private MarshalledObject proxyMarshalledObject;

    private byte[] proxyRegistrarBytes;

    /**
     * Exporter for myself
     */
    private Exporter serverExporter;
    /**
     * Remote reference for myself
     */
    private Registrar myRef;
    /**
     * Our service ID
     */
    private ServiceID myServiceID;
    /**
     * Our LookupLocator
     */
    private volatile LookupLocator myLocator;
    /**
     * Our login context, for logging out
     */
    private LoginContext loginContext;
    /**
     * Shutdown callback object, or null if no callback needed
     */
    private LifeCycle lifeCycle;

    /**
     * Map from ServiceID to SvcReg.  Every service is in this map under its serviceID.
     */
    private final Map<ServiceID, SvcReg> serviceByID = new ConcurrentHashMap<ServiceID, SvcReg>(80);

    private final Gauge<Integer> serviceByIdGauge = MetricUtils.sizeGauge(serviceByID);
    /**
     * Identity map from SvcReg to SvcReg, ordered by lease expiration. Every service is in this
     * map.
     */
    private final ConcurrentNavigableMap<SvcRegExpirationKey, SvcReg> serviceByTime =
            new FastConcurrentSkipListMap<SvcRegExpirationKey, SvcReg>();

    private final Gauge<Integer> serviceByTimeGauge = MetricUtils.sizeGauge(serviceByTime);
    /**
     * Map from String to HashMap mapping ServiceID to SvcReg.  Every service is in this map under
     * its types.
     */
    private final HashMap<String, Map> serviceByTypeName = new HashMap<String, Map>(80);
    @SuppressWarnings("unchecked")
    private final Gauge<Integer> serviceByTypeNameGauge = MetricUtils.sizeGauge(serviceByTypeName);
    /**
     * Map from EntryClass to HashMap[] where each HashMap is a map from Object (field value) to
     * ArrayList(SvcReg).  The HashMap array has as many elements as the EntryClass has fields
     * (including fields defined by superclasses).  Services are in this map multiple times, once
     * for each field of each entry it has.  The outer map is indexed by the first (highest)
     * superclass that defines the field.  This means that a HashMap[] has null elements for fields
     * defined by superclasses, but this is a small memory hit and is simpler than subtracting off
     * base index values when accessing the arrays.
     */
    private final HashMap<EntryClass, HashMap[]> serviceByAttr = new HashMap<EntryClass, HashMap[]>(200);
    private final Gauge<Integer> serviceByAttrGauge = MetricUtils.sizeGauge(serviceByAttr);

    /**
     * Map from EntryClass to ArrayList(SvcReg).  Services are in this map multiple times, once for
     * each no-fields entry it has (no fields meaning none of the superclasses have fields either).
     * The map is indexed by the exact type of the entry.
     */
    private final HashMap<EntryClass, ArrayList> serviceByEmptyAttr = new HashMap<EntryClass, ArrayList>(50);
    private final Gauge<Integer> serviceByEmptyAttrGauge = MetricUtils.sizeGauge(serviceByEmptyAttr);
    /**
     * All EntryClasses with non-zero numInstances
     */
    private final ArrayList<EntryClass> entryClasses = new ArrayList<EntryClass>();
    private final Gauge<Integer> entryClassesGauge = MetricUtils.sizeGauge(entryClasses);

    /**
     * Map from Long(eventID) to EventReg.  Every event registration is in this map under its
     * eventID.
     */
    private final Map<Long, EventReg> eventByID = new ConcurrentHashMap<Long, EventReg>(100);
    private final Gauge<Integer> eventByIDGauge = MetricUtils.sizeGauge(eventByID);
    /**
     * Identity map from EventReg to EventReg, ordered by lease expiration. Every event registration
     * is in this map.
     */
    private final ConcurrentNavigableMap eventByTime = new FastConcurrentSkipListMap();
    @SuppressWarnings("unchecked")
    private final Gauge<Integer> eventByTimeGauge = MetricUtils.sizeGauge(eventByTime);
    /**
     * Map from ServiceID to EventReg or EventReg[].  An event registration is in this map if its
     * template matches on (at least) a specific serviceID.
     */
    private final Map<ServiceID, Object> subEventByService = new ConcurrentHashMap<ServiceID, Object>(100);
    private final Gauge<Integer> subEventByServiceGauge = MetricUtils.sizeGauge(subEventByService);
    /**
     * Map from Long(eventID) to EventReg.  An event registration is in this map if its template
     * matches on ANY_SERVICE_ID.
     */
    private final Map<Long, EventReg> subEventByID = new ConcurrentHashMap<Long, EventReg>(100);
    private final Gauge<Integer> subEventByIDGauge = MetricUtils.sizeGauge(subEventByID);

    /**
     * A local service cache that holds a cache of an Item per template.
     */
    private final ConcurrentHashMap<Template, ConcurrentHashMap<ServiceID, Item>> lookupServiceCache = new ConcurrentHashMap<Template, ConcurrentHashMap<ServiceID, Item>>(500);
    private final Gauge<Integer> lookupServiceCacheGauge = MetricUtils.sizeGauge(lookupServiceCache); //todo make show internal size as well.

    /**
     * A flag indicating if the cache will be an indication that a Space service does not exists
     * within reggie.
     */
    private boolean useCacheForSpaceMiss = true;

    /**
     * Generator for resource (e.g., registration, lease) Uuids
     */
    private UuidGenerator resourceIdGenerator = new UuidGenerator();
    /**
     * Generator for service IDs
     */
    private UuidGenerator serviceIdGenerator = resourceIdGenerator;
    /**
     * Event ID
     */
    private long eventID = 0;
    /**
     * Random number generator for use in lookup
     */
    private final Random random = new Random();

    /**
     * Preparer for received remote event listeners
     */
    private ProxyPreparer listenerPreparer = new BasicProxyPreparer();
    /**
     * Preparer for received lookup locators
     */
    private ProxyPreparer locatorPreparer = listenerPreparer;

    /**
     * Maximum queued events per EventReg record.
     * When reached, the registration is canceled and all events for that target are discarded.
     * @since 12.3
     */
    private int maxEventsPerEventReg;

    /**
     * Holds events before they are moved to the eventsQ for processing by the TaskManager
     */
    private Map<EventReg, List<RegistrarEvent>> pendingQ;
    /**
     * Holds events to be processed by the TaskManager
     */
    private Map<EventReg, List<RegistrarEvent>> eventsQ;

    /**
     * Current maximum service lease duration granted, in milliseconds.
     */
    private volatile long maxServiceLease;
    /**
     * Current maximum event lease duration granted, in milliseconds.
     */
    private volatile long maxEventLease;

    /**
     * Manager for discovering other lookup services
     */
    private DiscoveryManagement discoer;
    /**
     * Manager for joining other lookup services
     */
    private JoinManager joiner;
    /**
     * Task manager for sending events and discovery responses
     * note: since 12.3 this is a single reference, not an array
     */
    private TaskManager taskerEvent;

    private ExecutorService taskerComm;

    /**
     * Service lease expiration thread
     */
    private Thread serviceExpirer;
    /**
     * Event lease expiration thread
     */
    private Thread eventExpirer;
    /**
     * Unicast discovery request packet receiving thread
     */
    private UnicastThread unicaster;
    /**
     * Multicast discovery request packet receiving thread
     */
    private Thread multicaster;
    /**
     * Multicast discovery announcement sending thread
     */
    private Thread announcer;

    /**
     * Concurrent object to control read and write access
     * NOTE - This is not a Reentrant Read/Write Lock.
     * This means you can't hold the get the lock twice from the same thread!
     */
    private final ReadersWriter concurrentObj = new ReadersWriter();

    /**
     * Canonical ServiceType for java.lang.Object
     */
    private ServiceType objectServiceType;

    /**
     * Flag indicating whether system state was recovered from a snapshot
     */
    private final boolean recoveredSnapshot = false;

    /**
     * Minimum value for maxServiceLease.
     */
    private long minMaxServiceLease = 1000 * 60;
    /**
     * Minimum value for maxEventLease.
     */
    private long minMaxEventLease = 1000 * 60 * 30;
    /**
     * Minimum average time between lease renewals, in milliseconds.
     */
    private long minRenewalInterval = 100;
    /**
     * Port for unicast discovery
     */
    private int unicastPort = 0;

    private final ReadWriteLock unicastPortRWL = new ReentrantReadWriteLock();

    /**
     * The groups we are a member of
     */
    private volatile String[] memberGroups = BootUtil.toArray(SystemInfo.singleton().lookup().groups());

    private final ReadWriteLock memberGroupRWL = new ReentrantReadWriteLock();

    /**
     * The groups we should join
     */
    private String[] lookupGroups = LookupGroups.none();

    private final ReadWriteLock lookupGroupsRWL = new ReentrantReadWriteLock();

    /**
     * The locators of other lookups we should join
     */
    private LookupLocator[] lookupLocators = BootUtil.toLookupLocators(SystemInfo.singleton().lookup().locators());

    private final ReadWriteLock lookupLocatorsRWL = new ReentrantReadWriteLock();

    /**
     * The attributes to use when joining (including with myself)
     */
    private Entry[] lookupAttrs;
    /**
     * Interval to wait in between sending multicast announcements
     */
    private long multicastAnnouncementInterval = 1000 * 60 * 2;
    /**
     * Multicast announcement sequence number
     */
    private volatile long announcementSeqNo = 0L;

    /**
     * Network interfaces to use for multicast discovery
     */
    private NetworkInterface[] multicastInterfaces;
    /**
     * Flag indicating whether network interfaces were explicitly specified
     */
    private boolean multicastInterfacesSpecified;
    /**
     * Interval to wait in between retrying failed interfaces
     */
    private int multicastInterfaceRetryInterval = 1000 * 60 * 5;
    /**
     * Utility for participating in version 2 of discovery protocols
     */
    private Discovery protocol2;
    /**
     * Cached raw constraints associated with unicastDiscovery method
     */
    private InvocationConstraints rawUnicastDiscoveryConstraints;
    /**
     * Constraints specified for incoming multicast requests
     */
    private DiscoveryConstraints multicastRequestConstraints;
    /**
     * Constraints specified for outgoing multicast announcements
     */
    private DiscoveryConstraints multicastAnnouncementConstraints;
    /**
     * Constraints specified for handling unicast discovery
     */
    private DiscoveryConstraints unicastDiscoveryConstraints;
    /**
     * Client subject checker to apply to incoming multicast requests
     */
    private ClientSubjectChecker multicastRequestSubjectChecker;
    /**
     * Maximum time to wait for calls to finish before forcing unexport
     */
    private volatile long unexportTimeout = 1000 * 60 * 2;
    /**
     * Time to wait between unexport attempts
     */
    private volatile long unexportWait = 1000;
    /**
     * Client subject checker to apply to unicast discovery attempts
     */
    private ClientSubjectChecker unicastDiscoverySubjectChecker;

    /**
     * Lock protecting startup and shutdown
     */
    private final ReadyState ready = new ReadyState();

    private final LongCounter items = new LongCounter();
    private final LongCounter listeners = new LongCounter();
    private final LongCounter pendingEvents = new LongCounter();
    private MetricManager metricManager;
    private MetricRegistrator metricRegistrator;

    private final InetAddress host;

    public static boolean isActive() {
        synchronized (_lock) {
            return isActive;
        }
    }

    private static void activate() {
        synchronized (_lock) {
            if (isActive)
                throw new UnsupportedOperationException("Another Lookup Service is already active");
            isActive = true;
        }
    }

    private static void deactivate() {
        synchronized (_lock) {
            isActive = false;
        }
    }

    /**
     * Constructs RegistrarImpl based on a configuration obtained using the provided string
     * arguments.  If activationID is non-null, the created RegistrarImpl runs as activatable; if
     * persistent is true, it persists/recovers its state to/from disk.  A RegistrarImpl instance
     * cannot be constructed as both activatable and non-persistent.  If lifeCycle is non-null, its
     * unregister method is invoked during shutdown.
     */
    public GigaRegistrar(String[] configArgs, final LifeCycle lifeCycle) throws Exception {
        if (System.getProperty(SystemProperties.LUS_FILE_PATH) != null) {
            throw new IllegalStateException("LUS should not be started when file-lus is configured");
        }
        try {
            //
            activate();
            this.metricManager = MetricManager.acquire();
            this.metricRegistrator = metricManager.createRegistrator("lus");
            final Configuration config = ConfigurationProvider.getInstance(
                    configArgs, getClass().getClassLoader());
            loginContext = (LoginContext) config.getEntry(
                    COMPONENT, "loginContext", LoginContext.class, null);

            this.host = SystemInfo.singleton().network().getHost();
            PrivilegedExceptionAction init = new PrivilegedExceptionAction() {
                public Object run() throws Exception {
                    init(config, lifeCycle);
                    return null;
                }
            };
            if (loginContext != null) {
                loginContext.login();
                try {
                    Subject.doAsPrivileged(
                            loginContext.getSubject(), init, null);
                } catch (PrivilegedActionException e) {
                    throw e.getCause();
                }
            } else {
                init.run();
            }
            registerMetrics(metricRegistrator);

        } catch (Throwable t) {
            logger.error("Reggie initialization failed", t);
            if (t instanceof Exception) {
                throw (Exception) t;
            }
            throw (Error) t;
        }
    }

    private void registerMetrics(MetricRegistrator registrator) {
        registrator.register("listeners", listeners);
        registrator.register("pendingEvents", pendingEvents);
        registrator.register("items", items);
        registrator.register("serviceById", serviceByIdGauge);
        registrator.register("serviceByTime", serviceByTimeGauge);
        registrator.register("serviceByTypeName", serviceByTypeNameGauge);
        registrator.register("serviceByAttr", serviceByAttrGauge);
        registrator.register("serviceByEmptyAttr", serviceByEmptyAttrGauge);
        registrator.register("entryClasses", entryClassesGauge);
        registrator.register("eventByID", eventByIDGauge);
        registrator.register("eventByTime", eventByTimeGauge);
        registrator.register("subEventByService", subEventByServiceGauge);
        registrator.register("subEventByID", subEventByIDGauge);
        registrator.register("lookupServiceCache", lookupServiceCacheGauge);

        MetricManager.registerThreadPoolMetrics(registrator.extend("comm-task-pool"), (DynamicThreadPoolExecutor) taskerComm);
        MetricManager.registerTaskManagerMetrics(registrator.extend("event-task-pool"), taskerEvent);
    }

    /**
     * Used to write the internal state to a file.
     * @see com.gigaspaces.internal.dump.InternalDumpProvider
     * @see com.gigaspaces.internal.dump.InternalDumpProcessor
     */
    public void dumpEventQueueState(PrintWriter writer) {
        try {
            concurrentObj.readLock();
        } catch (ConcurrentLockException e) {
            return;
        }
        try {
            boolean hasElements = false;
            writer.append("\nEvents Queue:");
            writer.append("\n- registered listeners: " + listeners.getCount());
            writer.append("\n- event listeners: " + eventsQ.size());
            writer.append("\n- listeners with pending events:");

            int max = 0;
            double avg = 0.0;
            for (Map.Entry<EventReg, List<RegistrarEvent>> entry : eventsQ.entrySet()) {
                int size = entry.getValue().size();
                if (size == 0) continue;
                writer.append("\n--- " + entry.getKey() + "\t has (" + size + ") pending events");
                hasElements = true;
                max = Math.max(max, size);
                avg += size;
            }
            if (hasElements) {
                writer.append("\n--- max=("+max + "), avg=("+String.format( "%.2f", (avg/eventsQ.size()))+") pending events");
            } else {
                writer.append(" 0");
            }

            writer.println();

            writer.append("\nTask Manager:");
            writer.append("\n- threads: " + taskerEvent.getThreadCount()
                    + ", max: "+taskerEvent.getMaxThreads()
                    + ", running: " + taskerEvent.getRunningThreadCount());
            writer.append("\n- total tasks: " + taskerEvent.getTotalTasks());
            List<TaskManager.Task> pending = taskerEvent.getPending();
            if (!pending.isEmpty()) {
                writer.append("\n- tasks pending execution:");
                for (TaskManager.Task task : pending) {
                    EventReg reg = ((EventTask) task).reg;
                    writer.append("\n--- " + reg);
                }
            }

            writer.println();

        } finally {
            concurrentObj.readUnlock();
        }
    }

    @Override
    public ManagerClusterInfo getManagerClusterInfo() {
        return SystemInfo.singleton().getManagerClusterInfo();
    }

    private final static class SvcRegExpirationKey implements Comparable {

        public final SvcReg svcReg;

        public final long leaseExpiration;

        private SvcRegExpirationKey(SvcReg svcReg, long leaseExpiration) {
            this.svcReg = svcReg;
            this.leaseExpiration = leaseExpiration;
        }

        /**
         * Primary sort by leaseExpiration, secondary by leaseID.  The secondary sort is immaterial,
         * except to ensure a total order (required by TreeMap).
         */
        public int compareTo(Object obj) {
            SvcRegExpirationKey reg = (SvcRegExpirationKey) obj;
            if (this == reg)
                return 0;
            int i = compare(leaseExpiration, reg.leaseExpiration);
            if (i != 0) {
                return i;
            }
            i = compare(svcReg.leaseID.getMostSignificantBits(),
                    reg.svcReg.leaseID.getMostSignificantBits());
            if (i != 0) {
                return i;
            }
            return compare(svcReg.leaseID.getLeastSignificantBits(),
                    reg.svcReg.leaseID.getLeastSignificantBits());
        }

        /**
         * Compares long values, returning -1, 0, or 1 if l1 is less than, equal to or greater than
         * l2, respectively.
         */
        private static int compare(long l1, long l2) {
            return (l1 < l2) ? -1 : ((l1 > l2) ? 1 : 0);
        }
    }

    /**
     * A service item registration record.
     */
    private final static class SvcReg implements Comparable, Serializable {

        private static final long serialVersionUID = 2L;

        /**
         * The service item.
         *
         * @serial
         */
        public final Item item;
        /**
         * The lease id.
         *
         * @serial
         */
        public final Uuid leaseID;

        /**
         * The registration time of the service.
         */
        public final long registrationTime;

        /**
         * The lease expiration time.
         *
         * @serial
         */
        public volatile long leaseExpiration;

        /**
         * Simple constructor
         */
        public SvcReg(Item item, Uuid leaseID, long registrationTime, long leaseExpiration) {
            this.item = item;
            this.leaseID = leaseID;
            this.registrationTime = registrationTime;
            this.leaseExpiration = leaseExpiration;
        }

        /**
         * Primary sort by leaseExpiration, secondary by leaseID.  The secondary sort is immaterial,
         * except to ensure a total order (required by TreeMap).
         */
        public int compareTo(Object obj) {
            if (obj == null) {
                return -1;
            }
            SvcReg reg = (SvcReg) obj;
            if (this == reg)
                return 0;
            int i = compare(leaseExpiration, reg.leaseExpiration);
            if (i != 0) {
                return i;
            }
            i = compare(leaseID.getMostSignificantBits(),
                    reg.leaseID.getMostSignificantBits());
            if (i != 0) {
                return i;
            }
            return compare(leaseID.getLeastSignificantBits(),
                    reg.leaseID.getLeastSignificantBits());
        }

        /**
         * Compares long values, returning -1, 0, or 1 if l1 is less than, equal to or greater than
         * l2, respectively.
         */
        private static int compare(long l1, long l2) {
            return (l1 < l2) ? -1 : ((l1 > l2) ? 1 : 0);
        }
    }

    private final static class EventRegKeyExpiration implements Comparable<EventRegKeyExpiration> {

        public final EventReg reg;

        public final long leaseExpiration;


        private EventRegKeyExpiration(EventReg reg, long leaseExpiration) {
            this.reg = reg;
            this.leaseExpiration = leaseExpiration;
        }

        public int compareTo(EventRegKeyExpiration reg) {
            if (this == reg)
                return 0;
            int delta = (int) (leaseExpiration - reg.leaseExpiration);
            if (delta != 0) {
                return delta;
            }
            return (int) (this.reg.eventID - reg.reg.eventID);
        }
    }


    /**
     * An event registration record.
     */
    private final static class EventReg implements Comparable<EventReg>, Serializable {

        private static final long serialVersionUID = 4L;

        /**
         * The event id.
         *
         * @serial
         */
        public final long eventID;
        /**
         * The lease id.
         *
         * @serial
         */
        public final Uuid leaseID;
        /**
         * The template to match.
         *
         * @serial
         */
        public final Template tmpl;
        /**
         * The transitions.
         *
         * @serial
         */
        public final int transitions;
        /**
         * The current sequence number.
         *
         * @serial
         */
        public long seqNo;
        /**
         * The event listener.
         */
        public transient RemoteEventListener listener;
        /**
         * The handback object.
         *
         * @serial
         */
        public final MarshalledObject handback;
        /**
         * The lease expiration time.
         *
         * @serial
         */
        public volatile long leaseExpiration;

        private transient AtomicBoolean locked = new AtomicBoolean(false);

        /**
         * Simple constructor
         */
        EventReg(long eventID, Uuid leaseID, Template tmpl,
                 int transitions, RemoteEventListener listener,
                 MarshalledObject handback, long leaseExpiration) {
            this.eventID = eventID;
            this.leaseID = leaseID;
            this.tmpl = tmpl;
            this.transitions = transitions;
            this.seqNo = 0;
            this.listener = listener;
            this.handback = handback;
            this.leaseExpiration = leaseExpiration;
        }

        /**
         * Primary sort by leaseExpiration, secondary by eventID.  The secondary sort is immaterial,
         * except to ensure a total order (required by TreeMap).
         */
        public int compareTo(EventReg reg) {
            if (this == reg)
                return 0;
            if (leaseExpiration < reg.leaseExpiration ||
                    (leaseExpiration == reg.leaseExpiration &&
                            eventID < reg.eventID))
                return -1;
            return 1;
        }

        @Override
        public int hashCode() {
            return leaseID.hashCode() + (int) (eventID ^ (eventID >>> 32)); //hashcode of long eventID
        }

        /**
         * @return true if EventTask has already been added for this EventReg.
         */
        public boolean isLocked() {
            return locked.get();
        }

        /**
         * @return true if succeeded to get a lock for this EventReg, indicating an EventTask can be added for processing.
         */
        public boolean acquireLock() {
            return locked.compareAndSet(false, true);
        }

        /**
         * Release the lock, allowing another EventTask to be added for this EventReg.
         */
        public void releaseLock() {
            locked.set(false);
        }

        @Override
        public String toString() {
            return listenerToString(listener)+" - reg.#"+eventID;
        }

        private String listenerToString(RemoteEventListener r) {
            //ip:port/pid[87581]/
            String ref = r.toString();
            int start = ref.indexOf("//");
            if (start != -1) {
                int end = ref.indexOf("]/", start);
                return ref.substring(start+2, end+2);
            }
            return ref;
        }

        /**
         * @serialData RemoteEventListener as a MarshalledInstance
         */
        private void writeObject(ObjectOutputStream stream)
                throws IOException {
            stream.defaultWriteObject();
            stream.writeObject(new MarshalledInstance(listener));
        }

        /**
         * Unmarshals the event listener.
         */
        private void readObject(ObjectInputStream stream)
                throws IOException, ClassNotFoundException {
            stream.defaultReadObject();
            MarshalledInstance mi = (MarshalledInstance) stream.readObject();
            try {
                listener = (RemoteEventListener) mi.get(false);
            } catch (Throwable e) {
                if (e instanceof Error &&
                        ThrowableConstants.retryable(e) ==
                                ThrowableConstants.BAD_OBJECT) {
                    throw (Error) e;
                }
                logger.warn(
                        "failed to recover event listener", e);
            }
        }
    }

    /**
     * Base class for iterating over all Items that match a Template.
     */
    private abstract class ItemIter {
        /**
         * Current time
         */
        public final long now = SystemTime.timeMillis();
        /**
         * True means duplicate items are possible
         */
        public boolean dupsPossible = false;
        /**
         * Template to match
         */
        protected final Template tmpl;
        /**
         * Next item to return
         */
        protected SvcReg reg;

        /**
         * Subclass constructors must initialize reg
         */
        protected ItemIter(Template tmpl) {
            this.tmpl = tmpl;
        }

        /**
         * Returns true if the iteration has more elements.
         */
        public boolean hasNext() {
            return reg != null;
        }

        /**
         * Returns the next element in the iteration as an Item.
         */
        public Item next() {
            if (reg == null)
                throw new NoSuchElementException();
            Item item = reg.item;
            step();
            return item;
        }

        /**
         * Returns the next element in the iteration as a SvcReg.
         */
        public SvcReg nextReg() {
            if (reg == null)
                throw new NoSuchElementException();
            SvcReg cur = reg;
            step();
            return cur;
        }

        /**
         * Set reg to the next matching element, or null if none
         */
        protected abstract void step();

        public int suggestedSize() {
            return 10;
        }
    }

    /**
     * Iterate over all Items.
     */
    private class AllItemIter extends ItemIter {
        /**
         * Iterator over serviceByID
         */
        private final Iterator<SvcReg> iter;

        /**
         * Assumes the empty template
         */
        AllItemIter() {
            super(null);
            iter = serviceByID.values().iterator();
            step();
        }

        /**
         * Set reg to the next matching element, or null if none
         */
        @Override
        protected void step() {
            while (iter.hasNext()) {
                reg = iter.next();
                if (reg.leaseExpiration > now)
                    return;
            }
            reg = null;
        }

        @Override
        public int suggestedSize() {
            return serviceByID.size();
        }
    }

    /**
     * Iterates over all services that match template's service types
     */
    private class SvcIterator extends ItemIter {
        /**
         * Iterator for list of matching services.
         */
        private final Iterator services;

        /**
         * tmpl.serviceID == null and tmpl.serviceTypes is not empty
         */
        SvcIterator(Template tmpl) {
            super(tmpl);
            Map map = serviceByTypeName.get(
                    tmpl.serviceTypes[0].getName());
            services = map != null ? map.values().iterator() :
                    Collections.EMPTY_LIST.iterator();
            step();
        }

        /**
         * Set reg to the next matching element, or null if none.
         */
        @Override
        protected void step() {
            if (tmpl.serviceTypes.length > 1) {
                while (services.hasNext()) {
                    reg = (SvcReg) services.next();
                    if (reg.leaseExpiration > now &&
                            matchType(tmpl.serviceTypes, reg.item.serviceType) &&
                            matchAttributes(tmpl, reg.item))
                        return;
                }
            } else {
                while (services.hasNext()) {
                    reg = (SvcReg) services.next();
                    if (reg.leaseExpiration > now &&
                            matchAttributes(tmpl, reg.item))
                        return;
                }
            }
            reg = null;
        }
    }

    /**
     * Iterate over all matching Items by attribute value.
     */
    private class AttrItemIter extends ItemIter {
        /**
         * SvcRegs obtained from serviceByAttr for chosen attr
         */
        protected ArrayList svcs;
        /**
         * Current index into svcs
         */
        protected int svcidx;

        /**
         * tmpl.serviceID == null and tmpl.serviceTypes is empty and tmpl.attributeSetTemplates[setidx].fields[fldidx]
         * != null
         */
        AttrItemIter(Template tmpl, int setidx, int fldidx) {
            super(tmpl);
            EntryRep set = tmpl.attributeSetTemplates[setidx];
            HashMap[] attrMaps =
                    serviceByAttr.get(getDefiningClass(set.eclass,
                            fldidx));
            if (attrMaps != null && attrMaps[fldidx] != null) {
                svcs = (ArrayList) attrMaps[fldidx].get(set.fields[fldidx]);
                if (svcs != null) {
                    svcidx = svcs.size();
                    step();
                }
            }
        }

        /**
         * Simple constructor
         */
        AttrItemIter(Template tmpl) {
            super(tmpl);
        }

        /**
         * Set reg to the next matching element, or null if none.
         */
        @Override
        protected void step() {
            while (--svcidx >= 0) {
                reg = (SvcReg) svcs.get(svcidx);
                if (reg.leaseExpiration > now &&
                        matchAttributes(tmpl, reg.item))
                    return;
            }
            reg = null;
        }
    }

    /**
     * Iterate over all matching Items by no-fields entry class.
     */
    private class EmptyAttrItemIter extends AttrItemIter {

        /**
         * tmpl.serviceID == null and tmpl.serviceTypes is empty and eclass has no fields
         */
        EmptyAttrItemIter(Template tmpl, EntryClass eclass) {
            super(tmpl);
            svcs = serviceByEmptyAttr.get(eclass);
            if (svcs != null) {
                svcidx = svcs.size();
                step();
            }
        }
    }

    /**
     * Iterate over all matching Items by entry class, dups possible.
     */
    private class ClassItemIter extends ItemIter {
        /**
         * Entry class to match on
         */
        private final EntryClass eclass;
        /**
         * Current index into entryClasses
         */
        private int classidx;
        /**
         * Values iterator for current HashMap
         */
        private Iterator iter;
        /**
         * SvcRegs obtained from iter or serviceByEmptyAttr
         */
        private ArrayList svcs;
        /**
         * Current index into svcs
         */
        private int svcidx = 0;

        /**
         * tmpl.serviceID == null and tmpl.serviceTypes is empty and tmpl.attributeSetTemplates is
         * non-empty
         */
        ClassItemIter(Template tmpl) {
            super(tmpl);
            dupsPossible = true;
            eclass = tmpl.attributeSetTemplates[0].eclass;
            classidx = entryClasses.size();
            step();
        }

        /**
         * Set reg to the next matching element, or null if none
         */
        @Override
        protected void step() {
            do {
                while (--svcidx >= 0) {
                    reg = (SvcReg) svcs.get(svcidx);
                    if (reg.leaseExpiration > now &&
                            matchAttributes(tmpl, reg.item))
                        return;
                }
            } while (stepValue());
            reg = null;
        }

        /**
         * Step to the next HashMap value, if any, reset svcs and svcidx, and return false if
         * everything exhausted.
         */
        private boolean stepValue() {
            while (true) {
                if (iter != null && iter.hasNext()) {
                    svcs = (ArrayList) iter.next();
                    svcidx = svcs.size();
                    return true;
                }
                if (!stepClass())
                    return false;
                if (iter == null)
                    return true;
            }
        }

        /**
         * Step to the next matching entry class, if any, reset iter using the HashMap for the last
         * field of the class (and reset (svcs and svcidx if the entry class has no fields), and
         * return false if everything exhausted.
         */
        private boolean stepClass() {
            while (--classidx >= 0) {
                EntryClass cand = entryClasses.get(classidx);
                if (!eclass.isAssignableFrom(cand))
                    continue;
                if (cand.getNumFields() > 0) {
                    cand = getDefiningClass(cand, cand.getNumFields() - 1);
                    HashMap[] attrMaps = serviceByAttr.get(cand);
                    iter = attrMaps[attrMaps.length - 1].values().iterator();
                } else {
                    iter = null;
                    svcs = serviceByEmptyAttr.get(cand);
                    svcidx = svcs.size();
                }
                return true;
            }
            return false;
        }
    }

    /**
     * Iterate over a singleton matching Item by serviceID.
     */
    private class IDItemIter extends ItemIter {

        /**
         * tmpl.serviceID != null
         */
        IDItemIter(Template tmpl) {
            super(tmpl);
            reg = serviceByID.get(tmpl.serviceID);
            if (reg != null &&
                    (reg.leaseExpiration <= now || !matchItem(tmpl, reg.item)))
                reg = null;
        }

        /**
         * Set reg to null
         */
        @Override
        protected void step() {
            reg = null;
        }
    }

    private static final float LOAD_FACTOR = 1.75f;

    /**
     * An event to be sent, and the listener to send it to.
     */
    private final class EventTask implements TaskManager.Task {

        /**
         * The event registration
         */
        public final EventReg reg;

        /**
         * Simple constructor, except increments reg.seqNo.
         */
        EventTask(EventReg reg) {
            this.reg = reg;
        }

        /**
         * Send the event
         */
        public void run() {

            if (logger.isDebugEnabled()) {
                logger.debug("Notifying listener {} of event {}", reg.listener, reg.eventID);
            }

            boolean requeue = false;
            try {
                List<RegistrarEvent> registrarEvents = eventsQ.get(reg);
                if (registrarEvents == null) {
                    return;
                }

                final int limit = calcSizeWithLoadFactor(registrarEvents); //use load-factor as to not cause over re-queueing

                for (int i = 0; i < limit; ++i) {
                    // check here if we really need to send the event
                    if (isEventDeleted() || isProxyClosed()) {
                        return;
                    }

                    if (registrarEvents.isEmpty()) {
                        return;
                    }

                    // check if we EventReg hasn't been deleted while we are processing
                    if (!eventsQ.containsKey(reg)) return;


                    if (exceededMaximumEventsAllowedPerRegistration(registrarEvents)) {
                        return;
                    }

                    final RegistrarEvent theEvent = registrarEvents.remove(0);
                    if (theEvent == null) {
                        return; //due to concurrency
                    }

                    if (!sendEvent(theEvent)) return;

                }

                if (registrarEvents.size() != 0) {
                    requeue = true; //there are remaining events, indicate 'requeue' for next time
                }

            } finally {
                if (requeue) {
                    taskerEvent.add(new EventTask(reg)); //re-queue for remaining, but don't release lock
                } else {
                    reg.releaseLock(); //lock is acquired when the EventTask is created. release it.
                }
            }
        }

        /**
         * Takes the current registrar size and calculates a load factor on it.
         * This is done to avoid over requeue and context-switches.
         * @param registrarEvents the events list
         * @return a size + load factor
         */
        private int calcSizeWithLoadFactor(List<RegistrarEvent> registrarEvents) {
            return (int)((registrarEvents.size()+1)*LOAD_FACTOR);
        }

        /**
         * Check if exceeds the maximum allowed events saved per-target registration.
         * If exceeded, we cancel the event registration and remove all queued events.
         *
         * @param events The list of events pending for this registered target.
         * @return true if exceeded, false otherwise.
         */
        private boolean exceededMaximumEventsAllowedPerRegistration(List<RegistrarEvent> events) {
            final int size = events.size();
            final boolean exceededBounds = size > maxEventsPerEventReg;
            if (exceededBounds) {
                try {
                    logger.warn("Cancelling registered listener: " + reg
                            + " - its events queue size ("+size+") exceeded the maximum of "+ maxEventsPerEventReg +".");

                    cancelEventLease(reg.eventID, reg.leaseID);

                }catch(Exception e) {
                    logger.warn("Failed to cancel registered listener: " + reg, e);
                }
            }
            return exceededBounds;
        }

        /**
         * @param theEvent to send
         * @return true if successful, false otherwise
         */
        private boolean sendEvent(RegistrarEvent theEvent) {

            try {
                pendingEvents.dec();
                reg.listener.notify(theEvent);
                return true;

            } catch (Throwable e) {

                if (isEventDeleted() || isProxyClosed()) {
                    return false;
                }

                switch (ThrowableConstants.retryable(e)) {
                    case ThrowableConstants.BAD_OBJECT:
                        if (e instanceof Error) {
                            logger.debug("Exception sending event to [" + reg.listener + "], serviceID [" + theEvent.getServiceID() + "], eventID [" + reg.eventID + "], " + reg.tmpl, e);
                            throw (Error) e;
                        }
                    case ThrowableConstants.BAD_INVOCATION:
                    case ThrowableConstants.UNCATEGORIZED:
                    /* If the listener throws UnknownEvent or some other
                     * definite exception, we can cancel the lease.
                     */
                        if (logger.isDebugEnabled()) {
                            logger.debug("Exception sending event to [" + reg.listener + "], ServiceID [" + theEvent.getServiceID() + "], eventID [" + reg.eventID + "], " + reg.tmpl + " canceling lease [" + reg.leaseID + "]", e);
                        }
                        try {
                            cancelEventLease(reg.eventID, reg.leaseID);
                        } catch (UnknownLeaseException ee) {
                            logger.debug(
                                    "Exception canceling event lease",
                                    e);
                        } catch (RemoteException ee) {
                            logger.debug(
                                    "The server has been shutdown",
                                    e);
                        }
                }
            }
            return false;
        }

        private boolean isEventDeleted() {
            if (reg.tmpl.serviceID != null) {
                return !subEventByService.containsKey(reg.tmpl.serviceID);
            } else {
                return !subEventByID.containsKey(reg.eventID);
            }
        }

        private boolean isProxyClosed() {
            ILRMIProxy ilrmiProxy = (ILRMIProxy) reg.listener;
            return (ilrmiProxy.isClosed());
        }

        /**
         * @return always return false; no need to prioritize.
         */
        public boolean runAfter(List tasks, int size) {
            return false;
        }
    }

    /**
     * Task for decoding multicast request packets.
     */
    private final class DecodeRequestTask implements Runnable {

        /**
         * The multicast packet to decode
         */
        private final DatagramPacket datagram;
        /**
         * The decoder for parsing the packet
         */
        private final Discovery decoder;

        DecodeRequestTask(DatagramPacket datagram, Discovery decoder) {
            this.datagram = datagram;
            this.decoder = decoder;
        }

        /**
         * Decodes this task's multicast request packet, spawning an AddressTask if the packet
         * satisfies the configured constraints, matches this registrar's groups, and does not
         * already contain this registrar's service ID in its list of known registrars.  This method
         * assumes that the protocol version of the request has already been checked.
         */
        public void run() {
            MulticastRequest req;
            try {
                req = decoder.decodeMulticastRequest(
                        datagram,
                        multicastRequestConstraints.getUnfulfilledConstraints(),
                        multicastRequestSubjectChecker, true);
            } catch (Exception e) {
                if (!(e instanceof InterruptedIOException) &&
                        logger.isDebugEnabled()) {
                    LogUtils.throwing(logger, getClass(), "run", e,
                            "exception decoding multicast request from {}:{}", datagram.getAddress(), datagram.getPort());
                }
                return;
            }
            String[] groups = req.getGroups();
            if ((groups.length == 0 || overlap(memberGroups, groups)) &&
                    indexOf(req.getServiceIDs(), myServiceID) < 0) {
                try {
                    req.checkConstraints();
                } catch (Exception e) {
                    if (!(e instanceof InterruptedIOException) &&
                            logger.isDebugEnabled()) {
                        LogUtils.throwing(logger, getClass(), "run", e,
                                "exception decoding multicast request from {}:{}", datagram.getAddress(), datagram.getPort());
                    }
                    return;
                }
                taskerComm.execute(new AddressTask(req.getHost(), req.getPort()));
            }
        }

        /**
         * No ordering
         */
        public boolean runAfter(List tasks, int size) {
            return false;
        }
    }

    /**
     * Address for unicast discovery response.
     */
    private final class AddressTask implements TaskManager.Task {

        /**
         * The address
         */
        public final String host;
        /**
         * The port
         */
        public final int port;

        /**
         * Simple constructor
         */
        AddressTask(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public int hashCode() {
            return host.hashCode();
        }

        /**
         * Two tasks are equal if they have the same address and port
         */
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof AddressTask))
                return false;
            AddressTask ua = (AddressTask) obj;
            return host.equals(ua.host) && port == ua.port;
        }

        /**
         * Connect and then process a unicast discovery request
         */
        public void run() {
            InetAddress[] addr = new InetAddress[]{};
            try {
                try {
                    InetAddress[] addrByName = InetAddress.getAllByName(host);
                    if (addrByName != null) {
                        addr = addrByName;
                    }
                } catch (UnknownHostException e) {
                    if (logger.isInfoEnabled()) {
                        LogUtils.throwing(LogLevel.INFO, logger, getClass(), "run", e, "failed to resolve host {}; connection will still be attempted", host);
                    }
                }
                long deadline = DiscoveryConstraints.process(
                        rawUnicastDiscoveryConstraints).getConnectionDeadline(
                        Long.MAX_VALUE);
                long now = System.currentTimeMillis();
                if (deadline <= now)
                    throw new SocketTimeoutException("timeout expired before"
                            + " connection attempt");
                long timeLeft = deadline - now;
                int timeout = timeLeft >= Integer.MAX_VALUE ?
                        Integer.MAX_VALUE : (int) timeLeft;
                // attempt connection even if host name was not resolved
                if (addr.length == 0) {
                    attemptResponse(
                            new InetSocketAddress(host, port), timeout);
                    return;
                }
                for (InetAddress anAddr : addr) {
                    try {
                        attemptResponse(
                                new InetSocketAddress(anAddr, port), timeout);
                        return;
                    } catch (Exception e) {
                        if (logger.isDebugEnabled()) {
                            LogUtils.throwing(logger, getClass(), "run", e, "exception responding to {}:{}", anAddr, port);
                        }
                    }
                    timeLeft = deadline - System.currentTimeMillis();
                    timeout = timeLeft >= Integer.MAX_VALUE ?
                            Integer.MAX_VALUE : (int) timeLeft;
                    if (timeLeft <= 0)
                        throw new SocketTimeoutException("timeout expired"
                                + " before successful response");
                }
            } catch (Exception e) {
                if (logger.isInfoEnabled()) {
                    LogUtils.throwing(LogLevel.INFO, logger, getClass(), "run", e, "failed to respond to {} on port {}", Arrays.asList(addr), port);
                }
            }
        }

        /**
         * No ordering
         */
        public boolean runAfter(List tasks, int size) {
            return false;
        }

        /**
         * attempt a connection to multicast request client
         */
        private void attemptResponse(InetSocketAddress addr, int timeout)
                throws Exception {
            Socket s = new Socket();
            try {
                s.connect(addr, timeout);
                respond(s);
            } finally {
                try {
                    s.close();
                } catch (IOException e) {
                    logger.debug("exception closing socket", e);
                }
            }
        }
    }

    /**
     * Socket for unicast discovery response.
     */
    private final class SocketTask implements Runnable {

        /**
         * The socket
         */
        public final Socket socket;

        /**
         * Simple constructor
         */
        SocketTask(Socket socket) {
            this.socket = socket;
        }

        /**
         * Process a unicast discovery request
         */
        public void run() {
            try {
                long time = 0;
                if (logger.isTraceEnabled()) {
                    time = SystemTime.timeMillis();
                }
                respond(socket);
                if (logger.isTraceEnabled()) {
                    long took = SystemTime.timeMillis() - time;
                    logger.trace("Unicast task [" + System.identityHashCode(this) + "] executed, took [" + took + "ms]");
                }
            } catch (Exception e) {
                if (logger.isDebugEnabled()) {
                    LogUtils.throwing(logger, getClass(), "run", e,
                            "exception handling unicast discovery from {}:{}", socket.getInetAddress(), socket.getPort());
                }
            }
        }

        /**
         * No ordering
         */
        public boolean runAfter(List tasks, int size) {
            return false;
        }
    }

    /**
     * Service lease expiration thread code
     */
    private class ServiceExpireThread extends InterruptedStatusThread {

        /**
         * Create a daemon thread
         */
        ServiceExpireThread() {
            super("service expire");
            setDaemon(true);
        }

        @Override
        public void run() {
            while (!hasBeenInterrupted()) {
                while (true) {
                    long now = SystemTime.timeMillis();
                    SvcRegExpirationKey regExpirationKey;
                    try {
                        regExpirationKey = serviceByTime.firstKey();
                    } catch (NoSuchElementException e) {
                        if (loggerServiceExpire.isTraceEnabled()) {
                            loggerServiceExpire.trace("No services to check");
                        }
                        // map is empty
                        break;
                    }
                    if (regExpirationKey.leaseExpiration > now) {
                        if (loggerServiceExpire.isTraceEnabled()) {
                            loggerServiceExpire.trace("[" + System.identityHashCode(regExpirationKey) + "]: Service [" + regExpirationKey.svcReg.item.serviceID + "]: lease [" + regExpirationKey.svcReg.leaseID + "/" + regExpirationKey.leaseExpiration + "] is higher than [" + now + "], bail");
                        }
                        break;
                    } else {
                        if (loggerServiceExpire.isDebugEnabled()) {
                            loggerServiceExpire.debug("[" + System.identityHashCode(regExpirationKey) + "]: Service [" + regExpirationKey.svcReg.item.serviceID + "]: lease [" + regExpirationKey.svcReg.leaseID + "/" + regExpirationKey.leaseExpiration + "] is lower than [" + now + "], try and expire...");
                        }
                    }
                    try {
                        concurrentObj.writeLock();
                    } catch (ConcurrentLockException e) {
                        // interrupted, bail
                        return;
                    }
                    try {
                        synchronized (regExpirationKey) {
                            // if we don't have it in our registry (it was removed)
                            // just remove it from the serviceByTime and continue
                            // Note, we do this here since the renew is not locked, so
                            // we might end up adding something to serviceByTime while a delete is in progress
                            SvcReg tmpReg = serviceByID.get(regExpirationKey.svcReg.item.serviceID);
                            if (tmpReg == null || !tmpReg.leaseID.equals(regExpirationKey.svcReg.leaseID)) {
                                Object removed = serviceByTime.remove(regExpirationKey);
                                if (loggerServiceExpire.isDebugEnabled()) {
                                    loggerServiceExpire.debug("[" + System.identityHashCode(regExpirationKey) + "]: Service [" + regExpirationKey.svcReg.item.serviceID + "]: Not found, removing (" + (removed != null) + ")");
                                }
                                continue;
                            }
                            // try and get the first key again, if  its not the same, someone already managed to renew
                            // until we managed to get the lock, simply continue to the next one
                            SvcRegExpirationKey otherExpirationKey;
                            try {
                                otherExpirationKey = serviceByTime.firstKey();
                            } catch (NoSuchElementException e) {
                                if (loggerServiceExpire.isTraceEnabled()) {
                                    loggerServiceExpire.trace("No services to check");
                                }
                                // map is empty
                                break;
                            }
                            if (!otherExpirationKey.equals(regExpirationKey)) {
                                if (loggerServiceExpire.isTraceEnabled()) {
                                    loggerServiceExpire.debug("[" + System.identityHashCode(regExpirationKey) + "]: Service [" + regExpirationKey.svcReg.item.serviceID + "]: Renew happened until lock acquired, continue");
                                }
                                continue;
                            }
                            if (loggerServiceExpire.isDebugEnabled()) {
                                loggerServiceExpire.debug("[" + System.identityHashCode(regExpirationKey) + "]: Service [" + regExpirationKey.svcReg.item.serviceID + "]: Expired, deleting ....");
                            }
                            deleteService(regExpirationKey, regExpirationKey.svcReg, now);
                            queueEvents();
                        }
                    } finally {
                        concurrentObj.writeUnlock();
                    }
                }
                try {
                    if (loggerServiceExpire.isTraceEnabled()) {
                        loggerServiceExpire.trace("Sleeping for 200 millis");
                    }
                    // we want to wake early, since space active election might rely on this
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }

    /**
     * Event lease expiration thread code
     */
    private class EventExpireThread extends InterruptedStatusThread {

        /**
         * Create a daemon thread
         */
        EventExpireThread() {
            super("event expire");
            setDaemon(true);
        }

        @Override
        public void run() {
            while (!hasBeenInterrupted()) {
                while (true) {
                    long now = SystemTime.timeMillis();
                    EventRegKeyExpiration regExpirationKey;
                    try {
                        regExpirationKey = (EventRegKeyExpiration) eventByTime.firstKey();
                    } catch (NoSuchElementException e) {
                        if (loggerEventExpire.isTraceEnabled()) {
                            loggerEventExpire.trace("No events to check");
                        }
                        break;
                    }
                    if (regExpirationKey.leaseExpiration > now) {
                        if (loggerEventExpire.isTraceEnabled()) {
                            loggerEventExpire.trace("[" + System.identityHashCode(regExpirationKey) + "]: Event [" + regExpirationKey.reg.eventID + "]: lease [" + regExpirationKey.leaseExpiration + "] is higher than [" + now + "], bail");
                        }
                        break;
                    } else {
                        if (loggerEventExpire.isDebugEnabled()) {
                            loggerEventExpire.debug("[" + System.identityHashCode(regExpirationKey) + "]: Event [" + regExpirationKey.reg.eventID + "]: lease [" + regExpirationKey.leaseExpiration + "] is lower than [" + now + "], try and expire...");
                        }
                    }
                    try {
                        concurrentObj.writeLock();
                    } catch (ConcurrentLockException e) {
                        return;
                    }
                    try {
                        synchronized (regExpirationKey) {
                            // if we don't have it in our registry (it was removed)
                            // just remove it from the eventsByTime and continue
                            // Note, we do this here since the renew is not locked, so
                            // we might end up adding something to eventByTime while a delete is in progress
                            EventReg tmpReg = eventByID.get(regExpirationKey.reg.eventID);
                            if (tmpReg == null || !tmpReg.leaseID.equals(regExpirationKey.reg.leaseID)) {
                                Object removed = eventByTime.remove(regExpirationKey);
                                if (loggerEventExpire.isDebugEnabled()) {
                                    loggerEventExpire.debug("[" + System.identityHashCode(regExpirationKey) + "]: Event [" + regExpirationKey.reg.eventID + "]: Not found, removing (" + (removed != null) + ")");
                                }
                                continue;
                            }
                            // try and get the first key again, if  its not the same, someone already managed to renew
                            // until we managed to get the lock, simply continue to the next one
                            EventRegKeyExpiration otherFirstKey;
                            try {
                                otherFirstKey = (EventRegKeyExpiration) eventByTime.firstKey();
                            } catch (NoSuchElementException e) {
                                if (loggerEventExpire.isTraceEnabled()) {
                                    loggerEventExpire.trace("No events to check");
                                }
                                break;
                            }
                            if (!otherFirstKey.equals(regExpirationKey)) {
                                if (loggerEventExpire.isTraceEnabled()) {
                                    loggerEventExpire.trace("[" + System.identityHashCode(regExpirationKey) + "]: Event [" + regExpirationKey.reg.eventID + "]: Renew happened until lock acquired, continue");
                                }
                                continue;
                            }
                            if (loggerEventExpire.isDebugEnabled()) {
                                loggerEventExpire.debug("[" + System.identityHashCode(regExpirationKey) + "]: Event [" + regExpirationKey.reg.eventID + "]: Expired, deleting ....");
                            }
                            deleteEvent(regExpirationKey, regExpirationKey.reg);
                        }
                    } finally {
                        concurrentObj.writeUnlock();
                    }
                }
                try {
                    // no need for high accuracy with event expiration
                    if (loggerEventExpire.isTraceEnabled()) {
                        loggerEventExpire.trace("Sleeping for 500 millis");
                    }
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }

    private void destroyImpl() {
        long now = SystemTime.timeMillis();
        long endTime = now + unexportTimeout;
        if (endTime < 0)
            endTime = Long.MAX_VALUE;
        boolean unexported = false;
            /* first try unexporting politely */
        while (!unexported && (now < endTime)) {
            unexported = serverExporter.unexport(false);
            if (!unexported) {
                try {
                    final long sleepTime =
                            Math.min(unexportWait, endTime - now);
                    Thread.sleep(sleepTime);
                    now = SystemTime.timeMillis();
                } catch (InterruptedException e) {
                    logger.debug("exception during unexport wait", e);
                }
            }
        }
            /* if still not unexported, forcibly unexport */
        if (!unexported) {
            serverExporter.unexport(true);
        }

            /* all daemons must terminate before deleting persistent store */
        serviceExpirer.interrupt();
        eventExpirer.interrupt();
        unicaster.interrupt();
        if (multicaster != null) {
            multicaster.interrupt();
        }
        if (announcer != null) {
            announcer.interrupt();
        }
        List<Runnable> pending = taskerComm.shutdownNow();
        taskerEvent.terminate();

        joiner.terminate();
        discoer.terminate();
        try {
            serviceExpirer.join();
            eventExpirer.join();
            unicaster.join();
            if (multicaster != null) {
                multicaster.join();
            }
            if (announcer != null) {
                announcer.join();
            }
        } catch (InterruptedException e) {
            // do nothing
        }
        closeRequestSockets(pending);
        if (lifeCycle != null) {
            lifeCycle.unregister(this);
        }
        if (loginContext != null) {
            try {
                loginContext.logout();
            } catch (LoginException e) {
                logger.info("logout failed", e);
            }
        }
        deactivate();
        logger.info("Reggie shutdown completed");
    }

    /**
     * Multicast discovery request thread code.
     */
    private class MulticastThread extends InterruptedStatusThread {

        /**
         * Multicast group address used by multicast requests
         */
        private final InetAddress requestAddr;
        /**
         * Multicast socket to receive packets
         */
        private final MulticastSocket socket;
        /**
         * Interfaces for which configuration failed
         */
        private final List<NetworkInterface> failedInterfaces = new ArrayList<NetworkInterface>();

        /**
         * Create a high priority daemon thread.  Set up the socket now rather than in run, so that
         * we get any exception up front.
         */
        MulticastThread() throws IOException {
            super("multicast request");
            setDaemon(true);
            if (multicastInterfaces != null && multicastInterfaces.length == 0) {
                requestAddr = null;
                socket = null;
                return;
            }
            requestAddr = Constants.getRequestAddress();
            socket = new MulticastSocket(Constants.getDiscoveryPort());
            if (multicastInterfaces != null) {
                final long startTime = LogUtils.getCurrTimeIfDebugEnabled(logger);
                LogLevel failureLogLevel = multicastInterfacesSpecified ?
                        LogLevel.WARNING : LogLevel.DEBUG;
                for (NetworkInterface nic : multicastInterfaces) {
                    try {
                        final long nicStartTime = LogUtils.getCurrTimeIfTraceEnabled(logger);
                        socket.setNetworkInterface(nic);
                        if (logger.isTraceEnabled())
                            logger.trace(LogUtils.formatDuration(nicStartTime, "MulticastThread() - setNetworkInterface() for nic " + nic));
                        socket.joinGroup(requestAddr);
                    } catch (IOException e) {
                        failedInterfaces.add(nic);
                        if (failureLogLevel.isEnabled(logger))
                            LogUtils.throwing(failureLogLevel, logger, getClass(), "<init>", e, "exception enabling {}", nic);
                    }
                }
                if (logger.isDebugEnabled())
                    logger.debug(LogUtils.formatDuration(startTime, "MulticastThread() processed " + multicastInterfaces.length + " network interfaces"));
            } else {
                try {
                    socket.joinGroup(requestAddr);
                } catch (IOException e) {
                    failedInterfaces.add(null);
                    logger.warn("exception enabling default interface", e);
                }
            }
        }

        @Override
        public void run() {
            if (multicastInterfaces != null && multicastInterfaces.length == 0) {
                return;
            }
            byte[] buf = new byte[
                    multicastRequestConstraints.getMulticastMaxPacketSize(
                            DEFAULT_MAX_PACKET_SIZE)];
            DatagramPacket dgram = new DatagramPacket(buf, buf.length);
            long retryTime =
                    SystemTime.timeMillis() + multicastInterfaceRetryInterval;
            while (!hasBeenInterrupted()) {
                try {
                    int timeout = 0;
                    if (!failedInterfaces.isEmpty()) {
                        timeout =
                                (int) (retryTime - SystemTime.timeMillis());
                        if (timeout <= 0) {
                            retryFailedInterfaces();
                            if (failedInterfaces.isEmpty()) {
                                timeout = 0;
                            } else {
                                timeout = multicastInterfaceRetryInterval;
                                retryTime =
                                        SystemTime.timeMillis() + timeout;
                            }
                        }
                    }
                    socket.setSoTimeout(timeout);
                    dgram.setLength(buf.length);
                    try {
                        socket.receive(dgram);
                    } catch (NullPointerException e) {
                        break; // workaround for bug 4190513
                    }

                    int pv;
                    try {
                        pv = ByteBuffer.wrap(dgram.getData(),
                                dgram.getOffset(),
                                dgram.getLength()).getInt();
                    } catch (BufferUnderflowException e) {
                        throw new DiscoveryProtocolException(null, e);
                    }
                    multicastRequestConstraints.checkProtocolVersion(pv);
                    taskerComm.execute(new DecodeRequestTask(dgram, getDiscovery(pv)));

                    buf = new byte[buf.length];
                    dgram = new DatagramPacket(buf, buf.length);

                } catch (SocketTimeoutException e) {
                    // retry failed network interfaces in next iteration
                } catch (InterruptedIOException e) {
                    break;
                } catch (Exception e) {
                    if (hasBeenInterrupted()) {
                        break;
                    }
                    logger.debug(
                            "exception receiving multicast request", e);
                }
            }
            socket.close();
        }

        /* This is a workaround for Thread.interrupt not working on
         * MulticastSocket.receive on all platforms.
         */
        @Override
        public synchronized void interrupt() {
            socket.close();
            super.interrupt();
        }

        /**
         * Attempts to configure each interface contained in the failedInterfaces list, removing it
         * from the list if configuration succeeds.  The null value is used to indicate the default
         * network interface.
         */
        private void retryFailedInterfaces() {
            final LogLevel level = multicastInterfacesSpecified ? LogLevel.INFO : LogLevel.DEBUG;
            if (level.isEnabled(logger))
                level.log(logger, "retryFailedInterfaces() will attempt to enable " + failedInterfaces.size() + " interfaces...");
            for (Iterator<NetworkInterface> i = failedInterfaces.iterator(); i.hasNext(); ) {
                NetworkInterface nic = i.next();
                long startTime = level.isEnabled(logger) ? System.currentTimeMillis() : 0;
                try {
                    if (nic != null)
                        socket.setNetworkInterface(nic);
                    socket.joinGroup(requestAddr);
                    i.remove();
                    if (level.isEnabled(logger)) {
                        String message = nic == null ? "Enabled default interface" : "Enabled " + nic;
                        level.log(logger, LogUtils.formatDuration(startTime, message));
                    }
                } catch (IOException e) {
                    // keep nic in failedInterfaces
                    if (level.isEnabled(logger)) {
                        String message = nic == null ? "Failed to enabled default interface" : "Failed to enabled " + nic;
                        level.log(logger, LogUtils.formatDuration(startTime, message));
                    }
                }
            }
        }
    }

    /**
     * Unicast discovery request thread code.
     */
    private class UnicastThread extends InterruptedStatusThread {
        /**
         * Server socket to accepts connections on.
         */
        private ServerSocket listen;
        /**
         * Listen port
         */
        public int port;

        /**
         * The host
         */
        public final InetAddress host;

        /**
         * Create a daemon thread.  Set up the socket now rather than in run, so that we get any
         * exception up front.
         */
        UnicastThread(InetAddress host, int port) throws IOException {
            super("unicast request");
            setDaemon(true);
            int backlog = Integer.getInteger(SystemProperties.LRMI_ACCEPT_BACKLOG,
                    SystemProperties.LRMI_ACCEPT_BACKLOG_DEFUALT);
            this.host = host;
            if (port == 0) {
                try {
                    listen = new ServerSocket(Constants.getDiscoveryPort(), backlog, host);
                } catch (IOException e) {
                    logger.debug("failed to bind to default port", e);
                }
            }
            if (listen == null) {
                listen = new ServerSocket(port, backlog, host);
            }
            this.port = listen.getLocalPort();
        }

        @Override
        public void run() {
            while (!hasBeenInterrupted()) {
                try {
                    Socket socket = listen.accept();
                    if (hasBeenInterrupted()) {
                        try {
                            socket.close();
                        } catch (IOException e) {
                            logger.debug("exception closing socket", e);
                        }
                        break;
                    }
                    SocketTask task = new SocketTask(socket);
                    if (logger.isTraceEnabled()) {
                        logger.trace("Unicast task [" + System.identityHashCode(task) + "] added");
                    }
                    try {
                        taskerComm.execute(task);
                    } catch (RejectedExecutionException e) {
                        logger.warn("Rejected task because of overflow in backlog, please consider increasing either the commTaskManager number of threads, or its backlog");
                        try {
                            socket.close();
                        } catch (Exception se) {
                            // ignore
                        }
                    }
                } catch (InterruptedIOException e) {
                    break;
                } catch (Exception e) {
                    logger.debug("exception listening on socket", e);
                }
                /* if we fail in any way, just forget about it */
            }
            try {
                listen.close();
            } catch (IOException e) {
                logger.debug("exception closing server socket", e);
            }
        }

        /* This is a workaround for Thread.interrupt not working on
         * ServerSocket.accept on all platforms.  ServerSocket.close
         * can't be used as a workaround, because it also doesn't work
         * on all platforms.
         */
        @Override
        public synchronized void interrupt() {
            try {
                (new Socket(host, port)).close();
            } catch (IOException e) {
                // do nothing
            } finally {
                super.interrupt();
            }
        }

    }

    /**
     * Multicast discovery announcement thread code.
     */
    private class AnnounceThread extends InterruptedStatusThread {
        /**
         * Multicast socket to send packets on
         */
        private final MulticastSocket socket;
        /**
         * Cached datagram packets
         */
        private DatagramPacket[] dataPackets = null;
        /**
         * LookupLocator associated with cached datagram packets
         */
        private LookupLocator lastLocator;
        /**
         * Groups associated with cached datagram packets
         */
        private String[] lastGroups;

        /**
         * Create a daemon thread.  Set up the socket now rather than in run, so that we get any
         * exception up front.
         */
        AnnounceThread() throws IOException {
            super("discovery announcement");
            setDaemon(true);
            if (multicastInterfaces == null || multicastInterfaces.length > 0) {
                socket = new MulticastSocket();
                socket.setTimeToLive(
                        multicastAnnouncementConstraints.getMulticastTimeToLive(
                                Constants.getTtl()));
            } else {
                socket = null;
            }
        }

        @Override
        public synchronized void run() {
            if (multicastInterfaces != null && multicastInterfaces.length == 0) {
                return;
            }
            try {
                while (!hasBeenInterrupted() && announce(memberGroups)) {
                    wait(multicastAnnouncementInterval);
                }
            } catch (InterruptedException e) {
                // do nothing
            }
            if (memberGroups.length > 0)
                announce(new String[0]);//send NO_GROUPS just before shutdown
            socket.close();
        }

        /**
         * Announce membership in the specified groups, and return false if interrupted, otherwise
         * return true.  This method is run from synchronized run method in thread.
         */
        private boolean announce(String[] groups) {
            if (dataPackets == null || !lastLocator.equals(myLocator) ||
                    !Arrays.equals(lastGroups, groups)) {
                List<DatagramPacket> packets = new ArrayList<DatagramPacket>();
                Discovery disco;
                try {
                    disco = getDiscovery(multicastAnnouncementConstraints
                            .chooseProtocolVersion());
                } catch (DiscoveryProtocolException e) {
                    throw new AssertionError(e);
                }
                EncodeIterator ei = disco.encodeMulticastAnnouncement(
                        new MulticastAnnouncement(announcementSeqNo++,
                                myLocator.getHost(),
                                myLocator.getPort(),
                                groups,
                                myServiceID),
                        multicastAnnouncementConstraints
                                .getMulticastMaxPacketSize(DEFAULT_MAX_PACKET_SIZE),
                        multicastAnnouncementConstraints
                                .getUnfulfilledConstraints());
                while (ei.hasNext()) {
                    try {
                        packets.addAll(Arrays.asList(ei.next()));
                    } catch (Exception e) {
                        LogLevel logLevel = e instanceof UnsupportedConstraintException ? LogLevel.DEBUG : LogLevel.INFO;
                        logLevel.log(logger, "exception encoding multicast announcement", e);
                    }
                }
                lastLocator = myLocator;
                lastGroups = groups;
                dataPackets = packets.toArray(new DatagramPacket[packets.size()]);
            }
            try {
                send(dataPackets);
            } catch (InterruptedIOException e) {
                return false;
            }
            return true;
        }

        /**
         * Attempts to multicast the given packets on each of the configured network interfaces.
         */
        private void send(DatagramPacket[] packets) throws InterruptedIOException {
            if (multicastInterfaces != null) {
                LogLevel failureLogLevel = multicastInterfacesSpecified ? LogLevel.WARNING : LogLevel.DEBUG;
                long startTime = LogUtils.getCurrTimeIfDebugEnabled(logger);
                for (NetworkInterface multicastInterface : multicastInterfaces)
                    send(packets, multicastInterface, failureLogLevel);
                if (logger.isDebugEnabled())
                    logger.debug(LogUtils.formatDuration(startTime, Thread.currentThread().getName() + ".send()"));
            } else {
                send(packets, null, LogLevel.WARNING);
            }
        }

        /**
         * Attempts to multicast the given packets on the specified network interface, logging
         * failures at the given logging level.  If the specified network interface is null, then
         * the default interface is used.
         */
        private void send(DatagramPacket[] packets, NetworkInterface nic, LogLevel failureLogLevel) throws InterruptedIOException {
            if (nic != null) {
                long startTime = LogUtils.getCurrTimeIfTraceEnabled(logger);
                try {
                    socket.setNetworkInterface(nic);
                    if (logger.isTraceEnabled())
                        logger.trace(LogUtils.formatDuration(startTime, "send() - setNetworkInterface() for nic " + nic));
                } catch (SocketException e) {
                    if (failureLogLevel.isEnabled(logger)) {
                        LogUtils.throwing(failureLogLevel, logger, getClass(), "send", e, "exception setting {}", nic);
                    }
                    if (logger.isTraceEnabled())
                        logger.trace(LogUtils.formatDuration(startTime, "send() - setNetworkInterface() failed for nic " + nic));
                    return;
                }
            }
            for (DatagramPacket packet : packets) {
                try {
                    socket.send(packet);
                } catch (InterruptedIOException e) {
                    throw e;
                } catch (IOException e) {
                    if (nic != null) {
                        if (failureLogLevel.isEnabled(logger)) {
                            LogUtils.throwing(failureLogLevel, logger, getClass(), "send", e, "exception sending packet on {}", nic);
                        }
                    } else {
                        failureLogLevel.log(logger, "exception sending packet on default interface", e);
                    }
                }
            }
        }
    }

    // This method's javadoc is inherited from an interface of this class
    public Object getServiceProxy() throws NoSuchObjectException {
        ready.check();
        return proxy;
    }

    // This method's javadoc is inherited from an interface of this class
    public Object getProxy() {
        // locally-called method - no need to check initialization state
        return myRef;
    }

    // This method's javadoc is inherited from an interface of this class
    public TrustVerifier getProxyVerifier() throws NoSuchObjectException {
        ready.check();
        return new ProxyVerifier(myRef, myServiceID);
    }

    private volatile long registerMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public ServiceRegistration register(Item nitem, long leaseDuration) throws NoSuchObjectException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        concurrentObj.writeLock();
        try {
            ready.check();
            ServiceRegistration reg = registerDo(nitem, leaseDuration);
            if (logger.isDebugEnabled()) {
                logger.debug("registered instance of {} as {} with lease of {} ms",
                                nitem.serviceType.getName(), reg.getServiceID(), (reg.getLease().getExpiration() - SystemTime.timeMillis()));
            }
            return reg;
        } finally {
            concurrentObj.writeUnlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > registerMaxDuration) {
                    registerMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + registerMaxDuration + "]");
            }
        }
    }

    public ServiceDetails serviceDetails(ServiceID serviceID) throws RemoteException {
        SvcReg svcReg = serviceByID.get(serviceID);
        if (svcReg == null) {
            return new ServiceDetails(myServiceID, false, -1);
        } else {
            return new ServiceDetails(myServiceID, true, svcReg.registrationTime);
        }
    }

    private volatile long lookupMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public MarshalledWrapper lookup(Template tmpl) throws NoSuchObjectException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        Item item = lookupItem(tmpl);
        if (item == null) {
            return null;
        }
        if (loggerStats.isTraceEnabled()) {
            long duration = SystemTime.timeMillis() - startTime;
            if (duration > lookupMaxDuration) {
                lookupMaxDuration = duration;
            }
            loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + lookupMaxDuration + "]");
        }
        return item.service;
    }

    private Item lookupItem(Template tmpl) throws NoSuchObjectException {
        if (loggerCache.isTraceEnabled()) {
            loggerCache.trace("Lookup item for template: " + tmpl);
        }
        ConcurrentHashMap<ServiceID, Item> items = lookupServiceCache.get(tmpl);
        if (items != null) {
            Item retVal = null;
            try {
                Collection<Item> values = items.values();
                int size = values.size();
                if (size > 0) {
                    // generate a random one to return
                    int index = random.nextInt(size);
                    Iterator<Item> it = values.iterator();
                    for (int i = 0; it.hasNext(); i++) {
                        retVal = it.next();
                        if (i == index) {
                            if (loggerCache.isTraceEnabled()) {
                                loggerCache.trace("Cache hit for template: " + tmpl + ", index [" + index + "] out of [" + size + "]");
                            }
                            return retVal;
                        }
                    }
                    // if we have a match on first, and it was removed from "under our feet"
                    // while we iterate, then we can return the first one
                    if (retVal != null) {
                        if (loggerCache.isTraceEnabled()) {
                            loggerCache.trace("Cache hit for template: " + tmpl + ", index [0] out of [" + size + "]");
                        }
                        return retVal;
                    }
                }
            } catch (NoSuchElementException e) {
                // if we have a match on first, and it was removed from "under our feet"
                // while we iterate, then we can return the first one
                if (retVal != null) {
                    return retVal;
                }
            }
        }

        // Did not find a match on the lookup cache
        // in this case, in refreshLookupCache, we make sure to add this templates to the cache explicitly
        // so, if we miss on them, it means that they are not there and no need to go through the read lock and
        // actual lookup
        if (useCacheForSpaceMiss && tmpl.serviceTypes != null && tmpl.serviceTypes.length == 1
                && tmpl.serviceTypes[0].getName().equals(SERVICE_CLASSNAME)
                && tmpl.attributeSetTemplates != null
                && (tmpl.attributeSetTemplates.length < 5)) {
            if (loggerCache.isTraceEnabled()) {
                loggerCache.trace("Cache hit (null) for template: " + tmpl);
            }
            return null;
        }

        Item item;
        concurrentObj.readLock();
        try {
            // just in case we a lot were waiting on this lock
            items = lookupServiceCache.get(tmpl);
            if (items != null && !items.isEmpty()) {
                if (loggerCache.isTraceEnabled()) {
                    loggerCache.trace("Cache hit for template: " + tmpl);
                }
                try {
                    Iterator<Item> it = items.values().iterator();
                    if (it.hasNext()) {
                        return it.next();
                    }
                } catch (NoSuchElementException e) {
                    // do nothing, continue
                }
            }
            ready.check();
            item = lookupDo(tmpl);
            if (item != null) {
                if (loggerCache.isTraceEnabled()) {
                    loggerCache.trace("Adding to cache template (on miss): " + tmpl
                            + "\n   ---- > " + item.serviceID + " " + item.serviceType);
                }
                items = lookupServiceCache.get(tmpl);
                if (items == null) {
                    items = new SizeConcurrentHashMap<ServiceID, Item>(1);
                    lookupServiceCache.put(tmpl, items);
                }
                items.put(item.serviceID, item);
            } // no need to remove from the cache on miss, since it was cleaned in refresh....
        } finally {
            concurrentObj.readUnlock();
        }
        return item;
    }

    private volatile long lookupMaxMatchesMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public Matches lookup(Template tmpl, int maxMatches) throws NoSuchObjectException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        if (maxMatches == 1) {
            ArrayList<Item> items = new ArrayList<Item>();
            Item item = lookupItem(tmpl);
            if (item != null) {
                items.add(item);
            }
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > lookupMaxMatchesMaxDuration) {
                    lookupMaxMatchesMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + lookupMaxMatchesMaxDuration + "]\t\tMAX MATCHES [1]");
            }
            return new Matches(items, items.size());
        }
        concurrentObj.readLock();
        try {
            ready.check();
            return lookupDo(tmpl, maxMatches);
        } finally {
            concurrentObj.readUnlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > lookupMaxMatchesMaxDuration) {
                    lookupMaxMatchesMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + lookupMaxMatchesMaxDuration + "]\t\tMAX MATCHES [" + maxMatches + "]");
            }
        }
    }

    public RegistrarEventRegistration notify(Template tmpl, int transitions, RemoteEventListener listener,
                                             MarshalledObject handback, long leaseDuration)
            throws RemoteException {
        return notify(tmpl, transitions, listener, handback, leaseDuration, ServiceRegistrar.NOTIFY_PLAIN);
    }

    private volatile long notifyMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public RegistrarEventRegistration notify(Template tmpl, int transitions, RemoteEventListener listener,
                                             MarshalledObject handback, long leaseDuration, int notifyType)
            throws RemoteException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        concurrentObj.writeLock();
        try {
            ready.check();
            RegistrarEventRegistration reg = notifyDo(
                    tmpl, transitions, listener, handback, leaseDuration, notifyType);
            if (logger.isDebugEnabled()) {
                logger.debug("registered event listener {} as {}", listener, ((ReferentUuid) reg.getLease()).getReferentUuid());
            }
            return reg;
        } finally {
            concurrentObj.writeUnlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > notifyMaxDuration) {
                    notifyMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + notifyMaxDuration + "]");
            }
        }
    }

    private volatile long getEntryClassesMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public EntryClassBase[] getEntryClasses(Template tmpl)
            throws NoSuchObjectException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        concurrentObj.readLock();
        try {
            ready.check();
            return getEntryClassesDo(tmpl);
        } finally {
            concurrentObj.readUnlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > getEntryClassesMaxDuration) {
                    getEntryClassesMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + getEntryClassesMaxDuration + "]");
            }
        }
    }

    private volatile long getFieldValuesMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public Object[] getFieldValues(Template tmpl, int setIndex, int field) throws NoSuchObjectException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        concurrentObj.readLock();
        try {
            ready.check();
            return getFieldValuesDo(tmpl, setIndex, field);
        } finally {
            concurrentObj.readUnlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > getFieldValuesMaxDuration) {
                    getFieldValuesMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + getFieldValuesMaxDuration + "]");
            }
        }
    }

    private volatile long getServiceTypesMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public ServiceTypeBase[] getServiceTypes(Template tmpl, String prefix) throws NoSuchObjectException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        concurrentObj.readLock();
        try {
            ready.check();
            return getServiceTypesDo(tmpl, prefix);
        } finally {
            concurrentObj.readUnlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > getServiceTypesMaxDuration) {
                    getServiceTypesMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + getServiceTypesMaxDuration + "]");
            }
        }
    }

    private volatile long getLocatorMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public LookupLocator getLocator() throws NoSuchObjectException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        ready.check();
        if (loggerStats.isTraceEnabled()) {
            long duration = SystemTime.timeMillis() - startTime;
            if (duration > getLocatorMaxDuration) {
                getLocatorMaxDuration = duration;
            }
            loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + getLocatorMaxDuration + "]");
        }
        return myLocator;
    }

    // This method's javadoc is inherited from an interface of this class
    public Object getAdmin() throws NoSuchObjectException {
        ready.check();
        return AdminProxy.getInstance(myRef, myServiceID);
    }

    private volatile long addAttributesMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public void addAttributes(ServiceID serviceID, Uuid leaseID, EntryRep[] attrSets)
            throws NoSuchObjectException, UnknownLeaseException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        try {
            concurrentObj.writeLock();
        } catch (ConcurrentLockException e) {
            return;
        }
        try {
            ready.check();
            if (serviceID.equals(myServiceID))
                throw new SecurityException("privileged service id");
            addAttributesDo(serviceID, leaseID, attrSets);
            queueEvents();
        } finally {
            concurrentObj.writeUnlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > addAttributesMaxDuration) {
                    addAttributesMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + addAttributesMaxDuration + "]");
            }
        }
    }

    private volatile long modifyAttributesMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public void modifyAttributes(ServiceID serviceID, Uuid leaseID, EntryRep[] attrSetTmpls, EntryRep[] attrSets)
            throws NoSuchObjectException, UnknownLeaseException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        try {
            concurrentObj.writeLock();
        } catch (ConcurrentLockException e) {
            return;
        }
        try {
            ready.check();
            if (serviceID.equals(myServiceID))
                throw new SecurityException("privileged service id");
            modifyAttributesDo(serviceID, leaseID, attrSetTmpls, attrSets);
            queueEvents();
        } finally {
            concurrentObj.writeUnlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > modifyAttributesMaxDuration) {
                    modifyAttributesMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + modifyAttributesMaxDuration + "]");
            }
        }
    }

    private volatile long setAttributesMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public void setAttributes(ServiceID serviceID, Uuid leaseID, EntryRep[] attrSets)
            throws NoSuchObjectException, UnknownLeaseException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        try {
            concurrentObj.writeLock();
        } catch (ConcurrentLockException e) {
            return;
        }
        try {
            ready.check();
            if (serviceID.equals(myServiceID))
                throw new SecurityException("privileged service id");
            setAttributesDo(serviceID, leaseID, attrSets);
            queueEvents();
        } finally {
            concurrentObj.writeUnlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > setAttributesMaxDuration) {
                    setAttributesMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + setAttributesMaxDuration + "]");
            }
        }
    }

    private volatile long cacncelServiceLeaseMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public void cancelServiceLease(ServiceID serviceID, Uuid leaseID) throws NoSuchObjectException, UnknownLeaseException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        try {
            concurrentObj.writeLock();
        } catch (ConcurrentLockException e) {
            return;
        }
        try {
            ready.check();
            cancelServiceLeaseDo(serviceID, leaseID);
            queueEvents();
            if (logger.isDebugEnabled()) {
                logger.debug("cancelled service registration {}", serviceID);
            }
        } finally {
            concurrentObj.writeUnlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > cacncelServiceLeaseMaxDuration) {
                    cacncelServiceLeaseMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + cacncelServiceLeaseMaxDuration + "]");
            }
        }
    }

    private volatile long renewServiceLeaseMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public long renewServiceLease(ServiceID serviceID, Uuid leaseID, long renewDuration)
            throws NoSuchObjectException, UnknownLeaseException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        try {
            ready.check();
            return renewServiceLeaseDo(serviceID, leaseID, renewDuration);
            /* addLogRecord is in renewServiceLeaseDo */
        } finally {
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > renewServiceLeaseMaxDuration) {
                    renewServiceLeaseMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + renewServiceLeaseMaxDuration + "]");
            }
        }
    }

    private volatile long cancelEventLeaseMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public void cancelEventLease(long eventID, Uuid leaseID) throws NoSuchObjectException, UnknownLeaseException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        try {
            concurrentObj.writeLock();
        } catch (ConcurrentLockException e) {
            return;
        }
        try {
            ready.check();
            cancelEventLeaseDo(eventID, leaseID);
            if (logger.isDebugEnabled()) {
                logger.debug("cancelled event registration {}", leaseID);
            }
        } finally {
            concurrentObj.writeUnlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > cancelEventLeaseMaxDuration) {
                    cancelEventLeaseMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + cancelEventLeaseMaxDuration + "]");
            }
        }
    }

    private volatile long renewEventLeaseMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public long renewEventLease(long eventID, Uuid leaseID, long renewDuration)
            throws NoSuchObjectException, UnknownLeaseException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        try {
            ready.check();
            return renewEventLeaseDo(eventID, leaseID, renewDuration);
            /* addLogRecord is in renewEventLeaseDo */
        } finally {
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > renewEventLeaseMaxDuration) {
                    renewEventLeaseMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + renewEventLeaseMaxDuration + "]");
            }
        }
    }

    private volatile long renewLeasesMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public RenewResults renewLeases(Object[] regIDs,
                                    Uuid[] leaseIDs,
                                    long[] renewDurations)
            throws NoSuchObjectException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        try {
            ready.check();
            return renewLeasesDo(regIDs, leaseIDs, renewDurations);
            /* addLogRecord is in renewLeasesDo */
        } finally {
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > renewLeasesMaxDuration) {
                    renewLeasesMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + renewLeasesMaxDuration + "]");
            }
        }
    }

    private volatile long cancelLeasesMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public Exception[] cancelLeases(Object[] regIDs, Uuid[] leaseIDs)
            throws NoSuchObjectException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        concurrentObj.writeLock();
        try {
            ready.check();
            Exception[] exceptions = cancelLeasesDo(regIDs, leaseIDs);
            queueEvents();
            if (logger.isDebugEnabled()) {
                for (int i = 0; i < regIDs.length; i++) {
                    if (exceptions != null && exceptions[i] != null) {
                        continue;
                    }
                    if (regIDs[i] instanceof ServiceID) {
                        logger.debug("cancelled service registration {}", regIDs[i]);
                    } else {
                        logger.debug("cancelled event registration {}", leaseIDs[i]);
                    }
                }
            }
            return exceptions;
        } finally {
            concurrentObj.writeUnlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > cancelLeasesMaxDuration) {
                    cancelLeasesMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + cancelLeasesMaxDuration + "]");
            }
        }
    }

    private volatile long getLookupAttributesMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public Entry[] getLookupAttributes() throws NoSuchObjectException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        concurrentObj.readLock();
        try {
            ready.check();
            /* no need to clone, never modified once created */
            return lookupAttrs;
        } finally {
            concurrentObj.readUnlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > getLookupAttributesMaxDuration) {
                    getLookupAttributesMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + getLookupAttributesMaxDuration + "]");
            }
        }
    }

    private volatile long addLookupAttributesMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public void addLookupAttributes(Entry[] attrSets) throws RemoteException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        try {
            concurrentObj.writeLock();
        } catch (ConcurrentLockException e) {
            return;
        }
        try {
            ready.check();
            EntryRep[] attrs = EntryRep.toEntryRep(attrSets, true);
            addAttributesDo(myServiceID, myLeaseID, attrs);
            joiner.addAttributes(attrSets);
            lookupAttrs = joiner.getAttributes();
            queueEvents();
        } catch (UnknownLeaseException e) {
            throw new AssertionError("Self-registration never expires");
        } finally {
            concurrentObj.writeUnlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > addLookupAttributesMaxDuration) {
                    addLookupAttributesMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + addLookupAttributesMaxDuration + "]");
            }
        }
    }

    private volatile long modifyLookupAttributesMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public void modifyLookupAttributes(Entry[] attrSetTemplates,
                                       Entry[] attrSets)
            throws RemoteException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        try {
            concurrentObj.writeLock();
        } catch (ConcurrentLockException e) {
            return;
        }
        try {
            ready.check();
            EntryRep[] tmpls = EntryRep.toEntryRep(attrSetTemplates, false);
            EntryRep[] attrs = EntryRep.toEntryRep(attrSets, false);
            modifyAttributesDo(myServiceID, myLeaseID, tmpls, attrs);
            joiner.modifyAttributes(attrSetTemplates, attrSets, true);
            lookupAttrs = joiner.getAttributes();
            queueEvents();
        } catch (UnknownLeaseException e) {
            throw new AssertionError("Self-registration never expires");
        } finally {
            concurrentObj.writeUnlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > modifyLookupAttributesMaxDuration) {
                    modifyLookupAttributesMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + modifyLookupAttributesMaxDuration + "]");
            }
        }
    }

    private volatile long getLookupGroupsMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public String[] getLookupGroups() throws NoSuchObjectException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        lookupGroupsRWL.readLock().lock();
        try {
            ready.check();
            /* no need to clone, never modified once created */
            return lookupGroups;
        } finally {
            lookupGroupsRWL.readLock().unlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > getLookupGroupsMaxDuration) {
                    getLookupGroupsMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + getLookupGroupsMaxDuration + "]");
            }
        }
    }

    private volatile long addLookupGroupsMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public void addLookupGroups(String[] groups) throws NoSuchObjectException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        lookupGroupsRWL.writeLock().lock();
        try {
            ready.check();
            DiscoveryGroupManagement dgm = (DiscoveryGroupManagement) discoer;
            try {
                dgm.addGroups(groups);
            } catch (IOException e) {
                throw new RuntimeException(e.toString());
            }
            lookupGroups = dgm.getGroups();
            if (logger.isDebugEnabled()) {
                logger.debug("added lookup groups {}", Arrays.asList(groups));
            }
        } finally {
            lookupGroupsRWL.writeLock().unlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > addLookupGroupsMaxDuration) {
                    addLookupGroupsMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + addLookupGroupsMaxDuration + "]");
            }
        }
    }

    private volatile long removeLookupGroupsMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public void removeLookupGroups(String[] groups)
            throws NoSuchObjectException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        lookupGroupsRWL.writeLock().lock();
        try {
            ready.check();
            DiscoveryGroupManagement dgm = (DiscoveryGroupManagement) discoer;
            dgm.removeGroups(groups);
            lookupGroups = dgm.getGroups();
            if (logger.isDebugEnabled()) {
                logger.debug("removed lookup groups {}", Arrays.asList(groups));
            }
        } finally {
            lookupGroupsRWL.writeLock().unlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > removeLookupGroupsMaxDuration) {
                    removeLookupGroupsMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + removeLookupGroupsMaxDuration + "]");
            }
        }
    }

    private volatile long setLookupGroupsMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public void setLookupGroups(String[] groups) throws NoSuchObjectException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        lookupGroupsRWL.writeLock().lock();
        try {
            ready.check();
            DiscoveryGroupManagement dgm = (DiscoveryGroupManagement) discoer;
            try {
                dgm.setGroups(groups);
            } catch (IOException e) {
                throw new RuntimeException(e.toString());
            }
            lookupGroups = dgm.getGroups();
            if (logger.isDebugEnabled()) {
                logger.debug("set lookup groups {}", (groups != null) ? Arrays.asList(groups) : null);
            }
        } finally {
            lookupGroupsRWL.writeLock().unlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > setLookupGroupsMaxDuration) {
                    setLookupGroupsMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + setLookupGroupsMaxDuration + "]");
            }
        }
    }

    private volatile long getLookupLocatorMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public LookupLocator[] getLookupLocators() throws NoSuchObjectException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        lookupLocatorsRWL.readLock().lock();
        try {
            ready.check();
            /* no need to clone, never modified once created */
            return lookupLocators;
        } finally {
            lookupLocatorsRWL.readLock().unlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > getLookupLocatorMaxDuration) {
                    getLookupLocatorMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + getLookupLocatorMaxDuration + "]");
            }
        }
    }

    private volatile long addLookupLocatorMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public void addLookupLocators(LookupLocator[] locators)
            throws RemoteException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        ready.check();
        locators = prepareLocators(locators, locatorPreparer, false);
        lookupLocatorsRWL.writeLock().lock();
        try {
            ready.check();
            DiscoveryLocatorManagement dlm =
                    (DiscoveryLocatorManagement) discoer;
            dlm.addLocators(locators);
            lookupLocators = dlm.getLocators();
            if (logger.isDebugEnabled()) {
                logger.debug("added lookup locators {}", Arrays.asList(locators));
            }
        } finally {
            lookupLocatorsRWL.writeLock().unlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > addLookupLocatorMaxDuration) {
                    addLookupLocatorMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + addLookupLocatorMaxDuration + "]");
            }
        }
    }

    private volatile long removeLookupLocatorMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public void removeLookupLocators(LookupLocator[] locators)
            throws RemoteException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        ready.check();
        locators = prepareLocators(locators, locatorPreparer, false);
        lookupLocatorsRWL.writeLock().lock();
        try {
            ready.check();
            DiscoveryLocatorManagement dlm =
                    (DiscoveryLocatorManagement) discoer;
            dlm.removeLocators(locators);
            lookupLocators = dlm.getLocators();
            if (logger.isDebugEnabled()) {
                logger.debug("removed lookup locators {}", Arrays.asList(locators));
            }
        } finally {
            lookupLocatorsRWL.writeLock().unlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > removeLookupLocatorMaxDuration) {
                    removeLookupLocatorMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + removeLookupLocatorMaxDuration + "]");
            }
        }
    }

    private volatile long setLookupLocatorMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public void setLookupLocators(LookupLocator[] locators)
            throws RemoteException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        ready.check();
        locators = prepareLocators(locators, locatorPreparer, false);
        lookupLocatorsRWL.writeLock().lock();
        try {
            ready.check();
            DiscoveryLocatorManagement dlm =
                    (DiscoveryLocatorManagement) discoer;
            dlm.setLocators(locators);
            lookupLocators = dlm.getLocators();
            if (logger.isDebugEnabled()) {
                logger.debug("set lookup locators {}", Arrays.asList(locators));
            }
        } finally {
            lookupLocatorsRWL.writeLock().unlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > setLookupLocatorMaxDuration) {
                    setLookupLocatorMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + setLookupLocatorMaxDuration + "]");
            }
        }
    }

    private volatile long addMembersGroupsMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public void addMemberGroups(String[] groups) throws NoSuchObjectException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        memberGroupRWL.writeLock().lock();
        try {
            ready.check();
            for (String group : groups) {
                if (indexOf(memberGroups, group) < 0)
                    memberGroups = (String[]) arrayAdd(memberGroups, group);
            }
            if (announcer != null) {
                synchronized (announcer) {
                    announcer.notify();
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("added member groups {}", Arrays.asList(groups));
            }
        } finally {
            memberGroupRWL.writeLock().unlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > addMembersGroupsMaxDuration) {
                    addMembersGroupsMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + addMembersGroupsMaxDuration + "]");
            }
        }
    }

    private volatile long removeMembersGroupsMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public void removeMemberGroups(String[] groups)
            throws NoSuchObjectException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        memberGroupRWL.writeLock().lock();
        try {
            ready.check();
            for (String group : groups) {
                int j = indexOf(memberGroups, group);
                if (j >= 0)
                    memberGroups = (String[]) arrayDel(memberGroups, j);
            }
            if (announcer != null) {
                synchronized (announcer) {
                    announcer.notify();
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("removed member groups {}", Arrays.asList(groups));
            }
        } finally {
            memberGroupRWL.writeLock().unlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > removeMembersGroupsMaxDuration) {
                    removeMembersGroupsMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + removeMembersGroupsMaxDuration + "]");
            }
        }
    }

    private volatile long getMembersGroupsMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public String[] getMemberGroups() throws NoSuchObjectException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        memberGroupRWL.readLock().lock();
        try {
            ready.check();
            /* no need to clone, never modified once created */
            return memberGroups;
        } finally {
            memberGroupRWL.readLock().unlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > getMembersGroupsMaxDuration) {
                    getMembersGroupsMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + getMembersGroupsMaxDuration + "]");
            }
        }
    }

    private volatile long setMembersGroupsMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public void setMemberGroups(String[] groups) throws NoSuchObjectException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        memberGroupRWL.writeLock().lock();
        try {
            ready.check();
            memberGroups = (String[]) removeDups(groups);
            if (announcer != null) {
                synchronized (announcer) {
                    announcer.notify();
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("set member groups {}", Arrays.asList(groups));
            }
        } finally {
            memberGroupRWL.writeLock().unlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > setMembersGroupsMaxDuration) {
                    setMembersGroupsMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + setMembersGroupsMaxDuration + "]");
            }
        }
    }

    private volatile long getUnicastPortMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public int getUnicastPort() throws NoSuchObjectException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        unicastPortRWL.readLock().lock();
        try {
            ready.check();
            return unicastPort;
        } finally {
            unicastPortRWL.readLock().unlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > getUnicastPortMaxDuration) {
                    getUnicastPortMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + getUnicastPortMaxDuration + "]");
            }
        }
    }

    private volatile long setUnicastPortMaxDuration = 0;

    // This method's javadoc is inherited from an interface of this class
    public void setUnicastPort(int port) throws IOException {
        long startTime = 0;
        if (loggerStats.isTraceEnabled()) {
            startTime = SystemTime.timeMillis();
        }
        unicastPortRWL.writeLock().lock();
        try {
            ready.check();
            if (port == unicastPort)
                return;
            if ((port == 0 && unicaster.port == Constants.getDiscoveryPort()) ||
                    port == unicaster.port) {
                unicastPort = port;
                return;
            }
            /* create a UnicastThread that listens on the new port */
            UnicastThread newUnicaster = new UnicastThread(host, port);
            /* terminate the current UnicastThread listening on the old port */
            unicaster.interrupt();
            try {
                unicaster.join();
            } catch (InterruptedException e) {
                // do nothing
            }
            /* start the UnicastThread listening on the new port */
            unicaster = newUnicaster;
            unicaster.start();
            unicastPort = port;
            myLocator = (proxy instanceof RemoteMethodControl) ?
                    new ConstrainableLookupLocator(
                            myLocator.getHost(), unicaster.port, null) :
                    new LookupLocator(myLocator.getHost(), unicaster.port);
            if (announcer != null) {
                synchronized (announcer) {
                    announcer.notify();
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("changed unicast discovery port to {}", unicaster.port);
            }
        } finally {
            unicastPortRWL.writeLock().unlock();
            if (loggerStats.isTraceEnabled()) {
                long duration = SystemTime.timeMillis() - startTime;
                if (duration > setUnicastPortMaxDuration) {
                    setUnicastPortMaxDuration = duration;
                }
                loggerStats.trace("DURATION [" + duration + "]\t\tMAX [" + setUnicastPortMaxDuration + "]");
            }
        }
    }

    // This method's javadoc is inherited from an interface of this class
    public void destroy() throws RemoteException {
        metricRegistrator.clear();
        metricManager.close();
        try {
            concurrentObj.priorityWriteLock();
        } catch (ConcurrentLockException e) {
            return;
        }
        try {
            ready.check();
            logger.info("starting Reggie shutdown");
            ready.shutdown();

            /*
              Termination thread code.  We do this in a separate thread to
              avoid deadlock, because ActivationGroup.inactive will block until
              in-progress RMI calls are finished.
             */
            Thread destroyThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    destroyImpl();
                }
            });
            destroyThread.setName("destroy");
            destroyThread.setDaemon(false);
            destroyThread.start();
        } finally {
            concurrentObj.writeUnlock();
        }
    }

    /**
     * Return a new array containing the elements of the given array plus the given element added to
     * the end.
     */
    private static Object[] arrayAdd(Object[] array, Object elt) {
        int len = array.length;
        Object[] narray =
                (Object[]) Array.newInstance(array.getClass().getComponentType(),
                        len + 1);
        System.arraycopy(array, 0, narray, 0, len);
        narray[len] = elt;
        return narray;
    }

    /**
     * Return a new array containing all the elements of the given array except the one at the
     * specified index.
     */
    private static Object[] arrayDel(Object[] array, int i) {
        int len = array.length - 1;
        Object[] narray =
                (Object[]) Array.newInstance(array.getClass().getComponentType(),
                        len);
        System.arraycopy(array, 0, narray, 0, i);
        System.arraycopy(array, i + 1, narray, i, len - i);
        return narray;
    }

    /**
     * Returns the first index of elt in the array, else -1.
     */
    private static int indexOf(Object[] array, Object elt) {
        return indexOf(array, array.length, elt);
    }

    /**
     * Returns the first index of elt in the array if < len, else -1.
     */
    private static int indexOf(Object[] array, int len, Object elt) {
        for (int i = 0; i < len; i++) {
            if (elt.equals(array[i]))
                return i;
        }
        return -1;
    }

    /**
     * Return true if the array is null or zero length
     */
    private static boolean isEmpty(Object[] array) {
        return (array == null || array.length == 0);
    }

    /**
     * Return true if some object is an element of both arrays
     */
    private static boolean overlap(Object[] arr1, Object[] arr2) {
        for (int i = arr1.length; --i >= 0; ) {
            if (indexOf(arr2, arr1[i]) >= 0)
                return true;
        }
        return false;
    }

    /**
     * Test if all elements of the array are null.
     */
    private static boolean allNull(Object[] array) {
        for (int i = array.length; --i >= 0; ) {
            if (array[i] != null)
                return false;
        }
        return true;
    }

    /**
     * Weed out duplicates.
     */
    private static Object[] removeDups(Object[] arr) {
        for (int i = arr.length; --i >= 0; ) {
            if (indexOf(arr, i, arr[i]) >= 0)
                arr = arrayDel(arr, i);
        }
        return arr;
    }

    /**
     * Delete item.attributeSets[i] and return the new array.
     */
    private static EntryRep[] deleteSet(Item item, int i) {
        item.attributeSets = (EntryRep[]) arrayDel(item.attributeSets, i);
        return item.attributeSets;
    }

    /**
     * Do a deep copy of the item, and substitute replacements for all embedded EntryClass instances
     * and null for the ServiceType and codebase (since they aren't needed on the client side).
     */
    private static Item copyItem(Item item) {
        item = (Item) item.clone();
        item.serviceType = null;
        item.codebase = null;
        EntryRep[] attrSets = item.attributeSets;
        for (int i = attrSets.length; --i >= 0; ) {
            attrSets[i].eclass = attrSets[i].eclass.getReplacement();
        }
        return item;
    }

    /**
     * Return the first (highest) class that defines the given field.  This would be a method on
     * EntryClass, but we want to minimize code downloaded into the client.
     */
    private static EntryClass getDefiningClass(EntryClass eclass, int fldidx) {
        while (true) {
            EntryClass sup = eclass.getSuperclass();
            if (sup.getNumFields() <= fldidx)
                return eclass;
            eclass = sup;
        }
    }

    /**
     * Adds a service registration to types in its hierarchy
     */
    private void addServiceByTypes(ServiceType type, SvcReg reg) {
        Map map = serviceByTypeName.get(type.getName());
        if (map == null) {
            map = new HashMap();
            serviceByTypeName.put(type.getName(), map);
        }
        map.put(reg.item.serviceID, reg);
        ServiceType[] ifaces = type.getInterfaces();
        for (int i = ifaces.length; --i >= 0; ) {
            addServiceByTypes(ifaces[i], reg);
        }
        ServiceType sup = type.getSuperclass();
        if (sup != null)
            addServiceByTypes(sup, reg);
    }

    /**
     * Deletes a service registration from types in its hierarchy
     */
    private void deleteServiceFromTypes(ServiceType type, SvcReg reg) {
        Map map = serviceByTypeName.get(type.getName());
        if (map != null) {
            map.remove(reg.item.serviceID);
            if ((map.isEmpty()) && !type.equals(objectServiceType))
                serviceByTypeName.remove(type.getName());
            ServiceType[] ifaces = type.getInterfaces();
            for (int j = ifaces.length; --j >= 0; ) {
                deleteServiceFromTypes(ifaces[j], reg);
            }
            ServiceType sup = type.getSuperclass();
            if (sup != null)
                deleteServiceFromTypes(sup, reg);
        }
    }

    /**
     * Test if an item matches a template.  This would be a method on Template, but we want to
     * minimize code downloaded into the client.
     */
    private static boolean matchItem(Template tmpl, Item item) {
        return ((tmpl.serviceID == null ||
                tmpl.serviceID.equals(item.serviceID)) &&
                matchType(tmpl.serviceTypes, item.serviceType) &&
                matchAttributes(tmpl, item));
    }

    /**
     * Test if a type is equal to or a subtype of every type in an array.
     */
    private static boolean matchType(ServiceType[] types, ServiceType type) {
        if (types != null) {
            for (int i = types.length; --i >= 0; ) {
                if (!types[i].isAssignableFrom(type))
                    return false;
            }
        }
        return true;
    }

    /**
     * Test if an entry matches a template.  This would be a method on EntryRep, but we want to
     * minimize code downloaded into the client.
     */
    private static boolean matchEntry(EntryRep tmpl, EntryRep entry) {
        if (!tmpl.eclass.isAssignableFrom(entry.eclass) ||
                tmpl.fields.length > entry.fields.length)
            return false;
        for (int i = tmpl.fields.length; --i >= 0; ) {
            if (tmpl.fields[i] != null &&
                    !tmpl.fields[i].equals(entry.fields[i]))
                return false;
        }
        return true;
    }

    /**
     * Test if there is at least one matching entry in the Item for each entry template in the
     * Template.
     */
    private static boolean matchAttributes(Template tmpl, Item item) {
        EntryRep[] tmpls = tmpl.attributeSetTemplates;
        if (tmpls != null) {
            EntryRep[] entries = item.attributeSets;
            outer:
            for (int i = tmpls.length; --i >= 0; ) {
                EntryRep etmpl = tmpls[i];
                for (int j = entries.length; --j >= 0; ) {
                    if (matchEntry(etmpl, entries[j]))
                        continue outer;
                }
                return false;
            }
        }
        return true;
    }

    /**
     * Test if an entry either doesn't match any template in an array, or matches a template but is
     * a subclass of the template type.
     */
    private static boolean attrMatch(EntryRep[] tmpls, EntryRep attrSet) {
        boolean good = true;
        if (tmpls != null) {
            for (int i = tmpls.length; --i >= 0; ) {
                EntryRep tmpl = tmpls[i];
                if (matchEntry(tmpl, attrSet)) {
                    if (tmpl.eclass.isAssignableFrom(attrSet.eclass) &&
                            !tmpl.eclass.equals(attrSet.eclass))
                        return true;
                    good = false;
                }
            }
        }
        return good;
    }

    /**
     * Test if the service has an entry of the given class or subclass with a field of the given
     * value.
     */
    private static boolean hasAttr(SvcReg reg, EntryClass eclass, int fldidx, Object value) {
        EntryRep[] sets = reg.item.attributeSets;
        for (int i = sets.length; --i >= 0; ) {
            EntryRep set = sets[i];
            if (eclass.isAssignableFrom(set.eclass) &&
                    ((value == null && set.fields[fldidx] == null) ||
                            (value != null && value.equals(set.fields[fldidx]))))
                return true;
        }
        return false;
    }

    /**
     * Test if the service has an entry of the exact given class (assumed to have no fields).
     */
    private static boolean hasEmptyAttr(SvcReg reg, EntryClass eclass) {
        EntryRep[] sets = reg.item.attributeSets;
        for (int i = sets.length; --i >= 0; ) {
            if (eclass.equals(sets[i].eclass))
                return true;
        }
        return false;
    }

    /**
     * Find the most specific types (of type) that don't match prefix and aren't equal to or a
     * supertype of any types in bases, and add them to types.
     */
    private static void addTypes(ArrayList<ServiceType> types, ArrayList<String> codebases, ServiceType[] bases,
                                 String prefix, ServiceType type, String codebase) {
        if (types.contains(type))
            return;
        if (bases != null) {
            for (int i = bases.length; --i >= 0; ) {
                if (type.isAssignableFrom(bases[i]))
                    return;
            }
        }
        if (prefix == null || type.getName().startsWith(prefix)) {
            types.add(type);
            codebases.add(codebase);
            return;
        }
        ServiceType[] ifs = type.getInterfaces();
        for (int i = ifs.length; --i >= 0; ) {
            addTypes(types, codebases, bases, prefix, ifs[i], codebase);
        }
        ServiceType sup = type.getSuperclass();
        if (sup != null)
            addTypes(types, codebases, bases, prefix, sup, codebase);
    }

    /**
     * Limit leaseDuration by limit, and check for negative value.
     */
    private static long limitDuration(long leaseDuration, long limit) {
        if (leaseDuration == Lease.ANY || leaseDuration > limit)
            leaseDuration = limit;
        else if (leaseDuration < 0)
            throw new IllegalArgumentException("negative lease duration");
        return leaseDuration;
    }

    /**
     * Returns new array containing locators from the given non-null array prepared using the given
     * proxy preparer.  If tolerateFailures is false, then any proxy preparation exception is
     * propagated to the caller. Otherwise, such exceptions are logged, and only successfully
     * prepared locators are included in the returned array.
     */
    private static LookupLocator[] prepareLocators(LookupLocator[] locators, ProxyPreparer preparer,
                                                   boolean tolerateFailures)
            throws RemoteException {
        List<LookupLocator> l = new ArrayList<LookupLocator>();
        for (LookupLocator locator : locators) {
            try {
                l.add((LookupLocator) preparer.prepareProxy(locator));
            } catch (Exception e) {
                if (!tolerateFailures) {
                    if (e instanceof RemoteException) {
                        throw (RemoteException) e;
                    }
                    throw (RuntimeException) e;
                }
                if (logger.isWarnEnabled()) {
                    LogUtils.throwing(LogLevel.WARNING, logger, GigaRegistrar.class, "prepareLocators", e, "failed to prepare lookup locator {}", locator);
                }
            }
        }
        return l.toArray(new LookupLocator[l.size()]);
    }

    /**
     * Add a service to our state.  This includes putting it in the serviceByID map under the
     * serviceID, in the serviceByTime map, in the serviceByType map under the service's
     * most-specific ServiceType, and in the serviceByAttr map under all of its attribute values,
     * incrementing the number of instances of each EntryClass, and updating entryClasses as
     * necessary.  If this is the first instance of that ServiceType registered, then we need to add
     * concrete class information to the type and all supertypes.
     */
    private void addService(SvcReg reg) {
        items.inc();
        synchronized (reg) {
            serviceByID.put(reg.item.serviceID, reg);
            if (loggerServiceExpire.isDebugEnabled()) {
                loggerServiceExpire.debug("[" + System.identityHashCode(reg) + "]: Service [" + reg.item.serviceID + "]: Registered with lease [" + reg.leaseID + "/" + reg.leaseExpiration + "], type [" + reg.item.serviceType.getName());
            }
            serviceByTime.put(new SvcRegExpirationKey(reg, reg.leaseExpiration), reg);
            addServiceByTypes(reg.item.serviceType, reg);
            EntryRep[] entries = reg.item.attributeSets;
            for (int i = entries.length; --i >= 0; ) {
                addAttrs(reg, entries[i]);
            }
            refreshServiceFromLookupCache(reg.item, false);
            computeMaxLeases();
        }
    }

    /**
     * Delete given service, generating events as necessary.  This includes deleting from the
     * serviceByID, serviceByTime, serviceByType, and serviceByAttr maps, decrementing the number of
     * instances of each EntryClass, and updating entryClasses as necessary.  If this is the last
     * registered instance of the service type, then we delete the concrete class information from
     * the type and all supertypes.
     */
    private void deleteService(SvcRegExpirationKey regExpKey, SvcReg reg, long now) {
        items.dec();
        synchronized (reg) {
            Item item = reg.item;
            generateEvents(item, null, now);
            serviceByID.remove(item.serviceID);
            if (loggerServiceExpire.isDebugEnabled()) {
                loggerServiceExpire.debug("[" + System.identityHashCode(reg) + "]: Service [" + reg.item.serviceID + "]: Deleting with lease [" + reg.leaseID + "/" + reg.leaseExpiration + "]");
            }
            if (regExpKey != null) {
                serviceByTime.remove(regExpKey);
            } else {
                serviceByTime.remove(new SvcRegExpirationKey(reg, reg.leaseExpiration));
            }
            deleteServiceFromTypes(item.serviceType, reg);
            EntryRep[] entries = item.attributeSets;
            for (int i = entries.length; --i >= 0; ) {
                deleteAttrs(reg, entries[i], false);
            }
            refreshServiceFromLookupCache(item, true);
            computeMaxLeases();
        }
    }

    private static final String CLUSTED_PROXY_CLASSNAME = SpaceProxyImpl.class.getName();

    private static final String CONTAINER_PROXY_CLASSNAME = "com.j_spaces.core.JSpaceContainerProxy";

    private static final String SERVICE_CLASSNAME = Service.class.getName();

    private static ServiceType SERVICE_SERVICE_TYPE;

    static {
        try {
            SERVICE_SERVICE_TYPE = ClassMapper.toServiceType(Service.class);
        } catch (MarshalException e) {
            logger.error("Failed to convert", e);
        }
    }

    /**
     * Refresh an item from the concurrent local cache (key is the temaplte, value is the Item). If
     * not delete, will also add several built in templates that will cause for higher hit ratio
     * (NOTE: this depends on the actual structure here...).
     */
    private void refreshServiceFromLookupCache(Item item, boolean delete) {
        if (loggerCache.isTraceEnabled()) {
            if (delete) {
                loggerCache.trace("Delete item: " + item.serviceID + ", " + item.serviceType + ", " + asList(item.attributeSets));
            } else {
                loggerCache.trace("Refresh item: " + item.serviceID + ", " + item.serviceType + ", " + asList(item.attributeSets));
            }
        }
        for (ConcurrentHashMap<ServiceID, Item> items : lookupServiceCache.values()) {
            Item oldItem = items.remove(item.serviceID);
            if (oldItem != null) {
                if (loggerCache.isTraceEnabled()) {
                    loggerCache.trace("Removing from cache template (explicitly): " + item.serviceID + ", " + item.serviceType);
                }
            }
        }

        // adding matching tempaltes. We rely on the fact that they are ordered by short class name

        if (item.serviceType.getName().equals(CLUSTED_PROXY_CLASSNAME)) {
            // Adding Another possible template
            EntryRep[] attrsSetTemplate = new EntryRep[3];
            attrsSetTemplate[0] = item.attributeSets[3]; // Container Name
            attrsSetTemplate[1] = item.attributeSets[0]; // Name
            // handle state, clone and null the first two fields
            attrsSetTemplate[2] = (EntryRep) item.attributeSets[6].clone(); // State
            attrsSetTemplate[2].fields[0] = null;
            attrsSetTemplate[2].fields[1] = null;
            Template tmpl = new Template(null, new ServiceType[]{SERVICE_SERVICE_TYPE}, attrsSetTemplate);
            deleteOrReplaceTemplate(item, delete, tmpl);

            // Adding Another possible template
            attrsSetTemplate = new EntryRep[2];
            attrsSetTemplate[0] = item.attributeSets[3]; // Container Name
            attrsSetTemplate[1] = item.attributeSets[0]; // Name
            tmpl = new Template(null, new ServiceType[]{SERVICE_SERVICE_TYPE}, attrsSetTemplate);
            deleteOrReplaceTemplate(item, delete, tmpl);

            // Adding Another possible template
            attrsSetTemplate = new EntryRep[2];
            // handle State, clone and null the first two fields
            attrsSetTemplate[0] = item.attributeSets[0]; // Name
            attrsSetTemplate[1] = (EntryRep) item.attributeSets[6].clone(); // State
            attrsSetTemplate[1].fields[0] = null;
            attrsSetTemplate[1].fields[1] = null;
            tmpl = new Template(null, new ServiceType[]{SERVICE_SERVICE_TYPE}, attrsSetTemplate);
            deleteOrReplaceTemplate(item, delete, tmpl);

            // another one for ClusterName and State
            attrsSetTemplate = new EntryRep[2];
            // handle State, clone and null the first two fields
            attrsSetTemplate[0] = item.attributeSets[1]; // ClusterName
            attrsSetTemplate[1] = (EntryRep) item.attributeSets[6].clone(); // State
            attrsSetTemplate[1].fields[0] = null;
            attrsSetTemplate[1].fields[1] = null;
            tmpl = new Template(null, new ServiceType[]{SERVICE_SERVICE_TYPE}, attrsSetTemplate);
            deleteOrReplaceTemplate(item, delete, tmpl);

            // another one for ClusterName and State
            attrsSetTemplate = new EntryRep[3];
            // handle State, clone and null the first two fields
            attrsSetTemplate[0] = (EntryRep) item.attributeSets[2].clone(); // ClusterGroup
            attrsSetTemplate[0].fields[0] = null;
            attrsSetTemplate[0].fields[1] = null;
            attrsSetTemplate[1] = item.attributeSets[1]; // ClusterName
            attrsSetTemplate[2] = (EntryRep) item.attributeSets[6].clone(); // State
            attrsSetTemplate[2].fields[0] = null;
            attrsSetTemplate[2].fields[1] = null;
            tmpl = new Template(null, new ServiceType[]{SERVICE_SERVICE_TYPE}, attrsSetTemplate);
            deleteOrReplaceTemplate(item, delete, tmpl);


            // Adding Another possible template(state=replicable)
            attrsSetTemplate = new EntryRep[3];
            attrsSetTemplate[0] = item.attributeSets[3]; // Container Name
            attrsSetTemplate[1] = item.attributeSets[0]; // Name
            // handle state, clone and null the first two fields
            attrsSetTemplate[2] = (EntryRep) item.attributeSets[6].clone(); // State
            attrsSetTemplate[2].fields[0] = null;//electable
            attrsSetTemplate[2].fields[2] = null;//state
            tmpl = new Template(null, new ServiceType[]{SERVICE_SERVICE_TYPE}, attrsSetTemplate);
            deleteOrReplaceTemplate(item, delete, tmpl);

            // Adding Another possible template(state=replicable)
            attrsSetTemplate = new EntryRep[2];
            // handle State, clone and null the first two fields
            attrsSetTemplate[0] = item.attributeSets[0]; // Name
            attrsSetTemplate[1] = (EntryRep) item.attributeSets[6].clone(); // State
            attrsSetTemplate[1].fields[0] = null;
            attrsSetTemplate[1].fields[2] = null;
            tmpl = new Template(null, new ServiceType[]{SERVICE_SERVICE_TYPE}, attrsSetTemplate);
            deleteOrReplaceTemplate(item, delete, tmpl);


            // Yet Another possible templates (for active election)
            if (item.attributeSets.length > 7) {
                attrsSetTemplate = new EntryRep[1];
                attrsSetTemplate[0] = item.attributeSets[7]; // ActiveElectionState
                tmpl = new Template(item.serviceID, new ServiceType[]{SERVICE_SERVICE_TYPE}, attrsSetTemplate);
                deleteOrReplaceTemplate(item, delete, tmpl);

                attrsSetTemplate = new EntryRep[4];
                attrsSetTemplate[0] = item.attributeSets[7]; // ActiveElectionState
                attrsSetTemplate[1] = item.attributeSets[2]; // ClusterGroup
                attrsSetTemplate[2] = item.attributeSets[1]; // ClusterName
                // handle State, clone and null the first two fields (matching is done on first field)
                attrsSetTemplate[3] = (EntryRep) item.attributeSets[6].clone(); // State
                attrsSetTemplate[3].fields[1] = null; //electionPriority
                attrsSetTemplate[3].fields[2] = null;
                tmpl = new Template(null, new ServiceType[]{SERVICE_SERVICE_TYPE}, attrsSetTemplate);
                deleteOrReplaceTemplate(item, delete, tmpl);

                attrsSetTemplate = new EntryRep[4];
                attrsSetTemplate[0] = item.attributeSets[7]; // ActiveElectionState
                attrsSetTemplate[1] = item.attributeSets[3]; // ContainerName
                attrsSetTemplate[2] = item.attributeSets[0]; // Name
                // matching is done on State (started)
                attrsSetTemplate[3] = (EntryRep) item.attributeSets[6].clone(); // State
                attrsSetTemplate[3].fields[0] = null;
                attrsSetTemplate[3].fields[1] = null;
                tmpl = new Template(null, new ServiceType[]{SERVICE_SERVICE_TYPE}, attrsSetTemplate);
                deleteOrReplaceTemplate(item, delete, tmpl);
            }
        } else if (item.serviceType.getName().equals(CONTAINER_PROXY_CLASSNAME)) {
            // Adding Another possible template
            EntryRep[] attrsSetTemplate = new EntryRep[2];
            attrsSetTemplate[0] = item.attributeSets[1]; // Conntainer Name
            attrsSetTemplate[1] = item.attributeSets[0]; // Name
            // handle state, clone and null the first two fields
            Template tmpl = new Template(null, new ServiceType[]{SERVICE_SERVICE_TYPE}, attrsSetTemplate);
            deleteOrReplaceTemplate(item, delete, tmpl);
        }
    }

    /**
     * If delete is marked, then tries to perform another lookup (should be very fast) and find if
     * something matches it. If so, it replaces the cache with the one that it found. If none is
     * found, then nothing is added to the cache. <p/> <p>In case the service is not deleted, then
     * just adds it to the cache.
     */
    private void deleteOrReplaceTemplate(Item item, boolean delete, Template tmpl) {
        ConcurrentHashMap<ServiceID, Item> items = lookupServiceCache.get(tmpl);
        if (delete) {
            if (items != null) {
                items.remove(item.serviceID);
                if (items.isEmpty()) {
                    lookupServiceCache.remove(tmpl);
                }
                if (loggerCache.isTraceEnabled()) {
                    loggerCache.trace("Removing cache template (explicitly): " + tmpl
                            + "\n   ---- > id: " + item.serviceID + " , type: " + item.serviceType + " , items left: " + items.size());
                }
            }
        } else {
            // here, we are under a writeLock, so no need to worry about concurrency in the
            // creation of the concurrent hash map
            if (items == null) {
                items = new SizeConcurrentHashMap<ServiceID, Item>(1);
                lookupServiceCache.put(tmpl, items);
            }
            items.put(item.serviceID, item);
            if (loggerCache.isTraceEnabled()) {
                loggerCache.trace("Adding to cache template (explicitly): " + tmpl
                        + "\n   ---- > " + item.serviceID + " " + item.serviceType);
            }
        }
    }

    /**
     * Add an event registration to our state.  This includes adding a template of each EntryClass,
     * putting the registration in the eventByID map, in the eventByTime map, and in either
     * subEventByService (if the template is for a specific service id) or subEventByID.  Since we
     * expect in most cases there will only ever be a s`ingle event registration for a given service
     * id, we avoid creating a singleton array in that case.
     */
    private void addEvent(EventReg reg) {
        synchronized (reg) {
            if (reg.listener == null)
                return; /* failed to recover from log */

            EntryRep[] tmpls = reg.tmpl.attributeSetTemplates;
            if (tmpls != null) {
                for (int i = tmpls.length; --i >= 0; ) {
                    EntryClass eclass = tmpls[i].eclass;
                    eclass.setNumTemplates(eclass.getNumTemplates() + 1);
                }
            }
            Long id = reg.eventID;
            listeners.inc();
            eventByID.put(id, reg);
            EventRegKeyExpiration regExpirationKey = new EventRegKeyExpiration(reg, reg.leaseExpiration);
            if (loggerEventExpire.isDebugEnabled()) {
                loggerEventExpire.debug("[" + System.identityHashCode(regExpirationKey) + "]: Event [" + regExpirationKey.reg.eventID + "]: Registered with lease [" + regExpirationKey.leaseExpiration + "] and listener [" + reg.listener);
            }
            eventByTime.put(regExpirationKey, reg);
            if (reg.tmpl.serviceID != null) {
                Object val = subEventByService.get(reg.tmpl.serviceID);
                if (val == null)
                    val = reg;
                else if (val instanceof EventReg)
                    val = new EventReg[]{(EventReg) val, reg};
                else
                    val = arrayAdd((EventReg[]) val, reg);
                subEventByService.put(reg.tmpl.serviceID, val);
            } else {
                subEventByID.put(id, reg);
            }
            computeMaxLeases();
        }
    }

    /**
     * Remove an event registration from our state.  This includes deleting a template of each
     * EntryClass, deleting the registration from the eventByID map, the eventByTime map, and either
     * the subEventByService or subEventByID map.
     */
    private void deleteEvent(EventRegKeyExpiration regExpiration, EventReg reg) {
        synchronized (reg) {
            EntryRep[] tmpls = reg.tmpl.attributeSetTemplates;
            if (tmpls != null) {
                for (int i = tmpls.length; --i >= 0; ) {
                    EntryClass eclass = tmpls[i].eclass;
                    eclass.setNumTemplates(eclass.getNumTemplates() - 1);
                }
            }
            listeners.dec();
            Long id = reg.eventID;
            eventByID.remove(id);

            List<RegistrarEvent> removedEvents = eventsQ.remove(reg);
            List<RegistrarEvent> removedPending = pendingQ.remove(reg);

            if (removedEvents != null) {
                pendingEvents.dec(removedEvents.size());
                removedEvents.clear();
            }
            if (removedPending != null) {
                removedPending.clear();
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Removed event registration for: " + reg);
            }

            if (regExpiration != null) {
                eventByTime.remove(regExpiration);
            } else {
                eventByTime.remove(new EventRegKeyExpiration(reg, reg.leaseExpiration));
            }
            if (reg.tmpl.serviceID != null) {
                Object val = subEventByService.get(reg.tmpl.serviceID);
                if (val == reg) {
                    subEventByService.remove(reg.tmpl.serviceID);
                } else {
                    Object[] array = (EventReg[]) val;
                    array = arrayDel(array, indexOf(array, reg));
                    if (array.length == 1)
                        val = array[0];
                    else
                        val = array;
                    subEventByService.put(reg.tmpl.serviceID, val);
                }
            } else {
                subEventByID.remove(id);
            }
            computeMaxLeases();
        }
    }

    /**
     * Put the service in the serviceByAttr map under all attribute values in the given entry, or in
     * the serviceByEmptyAttr map if the entry has no attributes, add a new instance of the
     * EntryClass, and update entryClasses as necessary.
     */
    private void addAttrs(SvcReg reg, EntryRep entry) {
        EntryClass eclass = entry.eclass;
        addInstance(eclass);
        Object[] fields = entry.fields;
        if (fields.length > 0) {
            /* walk backwards to make getDefiningClass more efficient */
            for (int i = fields.length; --i >= 0; ) {
                eclass = getDefiningClass(eclass, i);
                addAttr(reg, eclass, i, fields[i]);
            }
            return;
        }
        ArrayList regs = serviceByEmptyAttr.get(eclass);
        if (regs == null) {
            regs = new ArrayList(2);
            regs.add(reg);
            serviceByEmptyAttr.put(eclass, regs);
        } else if (!regs.contains(reg)) {
            regs.add(reg);
        }
    }

    /**
     * If checkDups is false, delete the service (if present) from serviceByAttr under all attribute
     * values of the given entry or from serviceByEmptyAttr if the entry has no attributes. If
     * checkDups is true, only delete for a given attribute value if the service has no other
     * entries of similar type that still have the same value.  Either way, delete an instance of
     * the EntryClass, and update entryClasses as necessary.
     */
    private void deleteAttrs(SvcReg reg, EntryRep entry, boolean checkDups) {
        EntryClass eclass = entry.eclass;
        deleteInstance(eclass);
        Object[] fields = entry.fields;
        if (fields.length == 0) {
            ArrayList regs = serviceByEmptyAttr.get(eclass);
            if (regs == null || (checkDups && hasEmptyAttr(reg, eclass)))
                return;
            int idx = regs.indexOf(reg);
            if (idx >= 0) {
                regs.remove(idx);
                if (regs.isEmpty())
                    serviceByEmptyAttr.remove(eclass);
            }
            return;
        }
        /* walk backwards to make getDefiningClass more efficient */
        for (int fldidx = fields.length; --fldidx >= 0; ) {
            eclass = getDefiningClass(eclass, fldidx);
            HashMap[] attrMaps = serviceByAttr.get(eclass);
            if (attrMaps == null ||
                    attrMaps[fldidx] == null ||
                    (checkDups && hasAttr(reg, eclass, fldidx, fields[fldidx])))
                continue;
            HashMap map = attrMaps[fldidx];
            Object value = fields[fldidx];
            ArrayList regs = (ArrayList) map.get(value);
            if (regs == null)
                continue;
            int idx = regs.indexOf(reg);
            if (idx < 0)
                continue;
            regs.remove(idx);
            if (!regs.isEmpty())
                continue;
            map.remove(value);
            if (!map.isEmpty())
                continue;
            attrMaps[fldidx] = null;
            if (allNull(attrMaps))
                serviceByAttr.remove(eclass);
        }
    }

    /**
     * Store all non-null elements of values into the given entry, and update serviceByAttr to
     * match.
     */
    private void updateAttrs(SvcReg reg, EntryRep entry, Object[] values) {
        EntryClass eclass = entry.eclass;
        /* walk backwards to make getDefiningClass more efficient */
        for (int fldidx = values.length; --fldidx >= 0; ) {
            Object oval = entry.fields[fldidx];
            Object nval = values[fldidx];
            if (nval != null && !nval.equals(oval)) {
                eclass = getDefiningClass(eclass, fldidx);
                HashMap map = addAttr(reg, eclass, fldidx, nval);
                entry.fields[fldidx] = nval;
                if (hasAttr(reg, eclass, fldidx, oval))
                    continue;
                ArrayList regs = (ArrayList) map.get(oval);
                regs.remove(regs.indexOf(reg));
                if (regs.isEmpty())
                    map.remove(oval); /* map cannot become empty */
            }
        }
    }

    /**
     * Put the service under the given attribute value for the given defining class and field, if it
     * isn't already there.  Return the HashMap for the given class and field.
     */
    private HashMap addAttr(SvcReg reg, EntryClass eclass, int fldidx, Object value) {
        HashMap[] attrMaps = serviceByAttr.get(eclass);
        if (attrMaps == null) {
            attrMaps = new HashMap[eclass.getNumFields()];
            serviceByAttr.put(eclass, attrMaps);
        }
        HashMap map = attrMaps[fldidx];
        if (map == null) {
            map = new HashMap(11);
            attrMaps[fldidx] = map;
        }
        ArrayList regs = (ArrayList) map.get(value);
        if (regs == null) {
            regs = new ArrayList(3);
            map.put(value, regs);
        } else if (regs.contains(reg))
            return map;
        regs.add(reg);
        return map;
    }

    /**
     * Add an instance of the EntryClass, and add the class to entryClasses if this is the first
     * such instance.
     */
    private void addInstance(EntryClass eclass) {
        int idx = entryClasses.indexOf(eclass);
        if (idx < 0) {
            entryClasses.add(eclass);
            idx = entryClasses.size() - 1;
        }
        eclass = entryClasses.get(idx);
        eclass.setNumInstances(eclass.getNumInstances() + 1);
    }

    /**
     * Delete an instance of the EntryClass, and remove the class from entryClasses if this is the
     * last such instance.
     */
    private void deleteInstance(EntryClass eclass) {
        int idx = entryClasses.indexOf(eclass);
        if (idx < 0) {
            return;
        }
        eclass = entryClasses.get(idx);
        int num = eclass.getNumInstances() - 1;
        if (num == 0)
            entryClasses.remove(idx);
        eclass.setNumInstances(num);
    }

    /**
     * Return an appropriate iterator for Items matching the Template.
     */
    private ItemIter matchingItems(Template tmpl) {
        if (tmpl.serviceID != null)
            return new IDItemIter(tmpl);
        if (!isEmpty(tmpl.serviceTypes))
            return new SvcIterator(tmpl);
        EntryRep[] sets = tmpl.attributeSetTemplates;
        if (isEmpty(sets))
            return new AllItemIter();
        for (int i = sets.length; --i >= 0; ) {
            Object[] fields = sets[i].fields;
            if (fields.length == 0) {
                EntryClass eclass = getEmptyEntryClass(sets[i].eclass);
                if (eclass != null)
                    return new EmptyAttrItemIter(tmpl, eclass);
            } else {
                /* try subclass fields before superclass fields */
                for (int j = fields.length; --j >= 0; ) {
                    if (fields[j] != null)
                        return new AttrItemIter(tmpl, i, j);
                }
            }
        }
        return new ClassItemIter(tmpl);
    }

    /**
     * Return member of entryClasses that is equal to or a subclass of the specified class, provided
     * there is exactly one such member and it has no fields.
     */
    private EntryClass getEmptyEntryClass(EntryClass eclass) {
        EntryClass match = null;
        for (int i = entryClasses.size(); --i >= 0; ) {
            EntryClass cand = entryClasses.get(i);
            if (eclass.isAssignableFrom(cand)) {
                if (cand.getNumFields() != 0 || match != null)
                    return null;
                match = cand;
            }
        }
        return match;
    }

    /**
     * Returns a list of services that match all types passed in
     */
    private ArrayList matchingServices(ServiceType[] types) {
        ArrayList matches = new ArrayList();
        if (isEmpty(types)) {
            Map map = serviceByTypeName.get(objectServiceType.getName());
            matches.addAll(map.values());
        } else {
            Map map = serviceByTypeName.get(types[0].getName());
            if (map != null)
                matches.addAll(map.values());
            if (types.length > 1) {
                for (Iterator it = matches.iterator(); it.hasNext(); ) {
                    SvcReg reg = (SvcReg) it.next();
                    if (!matchType(types, reg.item.serviceType))
                        it.remove();
                }
            }
        }
        return matches;
    }

    /**
     * Return any valid codebase for an entry class that has instances.
     */
    private String pickCodebase(EntryClass eclass, long now) throws ClassNotFoundException {
        if (eclass.getNumFields() == 0)
            return pickCodebase(eclass,
                    serviceByEmptyAttr.get(eclass),
                    now);
        int fldidx = eclass.getNumFields() - 1;
        HashMap[] attrMaps =
                serviceByAttr.get(getDefiningClass(eclass, fldidx));
        for (Object o : attrMaps[fldidx].values()) {
            try {
                return pickCodebase(eclass, (ArrayList) o, now);
            } catch (ClassNotFoundException ignored) {
                // do nothing
            }
        }
        throw new ClassNotFoundException();
    }

    /**
     * Return any valid codebase for an entry of the exact given class.
     */
    private String pickCodebase(EntryClass eclass, ArrayList svcs, long now) throws ClassNotFoundException {
        for (int i = svcs.size(); --i >= 0; ) {
            SvcReg reg = (SvcReg) svcs.get(i);
            if (reg.leaseExpiration <= now)
                continue;
            EntryRep[] sets = reg.item.attributeSets;
            for (int j = sets.length; --j >= 0; ) {
                if (eclass.equals(sets[j].eclass))
                    return sets[j].codebase;
            }
        }
        throw new ClassNotFoundException();
    }

    /**
     * Compute new maxServiceLease and maxEventLease values.  This needs to be called whenever the
     * number of services (#S) or number of events (#E) changes, or whenever any of the
     * configuration parameters change. The two base equations driving the computation are:
     * #S/maxServiceLease + #E/maxEventLease <= 1/minRenewalInterval maxServiceLease/maxEventLease =
     * minMaxServiceLease/minMaxEventLease
     */
    private void computeMaxLeases() {
        maxServiceLease =
                Math.max(minMaxServiceLease,
                        minRenewalInterval *
                                (serviceByID.size() +
                                        ((eventByID.size() * minMaxServiceLease) /
                                                minMaxEventLease)));
        maxEventLease = Math.max(minMaxEventLease,
                ((maxServiceLease * minMaxEventLease) /
                        minMaxServiceLease));
    }

    /**
     * Process a unicast discovery request, and respond.
     */
    private void respond(Socket socket) throws Exception {
        try {
            try {
                if (Constants.useSocketTcpNoDelay()) {
                    socket.setTcpNoDelay(true);
                }
                if (Constants.useSocketKeepAlive()) {
                    socket.setKeepAlive(true);
                }
            } catch (SocketException e) {
                if (logger.isDebugEnabled())
                    logger.debug(
                            "problem setting socket options", e);
            }
            socket.setSoTimeout(
                    unicastDiscoveryConstraints.getUnicastSocketTimeout(
                            DEFAULT_SOCKET_TIMEOUT));
            int pv = new DataInputStream(socket.getInputStream()).readInt();
            unicastDiscoveryConstraints.checkProtocolVersion(pv);
            getDiscovery(pv).handleUnicastDiscovery(
                    new UnicastResponse(myLocator.getHost(),
                            myLocator.getPort(),
                            memberGroups,
                            proxy, proxyMarshalledObject, proxyRegistrarBytes),
                    socket,
                    unicastDiscoveryConstraints.getUnfulfilledConstraints(),
                    unicastDiscoverySubjectChecker,
                    Collections.EMPTY_LIST);
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                logger.debug("exception closing socket", e);
            }
        }
    }

    /**
     * Returns Discovery instance implementing the given protocol version
     */
    private Discovery getDiscovery(int version) throws DiscoveryProtocolException {
        switch (version) {
            case Discovery.PROTOCOL_VERSION_1:
                return Discovery.getProtocol1();
            case Discovery.PROTOCOL_VERSION_2:
                return protocol2;
            default:
                throw new DiscoveryProtocolException(
                        "unsupported protocol version: " + version);
        }
    }

    /**
     * Close any sockets that were sitting in the task queue.
     */
    private void closeRequestSockets(List tasks) {
        for (int i = tasks.size(); --i >= 0; ) {
            Object obj = tasks.get(i);
            if (obj instanceof SocketTask) {
                try {
                    ((SocketTask) obj).socket.close();
                } catch (IOException e) {
                    // do nothing
                }
            }
        }
    }

    /**
     * Post-login (if login configured) initialization.
     */
    private void init(Configuration config, LifeCycle lifeCycle) throws IOException, ConfigurationException, ActivationException {
        final long startTime = System.currentTimeMillis();

        RuntimeInfo.logRuntimeInfo(logger, "Starting Lookup Service...");

        if (myServiceID == null) {
            myServiceID = newServiceID();
        }

        this.lifeCycle = lifeCycle;

        serverExporter = (Exporter) Config.getNonNullEntry(
                config, COMPONENT, "serverExporter", Exporter.class);

        /* fetch "initial*" config entries, if first time starting up */
        if (!recoveredSnapshot) {

            List<Entry> defaultEntries = loadInitialLookupAttributes();
            Entry[] defaultValue = defaultEntries.toArray(new Entry[defaultEntries.size()]);

            Entry[] initialLookupAttributes = (Entry[]) config.getEntry(
                    COMPONENT, "initialLookupAttributes", Entry[].class,
                    defaultValue);
            lookupGroups = (String[]) config.getEntry(
                    COMPONENT, "initialLookupGroups", String[].class,
                    lookupGroups);
            lookupLocators = (LookupLocator[]) config.getEntry(
                    COMPONENT, "initialLookupLocators", LookupLocator[].class,
                    lookupLocators);
            memberGroups = (String[]) config.getEntry(
                    COMPONENT, "initialMemberGroups", String[].class,
                    memberGroups);
            if (memberGroups == null) {
                throw new ConfigurationException(
                        "member groups cannot be ALL_GROUPS (null)");
            }
            memberGroups = (String[]) removeDups(memberGroups);
            unicastPort = Config.getIntEntry(
                    config, COMPONENT, "initialUnicastDiscoveryPort",
                    Integer.getInteger("com.sun.jini.reggie.initialUnicastDiscoveryPort", unicastPort), 0, 0xFFFF);

            if (initialLookupAttributes != null &&
                    initialLookupAttributes.length > 0) {
                List<Entry> l = new ArrayList<Entry>(Arrays.asList(baseAttrs));
                l.addAll(Arrays.asList(initialLookupAttributes));
                lookupAttrs = l.toArray(new Entry[l.size()]);
            } else {
                lookupAttrs = baseAttrs;
            }
        }

        /* fetch remaining config entries */
        MethodConstraints discoveryConstraints =
                (MethodConstraints) config.getEntry(COMPONENT,
                        "discoveryConstraints",
                        MethodConstraints.class, null);
        if (discoveryConstraints == null) {
            discoveryConstraints =
                    new BasicMethodConstraints(InvocationConstraints.EMPTY);
        }
        try {
            discoer = (DiscoveryManagement) config.getEntry(
                    COMPONENT, "discoveryManager", DiscoveryManagement.class);
        } catch (NoSuchEntryException e) {
            discoer = new LookupDiscoveryManager(LookupGroups.none(),
                    null,
                    null,
                    config,
                    true /* runningWithinRegistrar */);
        }
        listenerPreparer = (ProxyPreparer) Config.getNonNullEntry(
                config, COMPONENT, "listenerPreparer", ProxyPreparer.class,
                listenerPreparer);
        locatorPreparer = (ProxyPreparer) Config.getNonNullEntry(
                config, COMPONENT, "locatorPreparer", ProxyPreparer.class,
                locatorPreparer);
        minMaxEventLease = Config.getLongEntry(
                config, COMPONENT, "minMaxEventLease",
                minMaxEventLease, 1, MAX_LEASE);
        minMaxServiceLease = Config.getLongEntry(
                config, COMPONENT, "minMaxServiceLease",
                minMaxServiceLease, 1, MAX_LEASE);
        minRenewalInterval = Config.getLongEntry(
                config, COMPONENT, "minRenewalInterval",
                minRenewalInterval, 0, MAX_RENEW);
        multicastAnnouncementInterval = Config.getLongEntry(
                config, COMPONENT, "multicastAnnouncementInterval",
                multicastAnnouncementInterval, 1, Long.MAX_VALUE);

        multicastInterfaceRetryInterval = Config.getIntEntry(
                config, COMPONENT, "multicastInterfaceRetryInterval",
                multicastInterfaceRetryInterval, 1, Integer.MAX_VALUE);
        try {
            multicastInterfaces = (NetworkInterface[]) config.getEntry(
                    COMPONENT, "multicastInterfaces", NetworkInterface[].class);
            multicastInterfacesSpecified = true;
        } catch (NoSuchEntryException e) {
            multicastInterfaces = BootUtil.getNetworkInterfaces();
            multicastInterfacesSpecified = false;
        }
        if (multicastInterfaces == null) {
            logger.debug("using system default interface for multicast");
        } else if (multicastInterfaces.length == 0) {
            if (multicastInterfacesSpecified) {
                logger.debug("multicast disabled");
            } else {
                logger.error("no network interfaces detected");
            }
        } else if (logger.isDebugEnabled()) {
            logger.debug("multicasting on interfaces {}", Arrays.asList(multicastInterfaces));
        }

        try {
            multicastRequestSubjectChecker =
                    (ClientSubjectChecker) Config.getNonNullEntry(
                            config, COMPONENT, "multicastRequestSubjectChecker",
                            ClientSubjectChecker.class);
        } catch (NoSuchEntryException e) {
            // leave null
        }
        resourceIdGenerator = (UuidGenerator) Config.getNonNullEntry(
                config, COMPONENT, "resourceIdGenerator", UuidGenerator.class,
                resourceIdGenerator);
        serviceIdGenerator = (UuidGenerator) Config.getNonNullEntry(
                config, COMPONENT, "serviceIdGenerator", UuidGenerator.class,
                serviceIdGenerator);

        useCacheForSpaceMiss = (Boolean) Config.getNonNullEntry(config, COMPONENT, "useCacheForSpaceMiss", Boolean.class, Boolean.TRUE);

        taskerComm = DynamicExecutors.newBlockingThreadPool(
                Integer.getInteger(COMPONENT + ".commTaskManager.minThreads", 10),
                Integer.getInteger(COMPONENT + ".commTaskManager.maxThreads", 80),
                Long.getLong(COMPONENT + ".commTaskManager.keepAlive", 10 * 1000),
                Integer.getInteger(COMPONENT + ".commTaskManager.capacity", 200),
                Long.getLong(COMPONENT + ".commTaskManager.waitTime", 3 * 60 * 1000),
                Thread.NORM_PRIORITY + 2, "Reggie Comm Task", true);

        // TaskManager is configurable using system properties. e.g. -Dcom.sun.jini.reggie.eventTaskManager.maxThreads=4
        taskerEvent = (TaskManager) Config.getNonNullEntry(config, COMPONENT, "eventTaskManager", TaskManager.class,
                new TaskManager(Integer.getInteger(COMPONENT + ".eventTaskManager.maxThreads", 40),
                        Long.getLong(COMPONENT + ".eventTaskManager.timeout",15000),
                        2.0F, "Reggie Event Task",
                        Integer.getInteger(COMPONENT + ".eventTaskManager.retriesOnIdle",10)));

        // maxEventsPerEventReg is configurable by setting the system property: -Dcom.sun.jini.reggie.maxEventsPerEventReg=1500
        // This default value represents an approximation of 52.5 MB = (70 bytes per RegistrarEvent) x 500 listeners x maxEventsPerEventReg
        maxEventsPerEventReg = Config.getIntEntry(
                config, COMPONENT, "maxEventsPerEventReg", 1500,10, Integer.MAX_VALUE);


        pendingQ = new HashMap<EventReg, List<RegistrarEvent>>();
        eventsQ = new ConcurrentHashMap<EventReg, List<RegistrarEvent>>();

        unexportTimeout = Config.getLongEntry(
                config, COMPONENT, "unexportTimeout", unexportTimeout,
                0, Long.MAX_VALUE);
        unexportWait = Config.getLongEntry(
                config, COMPONENT, "unexportWait", unexportWait,
                0, Long.MAX_VALUE);

        //configure default for unicast discovery host
        String defaultValue = SystemInfo.singleton().network().getPublicHostId();

        String unicastDiscoveryHost;
        try {
            unicastDiscoveryHost = (String) Config.getNonNullEntry(
                    config, COMPONENT, "unicastDiscoveryHost", String.class, defaultValue);
        } catch (NoSuchEntryException e) {
            // fix for 4906732: only invoke getCanonicalHostName if needed
            unicastDiscoveryHost = defaultValue;
        }
        try {
            unicastDiscoverySubjectChecker =
                    (ClientSubjectChecker) Config.getNonNullEntry(
                            config, COMPONENT, "unicastDiscoverySubjectChecker",
                            ClientSubjectChecker.class);
        } catch (NoSuchEntryException e) {
            // leave null
        }

        /* initialize state based on recovered/configured values */
        objectServiceType = new ServiceType(Object.class, null, null);
        computeMaxLeases();
        protocol2 = Discovery.getProtocol2(null);
        /* cache unprocessed unicastDiscovery constraints to handle
        reprocessing of time constraints associated with that method */
        rawUnicastDiscoveryConstraints = discoveryConstraints.getConstraints(
                DiscoveryConstraints.unicastDiscoveryMethod);
        multicastRequestConstraints = DiscoveryConstraints.process(
                discoveryConstraints.getConstraints(
                        DiscoveryConstraints.multicastRequestMethod));
        multicastAnnouncementConstraints = DiscoveryConstraints.process(
                discoveryConstraints.getConstraints(
                        DiscoveryConstraints.multicastAnnouncementMethod));
        unicastDiscoveryConstraints = DiscoveryConstraints.process(
                rawUnicastDiscoveryConstraints);
        serviceExpirer = new ServiceExpireThread();
        eventExpirer = new EventExpireThread();
        unicaster = new UnicastThread(host, unicastPort);
        if (Constants.isMulticastEnabled()) {
            multicaster = new MulticastThread();
            announcer = new AnnounceThread();
        }
        myRef = (Registrar) serverExporter.export(this);
        proxy = RegistrarProxy.getInstance(myRef, myServiceID);
        MarshalledInstance proxyMarshalledInstance = new MarshalledInstance(proxy);
        proxyMarshalledObject = proxyMarshalledInstance.convertToMarshalledObject();

        OptimizedByteArrayOutputStream os = new OptimizedByteArrayOutputStream();
        try (MarshalOutputStream oos = new MarshalOutputStream(os, Collections.EMPTY_SET)) {
            oos.writeObject(proxy);
        }
        proxyRegistrarBytes = os.toByteArray();

        myLocator = (proxy instanceof RemoteMethodControl) ?
                new ConstrainableLookupLocator(
                        unicastDiscoveryHost, unicaster.port, null) :
                new LookupLocator(unicastDiscoveryHost, unicaster.port);
                        /* register myself */
        Item item = new Item(new ServiceItem(myServiceID,
                proxy,
                lookupAttrs));
        SvcReg reg = new SvcReg(item, myLeaseID, SystemTime.timeMillis(), Long.MAX_VALUE);
        addService(reg);

        try {
            DiscoveryGroupManagement dgm = (DiscoveryGroupManagement) discoer;
            String[] groups = dgm.getGroups();
            if (groups == null || groups.length > 0) {
                throw new ConfigurationException(
                        "discoveryManager must be initially configured with " +
                                "no groups");
            }
            DiscoveryLocatorManagement dlm =
                    (DiscoveryLocatorManagement) discoer;
            if (dlm.getLocators().length > 0) {
                throw new ConfigurationException(
                        "discoveryManager must be initially configured with " +
                                "no locators");
            }
            dgm.setGroups(lookupGroups);
            dlm.setLocators(lookupLocators);
            if (DynamicLookupLocatorDiscovery.dynamicLocatorsEnabled() &&
                    discoer instanceof LookupDiscoveryManager) {
                LookupDiscoveryManager ldm = (LookupDiscoveryManager) discoer;
                // making sure this dynamic locator discovery
                // initialization starts only after setting the initial locators
                ldm.getDynamicLocatorDiscovery().init(myServiceID);
            }

        } catch (ClassCastException e) {
            throw new ConfigurationException(null, e);
        }
        joiner = new JoinManager(proxy, lookupAttrs, myServiceID,
                discoer, null, config);

                        /* start up all the daemon threads */
        serviceExpirer.start();
        eventExpirer.start();
        unicaster.start();
        if (multicaster != null) {
            multicaster.start();
        }
        if (announcer != null) {
            announcer.start();
        }
        if (logger.isInfoEnabled()) {
            final long duration = (System.currentTimeMillis() - startTime);
            logger.info("Started Lookup Service " +
                    "[duration=" + BootUtil.formatDuration(duration) +
                    ", groups=" + Arrays.asList(memberGroups) +
                    ", service-id=" + myServiceID +
                    ", locator=" + myLocator +
                    "]");
        }
        ready.ready();
    }

    protected List<Entry> loadInitialLookupAttributes() {
        List<Entry> result = new ArrayList<Entry>();

        result.add(new net.jini.lookup.entry.Name("Lookup"));

        /* Get the JMX Service URL */
        JMXConnection jmxConnection = JMXUtilities.createJMXConnectionAttribute("LUS");
        if (jmxConnection != null) {
            result.add(jmxConnection);
        }

        if (DynamicLookupLocatorDiscovery.dynamicLocatorsEnabled()) {
            result.add(DynamicLookupLocatorDiscovery.DYNAMIC_LOCATORS_ENABLED_LOOKUP_ATTRIBUTE);
        }
        return result;
    }

    /**
     * The code that does the real work of register.
     */
    private ServiceRegistration registerDo(Item nitem, long leaseDuration) {
        if (nitem.service == null)
            throw new NullPointerException("null service");
        if (myServiceID.equals(nitem.serviceID))
            throw new IllegalArgumentException("reserved service id");
        if (nitem.attributeSets == null)
            nitem.attributeSets = emptyAttrs;
        else
            nitem.attributeSets = (EntryRep[]) removeDups(nitem.attributeSets);
        leaseDuration = limitDuration(leaseDuration, maxServiceLease);
        long now = SystemTime.timeMillis();
        if (nitem.serviceID == null) {
            /* new service, match on service object */
            Map svcs = serviceByTypeName.get(nitem.serviceType.getName());
            if (svcs != null) {
                for (Object o : svcs.values()) {
                    SvcReg reg = (SvcReg) o;
                    if (nitem.service.equals(reg.item.service)) {
                        nitem.serviceID = reg.item.serviceID;
                        deleteService(null, reg, now);
                        break;
                    }
                }
            }
            if (nitem.serviceID == null)
                nitem.serviceID = newServiceID();
        } else {
            /* existing service, match on service id */
            SvcReg reg = serviceByID.get(nitem.serviceID);
            if (reg != null)
                deleteService(null, reg, now);
        }
        Util.checkRegistrantServiceID(nitem.serviceID, logger, LogLevel.DEBUG);
        SvcReg reg = new SvcReg(nitem, newLeaseID(), now, now + leaseDuration);
        addService(reg);
        generateEvents(null, nitem, now);
        queueEvents();
        return Registration.getInstance(
                myRef,
                ServiceLease.getInstance(
                        myRef,
                        myServiceID,
                        nitem.serviceID,
                        reg.leaseID,
                        reg.leaseExpiration));
    }

    /**
     * The code that does the real work of lookup.  As a special case, if the template specifies at
     * least one service type to match, and there are multiple items that match the template, then
     * we make a random pick among them, in an attempt to load balance use of "equivalent" services
     * and avoid starving any of them.
     */
    private Item lookupDo(Template tmpl) {
        if (isEmpty(tmpl.serviceTypes) || tmpl.serviceID != null) {
            ItemIter iter = matchingItems(tmpl);
            if (iter.hasNext())
                return iter.next();
            return null;

        }
        List services = matchingServices(tmpl.serviceTypes);
        long now = SystemTime.timeMillis();
        int slen = services.size();
        if (slen == 0)
            return null;
        int srand = random.nextInt(Integer.MAX_VALUE) % slen;
        for (int i = 0; i < slen; i++) {
            SvcReg reg = (SvcReg) services.get((i + srand) % slen);
            if (reg.leaseExpiration > now && matchAttributes(tmpl, reg.item))
                return reg.item;
        }
        return null;
    }

    /**
     * The code that does the real work of lookup.  We do a deep copy of the items being returned,
     * both to avoid having them modified while being marshalled (by a concurrent update method),
     * and to substitute replacements for embedded EntryClass and ServiceType instances, to minimize
     * data sent back to the client.  If duplicates are possible from the iterator, we save all
     * matches, weeding out duplicates as we go, then trim to maxMatches and deep copy.
     */
    private Matches lookupDo(Template tmpl, int maxMatches) {
        if (maxMatches < 0)
            throw new IllegalArgumentException("negative maxMatches");
        int totalMatches = 0;
        ArrayList<Item> matches = null;
        ItemIter iter = matchingItems(tmpl);
        if (maxMatches > 0 || iter.dupsPossible) {
            int suggestedSize = iter.suggestedSize();
            suggestedSize = maxMatches < suggestedSize ? maxMatches : suggestedSize;
            matches = new ArrayList<Item>(suggestedSize);
        }
        if (iter.dupsPossible) {
            while (iter.hasNext()) {
                Item item = iter.next();
                if (!matches.contains(item))
                    matches.add(item);
            }
            totalMatches = matches.size();
            if (maxMatches > 0) {
                for (int i = matches.size(); --i >= maxMatches; )
                    matches.remove(i);
                for (int i = matches.size(); --i >= 0; ) {
                    matches.set(i, copyItem(matches.get(i)));
                }
            } else {
                matches = null;
            }
        } else {
            while (iter.hasNext()) {
                Item item = iter.next();
                totalMatches++;
                if (--maxMatches >= 0)
                    matches.add(copyItem(item)); //todo
            }
        }
        return new Matches(matches, totalMatches);
    }

    /**
     * The code that does the real work of notify. Every registration is given a unique event id.
     * The event id can thus also serve as a lease id.
     */
    private RegistrarEventRegistration notifyDo(Template tmpl, int transitions, RemoteEventListener listener, MarshalledObject handback, long leaseDuration, int notifyType) throws RemoteException {
        if (transitions == 0 ||
                transitions !=
                        (transitions & (ServiceRegistrar.TRANSITION_MATCH_NOMATCH |
                                ServiceRegistrar.TRANSITION_NOMATCH_MATCH |
                                ServiceRegistrar.TRANSITION_MATCH_MATCH)))
            throw new IllegalArgumentException("invalid transitions");
        if (listener == null)
            throw new NullPointerException("listener");
        listener =
                (RemoteEventListener) listenerPreparer.prepareProxy(listener);
        leaseDuration = limitDuration(leaseDuration, maxEventLease);
        long now = SystemTime.timeMillis();
        EventReg reg = new EventReg(eventID, newLeaseID(), tmpl, transitions,
                listener, handback, now + leaseDuration);
        eventID++;
        addEvent(reg);
        RegistrarEventRegistration retVal = new RegistrarEventRegistration(
                reg.eventID,
                proxy,
                EventLease.getInstance(
                        myRef,
                        myServiceID,
                        reg.eventID,
                        reg.leaseID,
                        reg.leaseExpiration),
                reg.seqNo,
                null);
        Matches matches = null;
        if (notifyType == ServiceRegistrar.NOTIFY_WITH_MATCHES) {
            matches = lookupDo(tmpl, Integer.MAX_VALUE);
        } else if (notifyType == ServiceRegistrar.NOTIFY_WITH_EVENTS) {
            matches = new Matches(new ArrayList(), 0);
            Matches itemMatches = lookupDo(tmpl, Integer.MAX_VALUE);
            for (int i = 0; i < itemMatches.items.size(); i++) {
                Item item = (Item) itemMatches.items.get(i);
                generateEvents(null, item, now);
            }
            queueEvents();
        }
        retVal.setMatches(matches);
        return retVal;
    }

    /**
     * The code that does the real work of getEntryClasses. If the template is empty, then we can
     * just use entryClasses, without having to iterate over items, but we have to work harder to
     * get codebases.
     */
    private EntryClassBase[] getEntryClassesDo(Template tmpl) {
        ArrayList<EntryClass> classes = new ArrayList<EntryClass>();
        ArrayList<String> codebases = new ArrayList<String>();
        if (tmpl.serviceID == null &&
                isEmpty(tmpl.serviceTypes) &&
                isEmpty(tmpl.attributeSetTemplates)) {
            long now = SystemTime.timeMillis();
            for (int i = entryClasses.size(); --i >= 0; ) {
                EntryClass eclass = entryClasses.get(i);
                try {
                    codebases.add(pickCodebase(eclass, now));
                    classes.add(eclass);
                } catch (ClassNotFoundException ignored) {
                }
            }
        } else {
            for (ItemIter iter = matchingItems(tmpl); iter.hasNext(); ) {
                Item item = iter.next();
                for (int i = item.attributeSets.length; --i >= 0; ) {
                    EntryRep attrSet = item.attributeSets[i];
                    if (attrMatch(tmpl.attributeSetTemplates, attrSet) &&
                            !classes.contains(attrSet.eclass)) {
                        classes.add(attrSet.eclass);
                        codebases.add(attrSet.codebase);
                    }
                }
            }
        }
        if (classes.isEmpty())
            return null; /* spec says null */
        EntryClassBase[] vals = new EntryClassBase[classes.size()];
        for (int i = vals.length; --i >= 0; ) {
            vals[i] = new EntryClassBase(
                    (classes.get(i)).getReplacement(),
                    codebases.get(i));
        }
        return vals;
    }

    /**
     * The code that does the real work of getFieldValues.  If the template is just a singleton
     * entry with all null fields, then we can do a faster computation by iterating over keys in the
     * given attribute's serviceByAttr map, rather than iterating over items.
     */
    private Object[] getFieldValuesDo(Template tmpl, int setidx,
                                      int fldidx) {
        List<Object> values = new ArrayList<Object>();
        EntryRep etmpl = tmpl.attributeSetTemplates[setidx];
        if (tmpl.serviceID == null &&
                isEmpty(tmpl.serviceTypes) &&
                tmpl.attributeSetTemplates.length == 1 &&
                allNull(etmpl.fields)) {
            long now = SystemTime.timeMillis();
            EntryClass eclass = getDefiningClass(etmpl.eclass, fldidx);
            boolean checkAttr = !eclass.equals(etmpl.eclass);
            HashMap[] attrMaps = serviceByAttr.get(eclass);
            if (attrMaps != null && attrMaps[fldidx] != null) {
                for (Object o : attrMaps[fldidx].entrySet()) {
                    Map.Entry ent = (Map.Entry) o;
                    ArrayList regs = (ArrayList) ent.getValue();
                    Object value = ent.getKey();
                    for (int i = regs.size(); --i >= 0; ) {
                        SvcReg reg = (SvcReg) regs.get(i);
                        if (reg.leaseExpiration > now &&
                                (!checkAttr ||
                                        hasAttr(reg, etmpl.eclass, fldidx, value))) {
                            values.add(value);
                            break;
                        }
                    }
                }
            }
        } else {
            for (ItemIter iter = matchingItems(tmpl); iter.hasNext(); ) {
                Item item = iter.next();
                for (int j = item.attributeSets.length; --j >= 0; ) {
                    if (matchEntry(etmpl, item.attributeSets[j])) {
                        Object value = item.attributeSets[j].fields[fldidx];
                        if (!values.contains(value))
                            values.add(value);
                    }
                }
            }
        }
        if (values.isEmpty())
            return null;
        return values.toArray();
    }

    /**
     * The code that does the real work of getServiceTypes.  If the template has at most service
     * types, then we can do a fast computation based solely on concrete classes, without having to
     * iterate over items, but we have to work a bit harder to get codebases.
     */
    private ServiceTypeBase[] getServiceTypesDo(Template tmpl, String prefix) {
        ArrayList<ServiceType> classes = new ArrayList<ServiceType>();
        ArrayList<String> codebases = new ArrayList<String>();
        if (tmpl.serviceID == null && isEmpty(tmpl.attributeSetTemplates)) {
            List services = matchingServices(tmpl.serviceTypes);
            for (Object service : services) {
                Item item = ((SvcReg) service).item;
                addTypes(classes, codebases, tmpl.serviceTypes, prefix,
                        item.serviceType, item.codebase);
            }
        } else {
            for (ItemIter iter = matchingItems(tmpl); iter.hasNext(); ) {
                Item item = iter.next();
                addTypes(classes, codebases, tmpl.serviceTypes, prefix,
                        item.serviceType, item.codebase);
            }
        }
        if (classes.isEmpty())
            return null; /* spec says null */
        ServiceTypeBase[] vals = new ServiceTypeBase[classes.size()];
        for (int i = vals.length; --i >= 0; ) {
            vals[i] = new ServiceTypeBase(
                    (classes.get(i)).getReplacement(),
                    codebases.get(i));
        }
        return vals;
    }

    /**
     * The code that does the real work of addAttributes. Add every element of attrSets to item,
     * updating serviceByAttr as necessary, incrementing the number of EntryClass instances, and
     * updating entryClasses as necessary.
     */
    private void addAttributesDo(ServiceID serviceID, Uuid leaseID, EntryRep[] attrSets) throws UnknownLeaseException {
        long now = SystemTime.timeMillis();
        SvcReg reg = serviceByID.get(serviceID);
        if (reg == null ||
                !reg.leaseID.equals(leaseID) ||
                reg.leaseExpiration <= now)
            throw new UnknownLeaseException();
        Item pre = (Item) reg.item.clone();
        EntryRep[] sets = reg.item.attributeSets;
        int i = 0;
        /* don't add if it's a duplicate */
        for (int j = 0; j < attrSets.length; j++) {
            EntryRep set = attrSets[j];
            if (indexOf(sets, set) < 0 && indexOf(attrSets, j, set) < 0) {
                attrSets[i++] = set;
                addAttrs(reg, set);
            }
        }
        if (i > 0) {
            int len = sets.length;
            EntryRep[] nsets = new EntryRep[len + i];
            System.arraycopy(sets, 0, nsets, 0, len);
            System.arraycopy(attrSets, 0, nsets, len, i);
            reg.item.attributeSets = nsets;
        }
        refreshServiceFromLookupCache(reg.item, false);
        generateEvents(pre, reg.item, now);
    }

    /**
     * The code that does the real work of modifyAttributes. Modify the attribute sets that match
     * attrSetTmpls, updating or deleting based on attrSets, updating serviceByAttr as necessary,
     * decrementing the number of EntryClass instances, and updating entryClasses as necessary.
     */
    private void modifyAttributesDo(ServiceID serviceID, Uuid leaseID, EntryRep[] attrSetTmpls, EntryRep[] attrSets) throws UnknownLeaseException {
        if (attrSetTmpls.length != attrSets.length)
            throw new IllegalArgumentException(
                    "attribute set length mismatch");
        for (int i = attrSets.length; --i >= 0; ) {
            if (attrSets[i] != null &&
                    !attrSets[i].eclass.isAssignableFrom(attrSetTmpls[i].eclass))
                throw new IllegalArgumentException(
                        "attribute set type mismatch");
        }
        long now = SystemTime.timeMillis();
        SvcReg reg = serviceByID.get(serviceID);
        if (reg == null ||
                !reg.leaseID.equals(leaseID) ||
                reg.leaseExpiration <= now)
            throw new UnknownLeaseException();
        Item pre = (Item) reg.item.clone();
        EntryRep[] preSets = pre.attributeSets;
        EntryRep[] sets = reg.item.attributeSets;
        for (int i = preSets.length; --i >= 0; ) {
            EntryRep preSet = preSets[i];
            EntryRep set = sets[i];
            for (int j = attrSetTmpls.length; --j >= 0; ) {
                if (matchEntry(attrSetTmpls[j], preSet)) {
                    EntryRep attrs = attrSets[j];
                    if (attrs == null) {
                        sets = deleteSet(reg.item, i);
                        deleteAttrs(reg, set, true);
                        break;
                    } else {
                        updateAttrs(reg, set, attrs.fields);
                    }
                }
            }
        }
        for (int i = sets.length; --i >= 0; ) {
            EntryRep set = sets[i];
            if (indexOf(sets, i, set) >= 0) {
                sets = deleteSet(reg.item, i);
                deleteInstance(set.eclass);
            }
        }
        reg.item.attributeSets = sets;
        refreshServiceFromLookupCache(reg.item, false);
        generateEvents(pre, reg.item, now);
    }

    /**
     * The code that does the real work of setAttributes. Replace all attributes of item with
     * attrSets, updating serviceByAttr as necessary, incrementing the number of EntryClass
     * instances, and updating entryClasses as necessary.
     */
    private void setAttributesDo(ServiceID serviceID, Uuid leaseID, EntryRep[] attrSets) throws UnknownLeaseException {
        if (attrSets == null)
            attrSets = emptyAttrs;
        else
            attrSets = (EntryRep[]) removeDups(attrSets);
        long now = SystemTime.timeMillis();
        SvcReg reg = serviceByID.get(serviceID);
        if (reg == null ||
                !reg.leaseID.equals(leaseID) ||
                reg.leaseExpiration <= now)
            throw new UnknownLeaseException();
        Item pre = (Item) reg.item.clone();
        EntryRep[] entries = reg.item.attributeSets;
        for (int i = entries.length; --i >= 0; ) {
            deleteAttrs(reg, entries[i], false);
        }
        reg.item.attributeSets = attrSets;
        for (int i = attrSets.length; --i >= 0; ) {
            addAttrs(reg, attrSets[i]);
        }
        refreshServiceFromLookupCache(reg.item, false);
        generateEvents(pre, reg.item, now);
    }

    /**
     * The code that does the real work of cancelServiceLease.
     */
    private void cancelServiceLeaseDo(ServiceID serviceID, Uuid leaseID) throws UnknownLeaseException {
        if (serviceID.equals(myServiceID))
            throw new SecurityException("privileged service id");
        long now = SystemTime.timeMillis();
        SvcReg reg = serviceByID.get(serviceID);
        if (reg == null ||
                !reg.leaseID.equals(leaseID) ||
                reg.leaseExpiration <= now)
            throw new UnknownLeaseException();
        deleteService(null, reg, now);
    }

    /**
     * The code that does the real work of renewServiceLease.
     */
    private long renewServiceLeaseDo(ServiceID serviceID, Uuid leaseID, long renewDuration) throws UnknownLeaseException {
        long now = SystemTime.timeMillis();
        long renewExpiration = renewServiceLeaseInt(serviceID, leaseID,
                renewDuration, now);
        return renewExpiration - now;
    }

    /**
     * Renew a service lease for a relative duration from now.
     */
    private long renewServiceLeaseInt(ServiceID serviceID, Uuid leaseID, long renewDuration, long now) throws UnknownLeaseException {
        long maxServiceLease = this.maxServiceLease;
        if (serviceID.equals(myServiceID))
            throw new SecurityException("privileged service id");
        if (renewDuration == Lease.ANY)
            renewDuration = maxServiceLease;
        else if (renewDuration < 0)
            throw new IllegalArgumentException("negative lease duration");
        SvcReg reg = serviceByID.get(serviceID);
        if (reg == null ||
                !reg.leaseID.equals(leaseID) ||
                reg.leaseExpiration <= now) {
            throw new UnknownLeaseException();
        }
        synchronized (reg) {
            if (renewDuration > maxServiceLease &&
                    renewDuration > reg.leaseExpiration - now)
                renewDuration = Math.max(reg.leaseExpiration - now,
                        maxServiceLease);
            long renewExpiration = now + renewDuration;
            if (loggerServiceExpire.isTraceEnabled()) {
                loggerServiceExpire.trace("[" + System.identityHashCode(reg) + "]: Service [" + reg.item.serviceID + "]: Renew lease [" + reg.leaseID + "/" + renewExpiration + "], old lease [" + reg.leaseExpiration + "]");
            }
            serviceByTime.remove(new SvcRegExpirationKey(reg, reg.leaseExpiration));
            reg.leaseExpiration = renewExpiration;
            serviceByTime.put(new SvcRegExpirationKey(reg, reg.leaseExpiration), reg);
            return renewExpiration;
        }
    }

    /**
     * The code that does the real work of cancelEventLease.
     */
    private void cancelEventLeaseDo(long eventID, Uuid leaseID) throws UnknownLeaseException {
        long now = SystemTime.timeMillis();
        EventReg reg = eventByID.get(eventID);
        if (reg == null || reg.leaseExpiration <= now)
            throw new UnknownLeaseException();
        deleteEvent(null, reg);
    }

    /**
     * The code that does the real work of renewEventLease.
     */
    private long renewEventLeaseDo(long eventID, Uuid leaseID, long renewDuration) throws UnknownLeaseException {
        long now = SystemTime.timeMillis();
        long renewExpiration = renewEventLeaseInt(eventID, leaseID,
                renewDuration, now);
        return renewExpiration - now;
    }

    private long renewEventLeaseInt
            (
                    long eventID,
                    Uuid leaseID,
                    long renewDuration,
                    long now)
            throws UnknownLeaseException {
        long maxEventLease = this.maxEventLease;
        if (renewDuration == Lease.ANY)
            renewDuration = maxEventLease;
        else if (renewDuration < 0)
            throw new IllegalArgumentException("negative lease duration");
        final EventReg reg = eventByID.get(eventID);
        if (reg == null)
            throw new UnknownLeaseException("trying to renew unknown lease,  event id: " + eventID);
        if (!reg.leaseID.equals(leaseID))
            throw new UnknownLeaseException("Lease id: " + leaseID + " does not match event: " +
                    eventID + " lease id: " + reg.leaseID);
        if (reg.leaseExpiration <= now)
            throw new UnknownLeaseException("Lease id: " + leaseID + " for event: " +
                    eventID + " already expired");
        synchronized (reg) {
            if (renewDuration > maxEventLease &&
                    renewDuration > reg.leaseExpiration - now)
                renewDuration = Math.max(reg.leaseExpiration - now, maxEventLease);
            long renewExpiration = now + renewDuration;
            /* force a re-sort: must remove before changing, then reinsert */
            eventByTime.remove(new EventRegKeyExpiration(reg, reg.leaseExpiration));
            reg.leaseExpiration = renewExpiration;
            eventByTime.put(new EventRegKeyExpiration(reg, reg.leaseExpiration), reg);
            return renewExpiration;
        }
    }

    /**
     * The code that does the real work of renewLeases.  Each element of regIDs must either be a
     * ServiceID (for a service lease) or a Long (for an event lease).  Renewals contains durations.
     * All three arrays must be the same length.
     */
    private RenewResults renewLeasesDo(Object[] regIDs, Uuid[] leaseIDs, long[] renewals) {
        long now = SystemTime.timeMillis();
        Exception[] exceptions = null;
        for (int i = 0; i < regIDs.length; i++) {
            Object id = regIDs[i];
            try {
                if (id instanceof ServiceID)
                    renewals[i] = renewServiceLeaseInt((ServiceID) id,
                            leaseIDs[i],
                            renewals[i], now);
                else
                    renewals[i] = renewEventLeaseInt((Long) id,
                            leaseIDs[i], renewals[i],
                            now);
            } catch (Exception e) {
                renewals[i] = -1;
                if (exceptions == null)
                    exceptions = new Exception[]{e};
                else
                    exceptions = (Exception[]) arrayAdd(exceptions, e);
            }
        }
        for (int i = regIDs.length; --i >= 0; ) {
            if (renewals[i] >= 0)
                renewals[i] -= now;
        }
        return new RenewResults(renewals, exceptions);
    }

    /**
     * The code that does the real work of cancelLeases.  Each element of regIDs must either be a
     * ServiceID (for a service lease) or a Long (for an event lease).  The two arrays must be the
     * same length. If there are no exceptions, the return value is null.  Otherwise, the return
     * value has the same length as regIDs, and has nulls for leases that successfully renewed.
     */
    private Exception[] cancelLeasesDo(Object[] regIDs, Uuid[] leaseIDs) {
        Exception[] exceptions = null;
        for (int i = regIDs.length; --i >= 0; ) {
            Object id = regIDs[i];
            try {
                if (id instanceof ServiceID)
                    cancelServiceLeaseDo((ServiceID) id, leaseIDs[i]);
                else
                    cancelEventLeaseDo((Long) id, leaseIDs[i]);
            } catch (Exception e) {
                if (exceptions == null)
                    exceptions = new Exception[regIDs.length];
                exceptions[i] = e;
            }
        }
        return exceptions;
    }

    /**
     * Generate events for all matching event registrations.  A null pre represents creation of a
     * new item, a null post represents deletion of an item.
     */
    private void generateEvents(Item pre, Item post, long now) {
        ServiceID sid = (pre != null) ? pre.serviceID : post.serviceID;
        Object val = subEventByService.get(sid);
        if (val instanceof EventReg) {
            generateEvent((EventReg) val, pre, post, sid, now);
        } else if (val != null) {
            EventReg[] regs = (EventReg[]) val;
            for (int i = regs.length; --i >= 0; ) {
                generateEvent(regs[i], pre, post, sid, now);
            }
        }
        for (EventReg eventReg : subEventByID.values()) {
            generateEvent(eventReg, pre, post, sid, now);
        }
    }

    /**
     * Generate an event if the event registration matches.  A null pre represents creation of a new
     * item, a null post represents deletion of an item.  sid is the serviceID of the item.
     */
    private void generateEvent(EventReg reg, Item pre, Item post, ServiceID sid, long now) {
        if (reg.leaseExpiration <= now)
            return;
        if ((reg.transitions &
                ServiceRegistrar.TRANSITION_NOMATCH_MATCH) != 0 &&
                (pre == null || !matchItem(reg.tmpl, pre)) &&
                (post != null && matchItem(reg.tmpl, post)))
            pendingEvent(reg, sid, post,
                    ServiceRegistrar.TRANSITION_NOMATCH_MATCH);
        else if ((reg.transitions &
                ServiceRegistrar.TRANSITION_MATCH_NOMATCH) != 0 &&
                (pre != null && matchItem(reg.tmpl, pre)) &&
                (post == null || !matchItem(reg.tmpl, post)))
            pendingEvent(reg, sid, post,
                    ServiceRegistrar.TRANSITION_MATCH_NOMATCH);
        else if ((reg.transitions &
                ServiceRegistrar.TRANSITION_MATCH_MATCH) != 0 &&
                (pre != null && matchItem(reg.tmpl, pre)) &&
                (post != null && matchItem(reg.tmpl, post)))
            pendingEvent(reg, sid, post,
                    ServiceRegistrar.TRANSITION_MATCH_MATCH);
    }

    private void pendingEvent
            (EventReg
                     reg,
             ServiceID
                     sid,
             Item
                     item,
             int transition) {
        pendingEvent(reg, sid, item, transition, true);
    }

    /**
     * Add a pending EventTask for this event registration.
     */
    private void pendingEvent(EventReg reg, ServiceID sid,
                                 Item item, int transition, boolean copyItem) {
        if (item != null && copyItem) {
            item = copyItem(item);
        }

        if (!eventByID.containsKey(reg.eventID)) return; //no longer in map

        List<RegistrarEvent> registrarEvents = pendingQ.get(reg);
        if (registrarEvents == null) {
            registrarEvents = Collections.synchronizedList(new ArrayList<RegistrarEvent>());
            pendingQ.put(reg, registrarEvents);
        }

        registrarEvents.add(new RegistrarEvent(proxy, reg.eventID, ++reg.seqNo, reg.handback, sid, transition, item));
    }

    /**
     * Queue all pending EventTasks for processing by the task manager.
     * Always called under write-lock
     */
    private void queueEvents() {

        //first go over all pending events recently added
        for (Map.Entry<EventReg, List<RegistrarEvent>> pending : pendingQ.entrySet()) {
            if (pending.getValue().isEmpty()) continue;

            List<RegistrarEvent> registrarEvents = eventsQ.get(pending.getKey());

            if (registrarEvents == null) {
                registrarEvents = Collections.synchronizedList(new ArrayList<RegistrarEvent>());
                registrarEvents.addAll(pending.getValue());
                eventsQ.put(pending.getKey(), registrarEvents);
            } else {
                registrarEvents.addAll(pending.getValue());
            }

            pendingEvents.inc(pending.getValue().size());
            pending.getValue().clear();
        }

        //add an EventTask to the TaskManager only if not already added (un-acquired 'lock')
        //an EventTask will handle all events for its target and then release the 'lock'
        for (Map.Entry<EventReg, List<RegistrarEvent>> next : eventsQ.entrySet()) {
            if (next.getValue().isEmpty()) continue;
            if (next.getKey().isLocked()) {continue;}

            EventReg reg = next.getKey();
            if (!reg.acquireLock()) {continue;}

            taskerEvent.add(new EventTask(reg));
        }
    }

    /**
     * Generate a new service ID
     */
    private ServiceID newServiceID
    () {
        Uuid uuid = serviceIdGenerator.generate();
        return new ServiceID(
                uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    /**
     * Generate a new lease ID
     */
    private Uuid newLeaseID
    () {
        return resourceIdGenerator.generate();
    }

    private String asList
            (Object[] a) {
        if (a == null) {
            return "null";
        }
        return Arrays.asList(a).toString();
    }


    // extran conf and stats implemenations

    public NIODetails getNIODetails() throws RemoteException {
        return NIOInfoHelper.getDetails();
    }

    public NIOStatistics getNIOStatistics() throws RemoteException {
        return NIOInfoHelper.getNIOStatistics();
    }

    @Override
    public void enableLRMIMonitoring() throws RemoteException {
        NIOInfoHelper.enableMonitoring();
    }

    @Override
    public void disableLRMIMonitoring() throws RemoteException {
        NIOInfoHelper.disableMonitoring();
    }

    @Override
    public LRMIMonitoringDetails fetchLRMIMonitoringDetails() throws RemoteException {
        return NIOInfoHelper.fetchMonitoringDetails();
    }

    public long getCurrentTimestamp() throws RemoteException {
        return System.currentTimeMillis();
    }

    public OSDetails getOSDetails() throws RemoteException {
        return OSHelper.getDetails();
    }

    public OSStatistics getOSStatistics() throws RemoteException {
        return OSHelper.getStatistics();
    }

    public JVMDetails getJVMDetails() throws RemoteException {
        return JVMHelper.getDetails();
    }

    public JVMStatistics getJVMStatistics() throws RemoteException {
        return JVMHelper.getStatistics();
    }

    public void runGc() throws RemoteException {
        System.gc();
    }

    public LogEntries logEntriesDirect(LogEntryMatcher matcher) throws RemoteException, IOException {
        return InternalLogHelper.logEntriesDirect(LogProcessType.LUS, matcher);
    }
}
