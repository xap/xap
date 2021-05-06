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


package com.gigaspaces.lrmi;

import com.gigaspaces.config.ConfigurationException;
import com.gigaspaces.config.lrmi.ITransportConfig;
import com.gigaspaces.config.lrmi.nio.NIOConfiguration;
import com.gigaspaces.exception.lrmi.ApplicationException;
import com.gigaspaces.exception.lrmi.RMIShutDownException;
import com.gigaspaces.internal.reflection.IMethod;
import com.gigaspaces.internal.reflection.ReflectionUtil;
import com.gigaspaces.internal.stubcache.StubCache;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.logger.LogUtils;
import com.gigaspaces.lrmi.ProtocolAdapter.Side;
import com.gigaspaces.lrmi.nio.Pivot;
import com.gigaspaces.lrmi.nio.async.LRMIThreadPoolExecutor;
import com.gigaspaces.lrmi.nio.watchdog.Watchdog;
import com.gigaspaces.management.transport.ITransportConnection;
import com.j_spaces.core.service.ServiceConfigLoader;
import com.j_spaces.kernel.ClassLoaderHelper;
import com.j_spaces.kernel.SystemProperties;
import com.j_spaces.kernel.threadpool.DynamicThreadPoolExecutor;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LRMI is the main LRMI class, from the developer's perspective.
 *
 * Each JVM has one LRMI Runtime instance, which is obtainable via the getRuntime() static method.
 *
 * The LRMI Runtime provides access to the Protocol Registry, so that protocols can be
 * plugged-in/out, and browsed.
 *
 * The LRMI Runtime provides export/unexport capabilities to servers. A remote object may be
 * exported on several protocols, with different properties.
 *
 * The LRMI Runtime maintains an internal Remote Object Registry table, which keeps information
 * about exported remote objects. Each remote object gets a unique id. in the LRMI Runtime
 * environment. This unique id is the same, for all protocols that the remote object is exported to
 * (although the connection URLs may be different for these protocols). The unique id is the key for
 * the Remote Object Registry.
 *
 * @author Igor Goldenberg
 * @since 4.0
 */
@com.gigaspaces.api.InternalApi
public class LRMIRuntime {
    private static final Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_LRMI);
    private static final Logger _exporterLogger = LoggerFactory.getLogger(Constants.LOGGER_LRMI_EXPORTER);
    private static final LRMIRuntime _runtime = new LRMIRuntime();

    final private ProtocolRegistry _protocolRegistry;
    final private ObjectRegistry _objectRegistry;
    final private long _id;
    final private LRMIThreadPoolExecutor _lrmiThreadPool;
    final private LRMIThreadPoolExecutor _livenessPriorityThreadPool;
    final private LRMIThreadPoolExecutor _monitoringPriorityThreadPool;
    final private LRMIThreadPoolExecutor _customThreadPool;
    final private StubCache _stubCache;
    final private INetworkMapper _networkMapper = constructNetworkMapper();
    //Current lrmi usage simply doesn't support shutdown on last registrar since the client 
    //holds selector threads for async operations and is a server for remote class loading, this behavior is mostly obsolete due to service grid.
    private final boolean _shutdownOnLastRegistrar = Boolean.getBoolean("com.gs.transport_protocol.lrmi.shutdownLastRegistrar");

    private boolean _monitorActivity = Boolean.getBoolean("com.gs.transport_protocol.lrmi.monitorActivity");
    final private Object _monitorActivityMemoryBarrier = new Object();
    private boolean _useNetworkInJVM = Boolean.parseBoolean(System.getProperty("com.gs.transport_protocol.lrmi.useNetworkInJVM", "true"));

    private volatile boolean _isShutdown;

    /**
     * Dummy object id
     */
    public static final int DUMMY_OBJECT_ID = -1;

    /**
     * private constructor to protect outside initialization
     */
    private LRMIRuntime() {
        _protocolRegistry = new ProtocolRegistry();
        _objectRegistry = new ObjectRegistry();
        _stubCache = new StubCache();

        boolean useSecureRandom = Boolean.getBoolean(SystemProperties.LRMI_USE_SECURE_RADNDOM);
        Random random;
        if (useSecureRandom) {
            random = new SecureRandom();
        } else {
            random = new Random();
        }
        _id = random.nextLong();

        ITransportConfig config = ServiceConfigLoader.getTransportConfiguration();
        _lrmiThreadPool = new LRMIThreadPoolExecutor(
                config.getMinThreads(), config.getMaxThreads(),
                config.getThreadPoolIdleTimeout(), config.getThreadsQueueSize(),
                Long.MAX_VALUE, Thread.NORM_PRIORITY, "LRMI-Connection", true, true);
        NIOConfiguration nioConfig = (NIOConfiguration) config;
        _livenessPriorityThreadPool = new LRMIThreadPoolExecutor(nioConfig.getSystemPriorityMinThreads(),
                nioConfig.getSystemPriorityMaxThreads(),
                config.getSystemPriorityThreadIdleTimeout(),
                config.getSystemPriorityQueueCapacity(),
                Long.MAX_VALUE,
                Thread.MAX_PRIORITY,
                "LRMI-Liveness",
                true, true);
        _monitoringPriorityThreadPool = new LRMIThreadPoolExecutor(nioConfig.getSystemPriorityMinThreads(),
                nioConfig.getSystemPriorityMaxThreads(),
                config.getSystemPriorityThreadIdleTimeout(),
                config.getSystemPriorityQueueCapacity(),
                Long.MAX_VALUE,
                Thread.NORM_PRIORITY,
                "LRMI-Monitoring",
                true, true);
        _customThreadPool = new LRMIThreadPoolExecutor(nioConfig.getCustomMinThreads(),
                nioConfig.getCustomMaxThreads(),
                nioConfig.getCustomThreadIdleTimeout(),
                nioConfig.getCustomQueueCapacity(),
                Long.MAX_VALUE,
                Thread.NORM_PRIORITY,
                "LRMI-Custom",
                true, true);
    }


    @SuppressWarnings("unchecked")
    static private INetworkMapper constructNetworkMapper() {
        String networkMapperName = System.getProperty(SystemProperties.LRMI_NETWORK_MAPPER);
        if (networkMapperName == null) {
            if (_logger.isDebugEnabled())
                _logger.debug("Creating default network mapper");
            return new DefaultNetworkMapper();
        }
        try {
            if (_logger.isDebugEnabled())
                _logger.debug("Creating custom network mapper provider " + networkMapperName);
            Class<INetworkMapper> loadClass = ClassLoaderHelper.loadClass(networkMapperName, true);
            return loadClass.newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("The specified network mapper [" + networkMapperName + "] could not be created", e);
        }
    }

    /**
     * @return the instance of LRMIRuntime
     */
    static public LRMIRuntime getRuntime() {
        return _runtime;
    }

    /**
     * Returns the LRMIRuntime instance.
     *
     * @param config   the transport protocol configuration.
     * @param initSide the protocol side initialization {@link Side} Client or Server.
     * @return the instance of LRMIRuntime
     * @throws ConfigurationException failed to initialize the runtime of LRMI.
     **/
    static public LRMIRuntime getRuntime(ITransportConfig config, ProtocolAdapter.Side initSide)
            throws ConfigurationException {
        if (config != null)
            _runtime.init(config, initSide);

        return _runtime;
    }

    public long getID() {
        return _id;
    }

    private void init(ITransportConfig config, ProtocolAdapter.Side initSide) throws ConfigurationException {
        _protocolRegistry.init(config, initSide);
    }

    public DynamicThreadPoolExecutor getThreadPool() {
        return _lrmiThreadPool;
    }

    public DynamicThreadPoolExecutor getMonitoringPriorityThreadPool() {
        return _monitoringPriorityThreadPool;
    }

    public DynamicThreadPoolExecutor getLivenessPriorityThreadPool() {
        return _livenessPriorityThreadPool;
    }

    public DynamicThreadPoolExecutor getCustomThreadPool() {
        return _customThreadPool;
    }

    public boolean isUseNetworkInJVM() {
        return _useNetworkInJVM;
    }

    /**
     * If set Network socket will be used even when the client and the server share the same JVM.
     */
    public void setUseNetworkInJVM(boolean useNetworkInJVM) {
        this._useNetworkInJVM = useNetworkInJVM;
    }

    public void setMonitorActivity(boolean trackActivity) {
        synchronized (_monitorActivityMemoryBarrier) {
            _monitorActivity = trackActivity;
        }
    }

    public boolean isMonitorActivity() {
        return _monitorActivity;
    }

    /**
     * Returns the protocol registry of this LRMI Runtime.
     *
     * @return the protocol registry of this LRMI Runtime.
     */
    public ProtocolRegistry getProtocolRegistry() {
        return _protocolRegistry;
    }

    public int getPort(ITransportConfig config) {
        return getRuntime().getProtocolRegistry().getPort(config);
    }

    public LRMIMonitoringDetails getMonitoringDetails(ITransportConfig config) {
        if (!isMonitorActivity())
            throw new IllegalStateException("LRMI monitoring must be enabled in order to get monitoring details");
        return getRuntime().getProtocolRegistry().getMonitoringDetails(config);
    }

    public void initServerSide() throws ConfigurationException {
        synchronized (_objectRegistry) {
            init(ServiceConfigLoader.getTransportConfiguration(), Side.SERVER);
        }
    }
    /**
     * Exports the specified remote object on the specified protocol. The returned ServerPeer may be
     * used to retrieve both the remote object id and the connection URL.
     */
    ServerPeer export(Remote object, ITransportConfig config)
            throws RemoteException, ConfigurationException {
        if (_isShutdown)
            throw new RMIShutDownException("LRMIRuntime was shutdown");
        if (_exporterLogger.isDebugEnabled()) {
            _exporterLogger.debug("Trying to export class=" + object.getClass());
        }

        String protocol = config.getProtocolName();

        synchronized (_objectRegistry) {
            init(config, Side.SERVER);

            // if protocol is not registered in protocol registry, register it
            ProtocolAdapter<?> protocolAdapter = _protocolRegistry.get(protocol);

            if (protocolAdapter == null) {
                throw new RemoteException("Failed to export object: protocol " + protocol +
                        " is not registered in protocol registry");
            }

            ObjectRegistry.Entry orEntry = _objectRegistry.getEntryFromObject(object);

            /* check if this is the first time the object is exported */
            if (orEntry == null) {
                orEntry = _objectRegistry.createEntry(object, UIDGen.nextId(), LRMIUtilities.getSortedLRMIMethodList(object.getClass()));
                
                /* create and init. server peer */
                ServerPeer serverPeer = protocolAdapter.newServerPeer(orEntry.m_ObjectId, orEntry.getExportedThreadClassLoader(), Pivot.getServiceDetails(object));
                serverPeer.beforeExport(config);

                /* register object under protocol in the object registry */
                orEntry.setProtocol(protocol);
                orEntry.setServerPeer(serverPeer);

                if (_exporterLogger.isDebugEnabled()) {
                    _exporterLogger.debug("LRMIRuntime exported remote object [localObj=" +
                            object.getClass().getName() + "@" + Integer.toHexString(System.identityHashCode(object)) +
                            ", protocol=" + protocol +
                            ", remoteObjID=" + orEntry.m_ObjectId +
                            ", RemoteObjectClassLoader=" + object.getClass().getClassLoader() +
                            ", ExportedThreadClassLoader: " + orEntry.getExportedThreadClassLoader() + "]");
                }
            }
            return orEntry.getServerPeer(); // return the already exported ServerPeer
        }
    }

    /**
     * Returns an instance of a dynamic {@link Proxy} which represents DynamicSmartStub and castable
     * to any remote declared interface of passed <code>obj<code>.
     *
     * @param remoteObj the remote object to make it available to receive remote method invocation.
     * @param config    the configuration of transport protocol.
     * @return dynamic smart stub.
     **/
    public Remote createDynamicProxy(Remote remoteObj, ITransportConfig config, boolean allowCache) {
        /* dynamic smart stub which servers as InvocationHandler(interceptor) of invoked methods */
        DynamicSmartStub genericStub = new DynamicSmartStub(remoteObj, config);

        /* get declared remote interfaces of this stub */
        Class[] remoteInterfaces = LRMIUtilities.getProxyInterfaces(remoteObj.getClass());

        /* return a dynamic proxy which is castable to the given interface */
        return (Remote) ReflectionUtil.createProxy(remoteObj.getClass().getClassLoader(), remoteInterfaces, genericStub, allowCache);
    }


    /**
     * Unexports the remote object from the specified protocol.
     *
     * @param object   the the object to unexport.
     * @param protocol the protocol to unexport the object from.
     * @param force    if <code>true</code> unexport should be performed even if the remote object
     *                 is in the process of serving clients' requests.
     * @throws java.rmi.RemoteException failed to unexport.
     * @throws NoSuchObjectException    if the remote object is not currently exported.
     */
    public void unexport(Remote object, String protocol, boolean force)
            throws RemoteException, NoSuchObjectException {
        if (_isShutdown)
            return;
        synchronized (_objectRegistry) {
            ObjectRegistry.Entry orEntry = _objectRegistry.getEntryFromObject(object);

            /* if object is not exported, throw an exception */
            if (orEntry == null)
                throw new NoSuchObjectException("Failed to unexport object: object is not exported");

            /* if object is not exported on the protocol, throw an exception */
            if (!orEntry.getProtocol().equals(protocol))
                throw new NoSuchObjectException("Failed to unexport object: " + object.getClass().getName() + " object is not exported on protocol " + protocol);

            /* unregister object in the object registry */
            _objectRegistry.removeEntry(object);

            /* unexport and unbound this ServerPeer from pivot */
            ServerPeer serverPeer = orEntry.getServerPeer();
            serverPeer.afterUnexport(force);

            /* if last registrar of this protocol - free resources */
            if (_shutdownOnLastRegistrar && _objectRegistry.isLastRegistrar(protocol)) {
                _protocolRegistry.remove(protocol);
                serverPeer.getProtocolAdapter().shutdown();
            }// if

            if (_logger.isDebugEnabled()) {
                _logger.debug("LRMIRuntime unexported remote object [localObj=" +
                        object.getClass().getName() + "@" + System.identityHashCode(object) +
                        ", protocol=" + protocol +
                        ", remoteObjID=" + orEntry.m_ObjectId + "]");
            }
        }// sync
    }


    /**
     * Returns the remote method invocation handler {@link ClientPeerInvocationHandler} which
     * represents remote object specified by the supplied protocol and connection URL. The returned
     * {@link ClientPeerInvocationHandler} maintains a pool of connections(sockets) to the remote
     * object.
     *
     * @param connectionURL the connectionURL to the remoteObj provided by {@link ServerPeer}.
     * @param config        the client configuration.
     * @return the remote method invocation handler.
     **/
    ClientPeerInvocationHandler getClientInvocationHandler(String connectionURL, ITransportConfig config, PlatformLogicalVersion serviceVersion)
            throws RemoteException, ConfigurationException {
        if (_isShutdown)
            throw new RMIShutDownException("LRMIRuntime was shutdown");
        String protocolName = config.getProtocolName();

        synchronized (_objectRegistry) {
            init(config, Side.CLIENT);

	        /* find protocol adapter */
            ProtocolAdapter<?> pa = _protocolRegistry.get(protocolName);
            if (pa == null)
                throw new RemoteException("Protocol " + protocolName + " not found in protocol registry");

            return pa.getClientInvocationHandler(connectionURL, config, serviceVersion);
        }
    }

    /**
     * Returns the object registry info of bound remote object in remote LRMI stack.
     *
     * @param objectId The exported objectId.
     * @return registry object or remote object or <code>null</code> if the supplied objectId was
     * never registered or already unexported.
     **/
    public ObjectRegistry.Entry getRegistryObject(long objectId) {
        return _objectRegistry.getEntryFromObjectIdIfExists(objectId);
    }

    /**
     * This method is called by a Server Peer after extracting the method name and arguments.
     */
    public Object invoked(long objectId, IMethod method, Object[] args)
            throws RemoteException, ApplicationException {
        /* get object from Object Registry */
        ObjectRegistry.Entry orEntry = _objectRegistry.getEntryFromObjectId(objectId);

        final Object targetObject = orEntry.m_Object;

        /* keep original thread context classLoader */
        ClassLoader orgThreadClassLoader = Thread.currentThread().getContextClassLoader();
        final boolean changeCL = orgThreadClassLoader != orEntry.getExportedThreadClassLoader();

        try {
            /* 
             * before invocation, set classLoader of exported thread(the thread which exported targetObject).
             */
            if (changeCL)
                ClassLoaderHelper.setContextClassLoader(orEntry.getExportedThreadClassLoader(), true /*ignore security*/);

            if (_logger.isDebugEnabled())
                LogUtils.entering(_logger, "LRMIRuntime - " + targetObject.getClass().getName() + "#" + objectId, method.getName(), args);

            //noinspection unchecked
            Object resultInv = method.invoke(targetObject, args);

            if (_logger.isDebugEnabled())
                LogUtils.exiting(_logger, "LRMIRuntime - " + targetObject.getClass().getName() + "#" + objectId, method.getName(), resultInv);

            return resultInv;
        } catch (IllegalArgumentException ex) {
            String exMsg = "LRMIRuntime - Failed to invoke RemoteMethod: [" + method + "] on [" + targetObject + "] Reason: " + ex.toString();

            if (_logger.isErrorEnabled())
                _logger.error(exMsg, ex);

            throw new ApplicationException(exMsg, ex);

        } catch (IllegalAccessException ex) {
            String exMsg = "LRMIRuntime - Failed to invoke RemoteMethod: [" + method + "] on [" + targetObject + "] Reason: " + ex.toString();

            if (_logger.isErrorEnabled())
                _logger.error(exMsg, ex);

            throw new ApplicationException(exMsg, ex);
        } catch (InvocationTargetException ex) {
            if (_logger.isDebugEnabled()) {
                String exMsg = "LRMIRuntime - Failed to invoke RemoteMethod: " + method + " Reason: " + ex.toString();

                _logger.debug(exMsg, ex.getTargetException());
            }

            throw new ApplicationException(null /* save message network serialization */, ex.getTargetException());
        } finally {
            if (changeCL)
                ClassLoaderHelper.setContextClassLoader(orgThreadClassLoader, true /*ignore security*/);
        }
    }// invoked()

    /**
     * @return the Transport connection info list for supplied remote objectID.
     **/
    public List<ITransportConnection> getRemoteObjectConnectionsList(long remoteObjID)
            throws NoSuchObjectException {
        return _objectRegistry.getEntryFromObjectId(remoteObjID).getServerPeer().getProtocolAdapter().getRemoteObjectConnectionsList(remoteObjID);
    }

    public int countRemoteObjectConnections(long remoteObjID)
            throws NoSuchObjectException {
        return _objectRegistry.getEntryFromObjectId(remoteObjID).getServerPeer().getProtocolAdapter().countRemoteObjectConnections(remoteObjID);
    }

    public StubCache getStubCache() {
        return _stubCache;
    }

    /**
     * Shutdown the lrmi runtime, once invoked the LRMI layer is destroyed and cannot be reused in
     * the current class loader which it was created in, as a result this is an irreversible
     * process.
     *
     * @since 7.1
     */
    public synchronized void shutdown() {
        if (_isShutdown)
            return;
        _isShutdown = true;

        for (ProtocolAdapter<?> protocolAdapter : _protocolRegistry.values())
            protocolAdapter.shutdown();

        _protocolRegistry.clear();
        _objectRegistry.clear();
        _stubCache.clear();

        _lrmiThreadPool.shutdownNow();
        _monitoringPriorityThreadPool.shutdownNow();
        _livenessPriorityThreadPool.shutdownNow();
        _customThreadPool.shutdown();

        DynamicSmartStub.shutdown();

        Watchdog.shutdown();
    }

    /**
     * Gets the network mapper
     */
    public INetworkMapper getNetworkMapper() {
        return _networkMapper;
    }

    public void simulatedDisconnectionByPID(int pid) {
        DynamicSmartStub.simulatedDisconnectionByPID(pid);
    }

    public void simulatedReconnectionByPID(int pid) {
        DynamicSmartStub.simulatedReconnectionByPID(pid);
    }

}
