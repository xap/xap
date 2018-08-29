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

import com.gigaspaces.annotation.lrmi.AsyncRemoteCall;
import com.gigaspaces.config.ConfigurationException;
import com.gigaspaces.config.lrmi.ITransportConfig;
import com.gigaspaces.exception.lrmi.ProxyClosedException;
import com.gigaspaces.internal.lrmi.ConnectionUrlDescriptor;
import com.gigaspaces.internal.lrmi.LRMIOutboundMonitoringDetailsImpl;
import com.gigaspaces.internal.lrmi.LRMIProxyMonitoringDetailsImpl;
import com.gigaspaces.internal.reflection.IMethod;
import com.gigaspaces.internal.reflection.ProxyInvocationHandler;
import com.gigaspaces.internal.reflection.ReflectionUtil;
import com.gigaspaces.internal.reflection.standard.StandardMethod;
import com.gigaspaces.internal.stubcache.StubId;
import com.gigaspaces.internal.utils.concurrent.ContextClassLoaderCallable;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.internal.version.PlatformVersion;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.nio.async.FutureContext;
import com.gigaspaces.lrmi.nio.async.IFuture;
import com.gigaspaces.start.SystemInfo;
import com.j_spaces.kernel.ClassLoaderHelper;

import net.jini.security.Security;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.UnexpectedException;
import java.rmi.UnmarshalException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * This class represents dynamic smart stub of underlying transport protocol. Due the dynamic proxy
 * {@link Proxy} this class serves as Interceptor of invoked methods. The invoked method injected by
 * dynamic-proxy delegates to the direct reference reflection call of exported object, only in case
 * this DynamicSmartStub still handles a direct reference of exported object {@link
 * #getLocalObjImpl()}. <p> If DynamicSmartStub is serializing, as part {@link
 * #writeExternal(java.io.ObjectOutput)} method the {@link #getLocalObjImpl()} will be exported to
 * the underlying transport protocol. The serialization process is an indication for
 * DynamicSmartStub about exiting out side from local JVM.<br> On deserialization the
 * DynamicSmartStub will try to aquire the direct localObject reference from the underlying
 * transport protocol. If direct reference wasn't acquired, it means the stub instance is not in
 * local JVM. Every future injected method will be called remotely.
 *
 * @author Igor Goldenberg
 * @see LRMIRuntime
 * @since 5.2
 */
@com.gigaspaces.api.InternalApi
public class DynamicSmartStub
        implements ProxyInvocationHandler, InvocationHandler, Serializable, Remote, Externalizable, ILRMIProxy {
    private static final long serialVersionUID = 1L;
    private static final byte SERIAL_VERSION = Byte.MIN_VALUE;

    final private static Logger _logger = Logger.getLogger(Constants.LOGGER_LRMI);

    /**
     * direct reference to the exported object, <code>null</code> if this stub was exited from
     * exported JVM
     */
    private transient Remote _localObj;

    /**
     * remote invocation handler constructed by Transport protocol
     */
    private transient MethodCachedInvocationHandler _remoteInvHandler;

    /**
     * the config object of exported object
     */
    private ITransportConfig _config;

    /**
     * connectionURL to the unique remote object, <code>null</code> if this stub was never
     * serialized
     */
    private String _connectionURL;
    private transient long _remoteProcessId;
    private transient InetSocketAddress _remoteNetworkAddress;
    private String _platformVersion;

    /**
     * ~cache
     */
    private int _hashCode;
    private long _remoteObjectId;
    private String _remoteObjClassName;
    private long _remoteClassLoaderId;
    private long _remoteLrmiRuntimeId;

    /**
     * indication whether this stub instance was unexported(closed).
     */
    private boolean _unexported;

    /**
     * Holds method name to index mapping
     */
    private Map<String, Integer> _methodMapping;
    transient private Map<String, LRMIMethodMetadata> _methodsMetadata;

    /**
     * Stub remote interfaces
     */
    private List<Class<?>> _stubInterfaces;

    private PlatformLogicalVersion _platformLogicalVersion;
    /**
     * keeps the the mapping between DynamicProxy method and directImpl method object key: dynamic
     * proxy method instance value: the direct method reference of target object.
     **/
    transient private Map<IMethod, IMethod> _identityMethodCache;

    /**
     * provides a method description mapping key: MethodDescriptor, value: the method instance of
     * {@link #_localObj}
     */
    transient private Map<String, IMethod> _methodDescTable;

    /**
     * Represents a ~cache of invocation handlers, if inv-handler with appropriate remoteObjId
     * already in ~cache use it. NOTE: Every stub does use the already initialized ConnectionPool
     * which part of InvocationHandler.
     **/
    final private static Map<String, MethodCachedInvocationHandler> _remoteInvHandlerCache = new HashMap<String, MethodCachedInvocationHandler>();

    final private static String LRMI_PROXY_CLASS_NAME = ILRMIProxy.class.getName();

    /**
     * thread context classLoader of exported object
     */
    transient private ClassLoader _exporterThreadContextClassLoader;
    //Indicates this stub is closed at the client side, can no longer be used
    transient private boolean _proxyClosed;

    transient public static final ThreadLocal<Boolean> markProxyAsClosed = new ThreadLocal<Boolean>() {
        @Override
        public Boolean initialValue() {
            return true;
        }
    };

    // HERE just for externalizable
    public DynamicSmartStub() {
    }

    /**
     * Constructor with package access level
     */
    DynamicSmartStub(Remote localObj, ITransportConfig config) {
        /* keep the ClassLoader of exported threaded process */
        _exporterThreadContextClassLoader = Thread.currentThread().getContextClassLoader();

        _localObj = localObj;
        _config = config;
        _remoteObjClassName = localObj.getClass().getName();
        _stubInterfaces = LRMIUtilities.getDeclaredRemoteInterfaces(localObj.getClass());
        _methodMapping = createMethodMapping();
        _platformVersion = PlatformVersion.getOfficialVersion();
        _platformLogicalVersion = PlatformLogicalVersion.getLogicalVersion();

        init();

		/* cache the hashCode of DynamicSmartStub */
        _hashCode = _remoteObjClassName.hashCode() ^ System.identityHashCode(localObj);
    }

    private Map<String, Integer> createMethodMapping() {
        List<IMethod> sortedMethodList = LRMIUtilities.getSortedMethodList(_stubInterfaces);
        Map<String, Integer> mapping = new HashMap<String, Integer>();
        for (int i = 0; i < sortedMethodList.size(); ++i)
            mapping.put(LRMIUtilities.getMethodNameAndDescriptor(sortedMethodList.get(i)), i);

        return mapping;
    }

    /**
     * init not serializable stub structures Flush to main memory once completed (must be called
     * after _methodMapping is initialized)
     */
    private synchronized void init() {
        _identityMethodCache = Collections.synchronizedMap(new IdentityHashMap<IMethod, IMethod>());
    }

    /**
     * Returns the direct method reference of the localObject. In most cases this method invokes
     * when "declaring method class" of dynamic proxy is not Assignable from locaObj class. It might
     * happen only if _localObj loaded by different ClassLoader than declaring class of dynamic
     * proxy method.
     *
     * @param dynamicProxyInvMethod the intercepted invoked method of dynamic proxy.
     * @return the direct method reference of localObject.
     **/
    final private IMethod getReferenceMethod(IMethod dynamicProxyInvMethod) throws UnexpectedException {
        IMethod targetInkMethod = _identityMethodCache.get(dynamicProxyInvMethod);
        if (targetInkMethod != null)
            return targetInkMethod;

        synchronized (_identityMethodCache) {
            targetInkMethod = _identityMethodCache.get(dynamicProxyInvMethod);
            if (targetInkMethod != null)
                return targetInkMethod;

		 /* lazy init creates on demand */
            if (_methodDescTable == null)
                _methodDescTable = LRMIUtilities.getMappingMethodDescriptor(_localObj.getClass());

		 /* lookup the direct method reference by method-description(String) */
            String methodDesc = LRMIUtilities.getMethodNameAndDescriptor(dynamicProxyInvMethod);
            targetInkMethod = _methodDescTable.get(methodDesc);
            if (targetInkMethod == null) {
                if (_logger.isLoggable(Level.SEVERE)) {
                    _logger.log(Level.SEVERE, "DynamicSmartStub - Failed to get method descriptor for  [" + dynamicProxyInvMethod + "] target class " +
                            "[" + _localObj.getClass() + "].\nMethod signature: " + methodDesc);
                }


                throw new UnexpectedException("Method descriptor is not found for method: " + dynamicProxyInvMethod);
            }

            _identityMethodCache.put(dynamicProxyInvMethod, targetInkMethod);
        }//sync

        return targetInkMethod;
    }


    /**
     * Invoke the desired method provided by dynamic proxy.
     *
     * @param targetObject the target object invoke the method on.
     * @param invokeMethod the method to invoke.
     * @param args         the method arguments.
     * @return the result of dispatching the method represented by this object on
     * <code>targetObject</code> with parameters <code>args</code>
     * @throws Throwable the exception throwable by invoked method.
     **/
    static private Object _invoke(final Object targetObject, final IMethod invokeMethod, Object[] args)
            throws Throwable {
        try {
            if (_logger.isLoggable(Level.FINEST))
                _logger.entering("DynamicSmartStub - " + targetObject.getClass().getName(), invokeMethod.getName(), args);

            Object resultInv = invokeMethod.invoke(targetObject, args);

            if (_logger.isLoggable(Level.FINEST))
                _logger.exiting("DynamicSmartStub - " + targetObject.getClass().getName(), invokeMethod.getName(), resultInv);

            return resultInv;

        } catch (InvocationTargetException e) {
            if (_logger.isLoggable(Level.FINEST)) {
                _logger.log(Level.FINEST, "DynamicSmartStub - Invoke method: [" + invokeMethod + "] on " +
                        "[" + targetObject.getClass() + "] thrown exception: " + e.toString());
            }

            throw e.getTargetException();
        } catch (IllegalArgumentException e) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE, "DynamicSmartStub - Failed to invoke method: [" + invokeMethod + "] on " +
                        "[" + targetObject.getClass() + "] thrown exception: " + e.toString() +
                        "\nInvoke method ClassLoader: " + invokeMethod.getDeclaringClass().getClassLoader() +
                        "\nTarget object instance ClassLoader: " + targetObject.getClass().getClassLoader());
            }

            throw e;

        } catch (IllegalAccessException e) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE, "DynamicSmartStub - Failed to invoke method: " + invokeMethod + "." +
                        "\nMethod does not have an access to the definition of " + targetObject.getClass() + " class.");
            }

            throw e;
        }
    }


    /**
     * Invoke by reflection the intercepted method on direct object reference.
     *
     * @param invokeMethod the method to invoke.
     * @param args         the method arguments.
     * @return the return value of invoked method, or <code>null</code>.
     * @throws Throwable failed to invoke method.
     **/
    final private Object invokeDirect(final IMethod invokeMethod, Object[] args)
            throws Throwable {
        /* keep the original Thread classLoader */
        ClassLoader orgThreadCL = Thread.currentThread().getContextClassLoader();
        final boolean changeCL = orgThreadCL != _exporterThreadContextClassLoader;

        try {
           /* set the exported contextClassLoader */
            if (changeCL)
                ClassLoaderHelper.setContextClassLoader(_exporterThreadContextClassLoader, true /*ignore security*/);


		  /*
			* the invoked method is not assignable from the class of localObject.
			* the localObject was loaded by different ClassLoader than declaring class of
			* dynamic-proxy method and can't be invoked directly by reflection.
			*/
            if (invokeMethod.getDeclaringClass().isAssignableFrom(_localObj.getClass())) {
			   /* make sure the invoke method is Accessible */
                if (!invokeMethod.isAccessible()) {
                    Security.doPrivileged(new PrivilegedAction<Object>() {
                        public Object run() {
                            invokeMethod.setAccessible(true);
                            return null;
                        }
                    });
                }

                return _invoke(_localObj, invokeMethod, args);
            }// if

            if (_logger.isLoggable(Level.FINEST))
                _logger.finest("DynamicSmartStub - declared ClassLoader of invoke method is not is assignable from ClassLoader of localObject." +
                        " Converting method arguments to ClassLoader of localObject." +
                        "\nMethod: " + invokeMethod +
                        "\nDeclared Method ClassLoader: " + invokeMethod.getDeclaringClass().getClassLoader() +
                        "\nLocalObject ClassLoader: " + _localObj.getClass().getClassLoader() +
                        "\nExported ThreadClassLoader: " + _exporterThreadContextClassLoader);

			/* convert the call to the classLoader of localObject object */
            final IMethod refMethod = getReferenceMethod(invokeMethod);
            final Object[] clArgs = (Object[]) LRMIUtilities.convertToAssignableClassLoader(args, _localObj.getClass().getClassLoader());

            boolean async = refMethod.getAnnotation(AsyncRemoteCall.class) != null;

           /* direct reference invoke on ref-object instance */
            Object returnValue = null;
            if (async) {
                Future<Object> future = LRMIRuntime.getRuntime().getThreadPool().submit(new ContextClassLoaderCallable<Object>() {
                    @Override
                    protected Object execute() throws Exception {
                        try {
                            return _invoke(_localObj, refMethod, clArgs);
                        } catch (Throwable t) {
                            throw new ExecutionException(t);
                        }
                    }
                });

                FutureContext.setFutureResult((IFuture<Object>) future);
            } else {
                returnValue = _invoke(_localObj, refMethod, clArgs);
            }

            if (returnValue == null)
                return null;

			/* convert back the result to the original ClassLoader of declaring method class */
            return LRMIUtilities.convertToAssignableClassLoader(returnValue, invokeMethod.getDeclaringClass().getClassLoader());
        } finally {
            if (changeCL)
                ClassLoaderHelper.setContextClassLoader(orgThreadCL, true /*ignore security*/);
        }
    }

    /**
     * Needed for standard java dynamic proxy.
     */
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable {
        return invoke(proxy, new StandardMethod(method), args);
    }

    /**
     * {@inheritDoc}
     **/
    public Object invoke(Object proxy, final IMethod method, Object[] args)
            throws Throwable {
		 /* no remote call if the invoked method is Object.equals(), hashCode() or toString() */
        Class<?> declaringClass = method.getDeclaringClass();
        if (declaringClass == Object.class) {
            if (method.getName().equals("hashCode"))
                return hashCode();

            if (method.getName().equals("equals"))
                return equals(args[0]);

            if (method.getName().equals("toString"))
                return toString();

            throw new InternalError("Unexpected Object method dispatched: " + method);
        }

	    /* handle special case of ILRMIProxy invocation */
        if (LRMI_PROXY_CLASS_NAME.equals(declaringClass.getName()))
            return invokeLRMIProxy(method, args);


	    /* DynamicSmartStub exists out side of exported JVM, do remote method call invocation */
        if (_localObj == null)
            return invokeRemote(proxy, method, args);

	    /* if not null DynamicSmartStub in local JVM, do direct method call on localObject */
        return invokeDirect(method, args);
    }

    /**
     * perform special handling of ILRMIProxy method
     **/
    private Object invokeLRMIProxy(final IMethod method, Object[] args) throws Exception {
        return method.invoke(this, args);
    }


    /**
     * performs remote invocation method
     */
    protected Object invokeRemote(Object proxy, final IMethod method, Object[] args) throws Throwable {
        ProxyInvocationHandler remoteInvocationHandler = getInvocationHandler();

        return remoteInvocationHandler.invoke(proxy, method, args);
    }

    private MethodCachedInvocationHandler getExistingInvocationHandler() {
        return _remoteInvHandler;
    }

    /**
     * returns the initialized remote InvocationHandler
     */
    protected MethodCachedInvocationHandler getInvocationHandler() throws RemoteException {
        MethodCachedInvocationHandler existingHandler = getExistingInvocationHandler();
        if (existingHandler != null)
            return existingHandler;

		/* lock the ~cache to insure only one instance of InvocationHandler per RemoteObjectId */
        synchronized (_remoteInvHandlerCache) {
            if (_proxyClosed)
                DynamicSmartStub.throwProxyClosedExeption(_connectionURL);
            //Double check under lock
            existingHandler = getExistingInvocationHandler();
            if (existingHandler != null)
                return existingHandler;

    		/* get the ~cache invocation handler by connectionURL */
            MethodCachedInvocationHandler peerInvocationHandler = _remoteInvHandlerCache.get(_connectionURL);

    		/* is invocation already cached */
            if (peerInvocationHandler != null) {
                peerInvocationHandler.incrementReference();
                _remoteInvHandler = peerInvocationHandler;
                return _remoteInvHandler;
            }

            try {
				/* get initialize LRMIRuntime */
                ClientPeerInvocationHandler clientInvocationHandler = LRMIRuntime.getRuntime().getClientInvocationHandler(_connectionURL, _config, _platformLogicalVersion);
                RemoteMethodCache methodCache = LRMIUtilities.createRemoteMethodCache(_stubInterfaces, _methodMapping, _methodsMetadata);
                _remoteInvHandler = new MethodCachedInvocationHandler(methodCache, clientInvocationHandler, _platformVersion, _platformLogicalVersion, _connectionURL);

				/* ~cache */
                _remoteInvHandlerCache.put(_connectionURL, _remoteInvHandler);

                if (_logger.isLoggable(Level.FINE))
                    _logger.fine(Thread.currentThread() + " DynamicSmartStub prepared connection: " + toString());
                return _remoteInvHandler;
            } catch (ProxyClosedException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new RemoteException("Failed to initialize client connection." + toString(), ex);
            }
        }// sync
    }

    /**
     * @return the remote objID
     */
    public long getRemoteObjID() {
        return _remoteObjectId;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        /* export the object only once */
        if (_localObj != null) {
            /* must be under lock, in order to prevent export object in time the DynamicSmartStub being closing */
            synchronized (this) {
              /* only if still not exported */
                if (_connectionURL == null) {
                    ClassLoader orgCL = Thread.currentThread().getContextClassLoader();
                    try {
                     /*
                      * on binding this stub to the remote LRMI stack,
                      * bind with CL of exported Thread which is creator of DynamicSmartStub.
                      */
                        Thread.currentThread().setContextClassLoader(_exporterThreadContextClassLoader);

                     /* bind to remote LRMI stack */
                        ServerPeer sp = LRMIRuntime.getRuntime().export(_localObj, _config);

                        _remoteObjectId = sp.getObjectId();
                        _remoteClassLoaderId = sp.getObjectClassLoaderId();
                        _remoteLrmiRuntimeId = LRMIRuntime.getRuntime().getID();

                        try {
                            this._connectionURL = sp.getConnectionURL();
                        } catch (NullPointerException e) {
                            _logger.log(Level.SEVERE, "DEBUG: config is " + _config);
                            throw e;
                        }

                    } catch (ConfigurationException ex) {
                        throw new RemoteException("Failed to export object: " + _remoteObjClassName, ex);
                    } finally {
                        Thread.currentThread().setContextClassLoader(orgCL);
                    }

                    if (_logger.isLoggable(Level.FINER)) {
                        _logger.finer("Exported and bound ServerEndPoint: " + toString());
                    }
                }// if
            }// sync
        }// if

        out.writeByte(SERIAL_VERSION);
        out.writeObject(_platformLogicalVersion);
        out.writeUTF(_platformVersion);
        out.writeObject(_config);
        out.writeUTF(_connectionURL);
        out.writeInt(_hashCode);
        out.writeLong(_remoteObjectId);
        out.writeLong(_remoteClassLoaderId);
        out.writeLong(_remoteLrmiRuntimeId);
        out.writeUTF(_remoteObjClassName);
        out.writeObject(_methodMapping);
        out.writeObject(filter(_stubInterfaces));
        out.writeBoolean(_unexported);
    }

    private List<Class<?>> filter(List<Class<?>> interfaces) {
        PlatformLogicalVersion platformLogicalVersion = LRMIInvocationContext.getEndpointLogicalVersion();
        List<Class<?>> res = new ArrayList<Class<?>>();
        for (Class<?> anInterface : interfaces) {
            if(anInterface.getName().equals("com.gigaspaces.internal.cluster.node.impl.router.CallbackVerifier")
                    && platformLogicalVersion.lessThan(PlatformLogicalVersion.v12_0_1)){
                continue;
            }
            res.add(anInterface);
        }
        return res;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        byte version = in.readByte();
        if (version != SERIAL_VERSION)
            throw new UnmarshalException("Requested version [" + version + "] does not match local version [" + SERIAL_VERSION + "]. Please make sure you are using the same version on both ends, local version is " + PlatformVersion.getOfficialVersion());
        _platformLogicalVersion = (PlatformLogicalVersion) in.readObject();
        _platformVersion = in.readUTF();
        _config = (ITransportConfig) in.readObject();
        _connectionURL = in.readUTF();
        ConnectionUrlDescriptor connectionUrlDescriptor = ConnectionUrlDescriptor.fromUrl(_connectionURL);
        _remoteNetworkAddress = connectionUrlDescriptor.getSocketAddress();
        _remoteProcessId = connectionUrlDescriptor.getPid();
        _hashCode = in.readInt();
        _remoteObjectId = in.readLong();
        _remoteClassLoaderId = in.readLong();
        _remoteLrmiRuntimeId = in.readLong();
        _remoteObjClassName = in.readUTF();
        _methodMapping = (Map<String, Integer>) in.readObject();
        _stubInterfaces = (List<Class<?>>) in.readObject();
        _unexported = in.readBoolean();

        /* init not serializable structures */
        init();

        /* makes an attempt to acquire direct reference by remoteObjectId if we are still in local JVM */
        if (_localObj == null && !LRMIRuntime.getRuntime().isUseNetworkInJVM()) {
            ObjectRegistry.Entry objectEntry = LRMIRuntime.getRuntime().getRegistryObject(_remoteObjectId);
            if (objectEntry != null) {
              /* recover the localObject and original ClassLoader of exported thread */
                _localObj = objectEntry.getObject();
                _exporterThreadContextClassLoader = objectEntry.getExportedThreadClassLoader();
            }
        }
    }

    /**
     * @return The instance of local object impl, or <code>null</code> if this Stub doesn't exist in
     * local exported VM
     **/
    public Object getLocalObjImpl() {
        return _localObj;
    }

    /***
     * @param obj the object to extract DynamicSmartStub instance.
     * @return the extracted DynamicSmartStub instance or <code>null</code> supplied object is not
     * DynamicSmartStub invocation handler and not {@link Proxy}.
     */
    public static DynamicSmartStub extractDynamicSmartStubFrom(Object obj) {
        DynamicSmartStub dynStub = null;

		/* get DynamicSmartStub from dynamic proxy */
        if (ReflectionUtil.isProxyClass(obj.getClass())) {
            Object ih = ReflectionUtil.getInvocationHandler(obj);
            if (ih instanceof DynamicSmartStub)
                dynStub = (DynamicSmartStub) ih;
        } else {
            if (obj instanceof DynamicSmartStub)
                dynStub = (DynamicSmartStub) obj;
        }

        return dynStub;
    }

    /**
     * @return <code>true</code> if this instance of DynamicSmartStub is <b>collocated</b> with
     * exported endpoint object, otherwise <code>false</code> if this stub is <b>remote</b>.
     **/
    public boolean isCollocated() {
        return _localObj != null;
    }


    /**
     * @return the HashCode of this DynamicSmartStub
     */
    @Override
    public int hashCode() {
        return _hashCode;
    }

    /**
     * Equals the DynamicSmartStub instances by direct obj reference if the stubs in local VM,
     * otherwise equals by remoteObjId
     **/
    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;

		/* equals dynamic proxy instances */
        if (this == obj)
            return true;

        DynamicSmartStub eqSt = extractDynamicSmartStubFrom(obj);

		/* the object is not DynamicSmartStub */
        if (eqSt == null)
            return false;

		/* equals invocation handler instances */
        if (eqSt == this)
            return true;

		/* equals the local obj references if we still in local VM */
        if (_localObj != null && eqSt.getLocalObjImpl() != null)
            return _localObj == eqSt.getLocalObjImpl();

		/* equals by remote objectId */
        return _remoteObjectId == eqSt._remoteObjectId;
    }

    /**
     * @return <code>true</code> if DynamicSmartStub was closed
     */
    synchronized public boolean isUnexported() {
        return _unexported;
    }

    /**
     * CloseDynamicSmartStub. Unexport if need localObject from underlying transport protocol.
     *
     * NOTE: This method must be called only by {@link GenericExporter}.
     **/
    synchronized void unexport() {
        if (_unexported)
            return;

        _unexported = true;

        try {
		 /* not null if _localObj was once exported to underlying transport protocol */
            if (_connectionURL != null) {
                LRMIRuntime.getRuntime().unexport(_localObj, _config.getProtocolName(), true);
            }
        } catch (NoSuchObjectException e) {
            if (_logger.isLoggable(Level.FINE))
                _logger.fine("RemoteObject: [" + _localObj.getClass().getName() + "] was never exported.");
        } catch (RemoteException e) {
            if (_logger.isLoggable(Level.SEVERE))
                _logger.log(Level.SEVERE, "Failed to unexport " + _localObj.getClass().getName() + " object.", e);
        }
    }// close

    @Override
    public void closeProxy() {
        if (_proxyClosed)
            return;

        synchronized (_remoteInvHandlerCache) {
            if (_proxyClosed)
                return;

            MethodCachedInvocationHandler existingInvocationHandler = getExistingInvocationHandler();
            //Remove reference, if this is the last one, remove this from the cache
            if (existingInvocationHandler != null && existingInvocationHandler.decrementReference())
                _remoteInvHandlerCache.remove(_connectionURL);
            _remoteInvHandler = null;
            if (markProxyAsClosed == null || markProxyAsClosed.get())
                _proxyClosed = true;
        }
    }

    @Override
    public boolean isClosed() {
        return _proxyClosed;
    }

    public boolean isRemote() {
        return _localObj == null;
    }

    public PlatformLogicalVersion getServicePlatformLogicalVersion() {
        return _platformLogicalVersion;
    }

    public long getGeneratedTraffic() {
        MethodCachedInvocationHandler remoteInvHandler = _remoteInvHandler;
        if (remoteInvHandler == null)
            return 0;

        return remoteInvHandler.getInvocationHandler().getGeneratedTraffic();
    }

    public long getReceivedTraffic() {
        MethodCachedInvocationHandler remoteInvHandler = _remoteInvHandler;
        if (remoteInvHandler == null)
            return 0;

        return remoteInvHandler.getInvocationHandler().getReceivedTraffic();
    }

    /**
     * Returns the connectionURL of exported remote object.
     *
     * @return the unique connectionURL of exported remote object. This URL contains
     * [Protocol]://[ServerEndPointIP:Port]/[RemoteObjectUID]_[StubSeqID] Example:
     * NIO://192.168.0.100:61754/1164059001211_0
     **/
    public String getConnectionUrl() {
        return _connectionURL;
    }

    @Override
    public long getRemoteProcessId() {
        return _remoteProcessId;
    }

    @Override
    public String getRemoteHostName() {
        return _remoteNetworkAddress != null ? _remoteNetworkAddress.getHostName() : null;
    }

    @Override
    public String getRemoteHostAddress() {
        return _remoteNetworkAddress != null ? _remoteNetworkAddress.getAddress().getHostAddress() : null;
    }

    @Override
    public InetSocketAddress getRemoteNetworkAddress() {
        return _remoteNetworkAddress;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("DynamicSmartStub [ImplObjClass: " + _remoteObjClassName);
        if (_localObj != null) {
            sb.append(", RemoteObjRef: Still in local VM");
            sb.append(", ObjectClassLoader: " + _localObj.getClass().getClassLoader());
            sb.append(", ExportedThreadClassLoader: " + _exporterThreadContextClassLoader);
        }

        if (_connectionURL != null)
            sb.append(", ConnectionURL: " + _connectionURL);

        sb.append(", MaxConnPool: " + _config.getConnectionPoolSize());
        if (_localObj != null)
            sb.append(", localImpl toString: [" + _localObj.toString() + "]");

        sb.append(" ]");

        return sb.toString();
    }

    public StubId getStubId() {
        return new StubId(_remoteLrmiRuntimeId, _remoteObjectId);
    }

    public void disable() throws RemoteException {
        getInvocationHandler().disable();
    }

    public void enable() throws RemoteException {
        getInvocationHandler().enable();
    }

    // Memory barrier.
    public synchronized void overrideMethodsMetadata(
            Map<String, LRMIMethodMetadata> methodsMetadata) {
        _methodsMetadata = methodsMetadata;
    }

    public static void throwProxyClosedExeption(String connectionUrl) throws ProxyClosedException {
        throw new ProxyClosedException("proxy is closed [" + connectionUrl + "]");
    }

    public static LRMIOutboundMonitoringDetailsImpl getMonitoringDetails() {
        LinkedList<LRMIProxyMonitoringDetailsImpl> lrmiProxyMonitoringDetails = new LinkedList<LRMIProxyMonitoringDetailsImpl>();
        synchronized (_remoteInvHandlerCache) {
            for (MethodCachedInvocationHandler invocationHandler : _remoteInvHandlerCache.values())
                lrmiProxyMonitoringDetails.add(invocationHandler.getMonitoringDetails());

            return new LRMIOutboundMonitoringDetailsImpl(lrmiProxyMonitoringDetails.toArray(new LRMIProxyMonitoringDetailsImpl[lrmiProxyMonitoringDetails.size()]));
        }
    }

    static void shutdown() {
        synchronized (_remoteInvHandlerCache) {
            for (MethodCachedInvocationHandler cachedHandler : _remoteInvHandlerCache.values()) {
                cachedHandler.getInvocationHandler().close();
            }
            _remoteInvHandlerCache.clear();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        closeProxy();
        super.finalize();
    }


    public static void simulatedDisconnectionByPID(int pid) {
        synchronized (_remoteInvHandlerCache) {
            String pidToken = "pid[" + pid + "]";
            if (_logger.isLoggable(Level.INFO))
                _logger.info("disabling outbound LRMI stubs to pid [ " + pid + " ] my pid is [ " + SystemInfo.singleton().os().processId() + " ]");
            for (MethodCachedInvocationHandler invocationHandler : _remoteInvHandlerCache.values()) {
                if (invocationHandler.getConnectionURL().contains(pidToken)) {
                    if (_logger.isLoggable(Level.INFO))
                        _logger.info("disabling stub [ " + invocationHandler.getConnectionURL() + " ]");
                    invocationHandler.disable();
                }
            }
        }
    }

    public static void simulatedReconnectionByPID(int pid) {
        synchronized (_remoteInvHandlerCache) {
            String pidToken = "pid[" + pid + "]";
            if (_logger.isLoggable(Level.INFO))
                _logger.info("enabling outbound LRMI stubs to pid [ " + pid + " ] my pid is [ " + SystemInfo.singleton().os().processId() + " ]");
            for (MethodCachedInvocationHandler invocationHandler : _remoteInvHandlerCache.values()) {
                if (invocationHandler.getConnectionURL().contains(pidToken)) {
                    _logger.log(Level.INFO, "enabling stub [ " + invocationHandler.getConnectionURL() + " ]");
                    invocationHandler.enable();
                }
            }
        }
    }
}