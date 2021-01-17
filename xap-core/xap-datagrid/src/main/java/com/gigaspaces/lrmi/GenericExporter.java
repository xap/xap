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

/*
 * @(#)GenericExporter.java 1.0 02/9/2006 17:57PM
 */

package com.gigaspaces.lrmi;

import com.gigaspaces.annotation.lrmi.OneWayRemoteCall;
import com.gigaspaces.config.lrmi.ITransportConfig;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.logger.Constants;

import net.jini.export.Exporter;

import java.io.*;
import java.lang.ref.WeakReference;
import java.rmi.Remote;
import java.rmi.server.ExportException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class provides generic exporting of multiple remote objects such that it can receive remote
 * or local method invocations, and later for unexporting that same remote object. <p> Details of
 * the export and unexport behavior, including the underlying transport communication protocols used
 * for remote invocation and additional remote invocation semantics, are defined by the particular
 * implementation of {@link ITransportConfig} configuration object. <p> When the object exported by
 * {@link #export(Remote)} method, the return value is dynamic proxy(SmartStub) which represents a
 * direct reference to the exported object.<br> If the exported stub is located in <b>Local VM</b>
 * with remote object, every invoked method will have a direct reference call on remote object,
 * otherwise the stub was serialized and moved out side of local VM.<br> If the exported stub was
 * never serialized and no reference left pointing to this stub, this stub will be collected by GC
 * without calling explicitly {@link #unexport(boolean)} method. <p> Working with GenericExporter,
 * <li> No need to create RMI stubs by rmic compiler. <li> Ability to define {@link
 * OneWayRemoteCall} annotation on desired remote method. The remote interface method with defined
 * {@link OneWayRemoteCall} annotation will be called as one-way(async) remote call without return
 * value and without being blocking while the remote call will be execute by Remote implementation.
 * <li> Every invoked method on exported stub will be performed with direct reference method call,
 * if the stub is located in local VM with obj-impl. <p>
 *
 * Steps to create distributed RPC service. <ul> <li> Define remote interface <li> Implement the
 * remote interface <li> Create an instance of GenericExporter with appropriate {@link
 * ITransportConfig} configuration object. <li> export the impl-obj with GenericExporter. <li>
 * unexport to remove the remote object, from the runtime of underlying transport protocol. </ul>
 *
 * <pre>
 * <code>
 * Example:
 *
 * public interface IRemoteHello
 *    extends Remote
 * {
 * 	 String say( String name )
 * 	   throws RemoteException;
 *
 *     @OneWayRemoteCall
 * 	 void sayOneWay( String name )
 * 	   throws RemoteException;
 *  }
 *
 * Exporter exporter = new GenericExporter( new NIOConfiguration() );
 * IRemoteHello helloImpl = new RemoteHelloImpl();
 * IRemoteHello helloStub = exporter.export( helloImpl );
 * helloStub.say("Hello from Duke3D");
 * exporter.unexport( hellowImpl );
 * </code>
 * </pre>
 *
 * @author Igor Goldenberg
 * @see OneWayRemoteCall
 * @see ITransportConfig
 * @since 5.2
 */
@SuppressWarnings("unchecked")
@com.gigaspaces.api.InternalApi
public class GenericExporter
        implements Exporter, Externalizable {
    private static final long serialVersionUID = 1L;

    // logger
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_LRMI_EXPORTER);

    /**
     * the configuration object this exporter
     */
    private ITransportConfig _config;

    /**
     * cache all exported object in local VM as WeakReference
     */
    transient private Map<WeakKey, WeakReference<Remote>> _identityExportObjTable = new HashMap<WeakKey, WeakReference<Remote>>();


    /**
     * Creates a key that holds a weak reference to the argument and compares it using ==.
     */
    private static class WeakKey
            extends WeakReference<Remote> {
        private int hashCode;
        private String className;

        public WeakKey(Remote obj) {
            super(obj);

            if (obj == null)
                throw new IllegalArgumentException("The Key can't be null");

            className = obj.getClass().getName();
            hashCode = obj.getClass().hashCode() ^ System.identityHashCode(obj);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        /**
         * Returns true if the argument is an instance of the same concrete class, and if or if
         * neither object has had its weak and their values are == (equals by reference).
         */
        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof WeakKey))
                return false;

            WeakKey weakKey = (WeakKey) o;
            Object key = weakKey.get();

            return key == get();
        }

        @Override
        public String toString() {
            Object implObj = get();

            if (implObj == null)
                return "ImplObj: [Collected by GC-" + className + "@" + hashCode;

            return "ImplObj: [" + className + "@" + hashCode;
        }
    }// WeakKey

    public GenericExporter() {
    }

    /**
     * Constructor.
     *
     * @param config The configuration object for this exporter. All exported objects will be used
     *               by {@link ITransportConfig}.
     */
    public GenericExporter(ITransportConfig config) {
        _config = config;
    }

    /**
     * @return the transport configuration for this exporter instance.
     */
    public ITransportConfig getConfiguration() {
        return _config;
    }


    /**
     * Exports the specified remote object and returns a proxy that can be used to invoke remote
     * methods on the exported remote object. This method must only be invoked once on a given
     * <code>Exporter</code> instance.
     *
     * <p>The returned proxy implements an implementation-specific set of remote interfaces of the
     * remote object and may also implement additional implementation-specific interfaces.
     *
     * <p>A remote interface is an interface that extends the interface <code>java.rmi.Remote</code>
     * and whose methods each declare at least one exception whose type is
     * <code>java.rmi.RemoteException</code> or one of its superclasses.
     *
     * <p>If the <code>impl</code> object already exported, returns the same proxy instance
     * representing this <code>impl</code>
     *
     * @param impl a remote object to export
     * @return a proxy for the remote object
     * @throws ExportException          if a problem occurs exporting the object
     * @throws SecurityException        if a <code>SecurityException</code> occurs exporting the
     *                                  object
     * @throws IllegalArgumentException if <code>impl</code> is <code>null</code>
     * @throws IllegalStateException    if an object has already been exported with this
     *                                  <code>Exporter</code> instance
     **/
    public Remote export(Remote impl) throws ExportException {
        return export(impl, _config, true);
    }

    /**
     * Exports the specified remote object and returns a proxy that can be used to invoke remote
     * methods on the exported remote object. This method must only be invoked once on a given
     * <code>Exporter</code> instance.
     *
     * <p>The returned proxy implements an implementation-specific set of remote interfaces of the
     * remote object and may also implement additional implementation-specific interfaces.
     *
     * <p>A remote interface is an interface that extends the interface <code>java.rmi.Remote</code>
     * and whose methods each declare at least one exception whose type is
     * <code>java.rmi.RemoteException</code> or one of its superclasses.
     *
     * <p>If the <code>impl</code> object already exported, returns the same proxy instance
     * representing this <code>impl</code>
     *
     * @param impl       a remote object to export
     * @param allowCache allow the exported object to cached when serialized over the network
     * @return a proxy for the remote object
     * @throws ExportException          if a problem occurs exporting the object
     * @throws SecurityException        if a <code>SecurityException</code> occurs exporting the
     *                                  object
     * @throws IllegalArgumentException if <code>impl</code> is <code>null</code>
     * @throws IllegalStateException    if an object has already been exported with this
     *                                  <code>Exporter</code> instance
     **/
    public Remote export(Remote impl, boolean allowCache) throws ExportException {
        return export(impl, _config, allowCache);
    }

    /**
     * Exports the specified remote object with desired {@link ITransportConfig} configuration
     * object and returns a proxy that can be used to invoke remote methods on the exported remote
     * object. This method must only be invoked once on a given <code>Exporter</code> instance.
     *
     * <p>The returned proxy implements an implementation-specific set of remote interfaces of the
     * remote object and may also implement additional implementation-specific interfaces.
     *
     * <p>A remote interface is an interface that extends the interface <code>java.rmi.Remote</code>
     * and whose methods each declare at least one exception whose type is
     * <code>java.rmi.RemoteException</code> or one of its superclasses.
     *
     * <p>If the <code>impl</code> object already exported, returns the same proxy instance
     * representing this <code>impl</code>
     *
     * @param config the configuration object to export the <code>impl</code> object.
     * @param impl   a remote object to export
     * @return a proxy for the remote object
     * @throws ExportException          if a problem occurs exporting the object
     * @throws SecurityException        if a <code>SecurityException</code> occurs exporting the
     *                                  object
     * @throws IllegalArgumentException if <code>impl</code> is <code>null</code>
     * @throws IllegalStateException    if an object has already been exported with this
     *                                  <code>Exporter</code> instance
     **/
    public Remote export(Remote impl, ITransportConfig config, boolean allowCache) throws ExportException {
        if (_logger.isDebugEnabled()) {
            _logger.debug("Trying to export class=" + impl.getClass());
        }

        if (impl == null)
            throw new IllegalArgumentException("Remote obj can not be null");

        if (config == null)
            throw new IllegalArgumentException("Configuration object can not be null");

		/*
        * allow to export the same object more then once, return the already
		 * exported stub for this <code>impl</code> obj.
		 */
        WeakKey wk = new WeakKey(impl);
        WeakReference<Remote> value = _identityExportObjTable.get(wk);
        Remote implStub = value != null ? value.get() : null;

        if (implStub != null)
            return implStub;

		 /* export the impl obj to the DynamicSmartStub */
        implStub = LRMIRuntime.getRuntime().createDynamicProxy(impl, config, allowCache);

        _identityExportObjTable.put(wk, new WeakReference(implStub));

        if (_logger.isDebugEnabled())
            _logger.debug("ObjImpl: [" + implStub + "] was exported.");

        return implStub;
    }

    /**
     * Unexport the desired obj from the underlying transport protocol and remove the obj from the
     * manage cache.
     */
    protected boolean _unexport(Remote obj) {
        WeakReference<Remote> weakStub = _identityExportObjTable.get(new WeakKey(obj));
        Remote dynamicProxy = weakStub != null ? weakStub.get() : null;
		
		/* was never exported or already collected by GC */
        if (dynamicProxy == null)
            return true;

		/* extract the DynamicSmartStub from dynamic proxy */
        DynamicSmartStub dynamicSmartStub = TransportProtocolHelper.extractSmartStubFromProxy(dynamicProxy);
        dynamicSmartStub.unexport();

        if (_logger.isDebugEnabled())
            _logger.debug("ObjImpl: [" + dynamicSmartStub.getLocalObjImpl() + "] was unexported.");

        return true;
    }


    /**
     * Removes the remote object, obj, from the runtime of underlying transport protocol. If
     * successful, the object can no longer accept incoming RMI calls. If the force parameter is
     * true, the object is forcibly unexported even if there are pending calls to the remote object
     * or the remote object still has calls in progress.  If the force parameter is false, the
     * object is only unexported if there are no pending or in progress calls to the object.
     *
     * @param obj the remote object to be unexported pending or in-progress calls; if false, only
     *            unexports the object if there are no pending or in-progress calls
     * @return true if operation is successful, false otherwise
     */
    public boolean unexport(Remote obj) {
        _unexport(obj);

		/* remove from cache */
        _identityExportObjTable.remove(new WeakKey(obj));

		/* always success status */
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public boolean unexport(boolean force) {
		/* unexport all objects */
        for (Iterator<WeakKey> iter = _identityExportObjTable.keySet().iterator(); iter.hasNext(); ) {
            WeakKey exportedObjImpl = iter.next();
            Remote obj = exportedObjImpl.get();
			
		  /* 
			* if obj==null this obj was collected by GC and was never exported 
			* by underlying transport protocol 
			*/
            if (obj != null)
                _unexport(obj);

			/* no maintenance anymore */
            iter.remove();
        }// for

		/*
		 * always return <code>true</code> because we manage multiple exported
		 * obj per Exporter
		 */
        return true;
    }


    /**
     * Returns <code>true</code> if the provided obj exported by GenericExporter.
     *
     * @param obj the object to check.
     * @return <code>true</code> if provided obj exported and manage by GenericExporter.
     **/
    public boolean isExported(Remote obj) {
        return _identityExportObjTable.containsKey(new WeakKey(obj));
    }

//    /**
//     * reconstruct the GenericExporter after serialization
//     */
//    private void readObject(java.io.ObjectInputStream in) throws IOException,
//            ClassNotFoundException {
//        in.defaultReadObject();
//
//        _identityExportObjTable = new HashMap<WeakKey, WeakReference<Remote>>();
//    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, _config);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _config = IOUtils.readObject(in);
    }
}
