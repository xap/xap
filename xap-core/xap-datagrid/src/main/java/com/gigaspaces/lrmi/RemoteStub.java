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
 * @(#)RemoteStub.java 1.0   23/05/2003  9:50AM
 */

package com.gigaspaces.lrmi;

import com.gigaspaces.internal.stubcache.StubId;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.logger.Constants;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetSocketAddress;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>RemoteStub</code> class is the common superclass to client stubs and provides the
 * framework to support a wide range of remote reference semantics.  Stub objects are surrogates
 * that support exactly the same set of remote interfaces defined by the actual implementation of
 * the remote object.
 *
 * @author Igor Goldenberg
 * @since 5.2
 */
@com.gigaspaces.api.InternalApi
public class RemoteStub<T>
        implements Remote, Externalizable, ILRMIProxy {
    static final long serialVersionUID = 1L;

    // logger
    final static private Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_LRMI);

    /**
     * Unchangeable dynamic proxy instance
     */
    private T _dynamicProxy;

    /**
     * the direct object reference, not <code>null</code> if this stub was never serialized
     */
    transient private T _directObjRef;

    // HERE for Externalizable
    public RemoteStub() {
    }

    /**
     * Constructor to initialize extended stub-class.
     *
     * @param directObjRef the direct object reference. To save dynamic-proxy invocation call.
     * @param dynamicProxy the dynamic proxy of this stub.
     * @throws IllegalArgumentException dynamicProxy is <code>null</code>.
     */
    public RemoteStub(T directObjRef, T dynamicProxy) {
        if (dynamicProxy == null)
            throw new IllegalArgumentException("Failed to initialize " + getClass() + " stub. DynamicProxy argument can not be null");

        _directObjRef = directObjRef;
        _dynamicProxy = dynamicProxy;
    }

    protected RemoteStub(RemoteStub<T> other) {
        this._directObjRef = other._directObjRef;
        this._dynamicProxy = other._dynamicProxy;
    }

    /**
     * @return the direct object reference or dynamic proxy if this stub was serialized.
     */
    public T getProxy() {
        return _directObjRef != null ? _directObjRef : _dynamicProxy;
    }

    public T getDynamicProxy() {
        return _dynamicProxy;
    }

    public boolean isDirect() {
        return _directObjRef != null;
    }

    /**
     * Initialize the abstract logic of extended stub. The best practice to call this method from
     * static initializer of extended stub-class. This method call <code>initCall.call()</code>
     *
     * @param initCall the {@link Callable} interface with abstract init logic within call()
     *                 method.
     */
    protected static void init(Callable initCall) {
        try {
            initCall.call();
        } catch (Exception ex) {
            if (_logger.isErrorEnabled()) {
                _logger.error("Failed to initialize remote stub.", ex);
            }
        }
    }// init

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (obj instanceof RemoteStub) {
            RemoteStub targetRemoteStub = (RemoteStub) obj;

            return getProxy().equals(targetRemoteStub.getProxy());
        }

        return false;
    }

    @Override
    public int hashCode() {
        return getProxy().hashCode();
    }

    @Override
    public String toString() {
        return getProxy().toString();
    }

    /**
     * Returns <code>true</code> if supplied object is {@link RemoteStub}.
     *
     * @param obj the object to check.
     * @return <code>true</code> if supplied object is {@link RemoteStub}.
     */
    public static boolean isStub(Object obj) {
        return obj instanceof RemoteStub;
    }


    /**
     * @return <code>true</code> if RemoteStub instance is collocated with exported endpoint object
     * and communication performs via direct endpoint object reference(No network/socket), otherwise
     * <code>false</code> if communication performs via socket.
     */
    public boolean isCollocated() {
        if (_directObjRef != null)
            return true;

        DynamicSmartStub dynSmartStub = DynamicSmartStub.extractDynamicSmartStubFrom(_dynamicProxy);
        if (dynSmartStub != null)
            return dynSmartStub.isCollocated();

        return false;
    }

    /**
     * Returns <code>true</code> if supplied object instance is {@link RemoteStub} and collocate
     * with RemoteStub endpoint.
     *
     * @param obj the object instance to check.
     * @return <code>true</code> if object embedded stub, otherwise <code>false</code>.
     */
    public static boolean isCollocatedStub(Object obj) {
        return isStub(obj) ? ((RemoteStub) obj).isCollocated() : false;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(_dynamicProxy);
    }

    @SuppressWarnings("unchecked")
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _dynamicProxy = (T) in.readObject();

        /* recover the direct object reference if exists */
        DynamicSmartStub dynSmartStub = DynamicSmartStub.extractDynamicSmartStubFrom(_dynamicProxy);
        if (dynSmartStub != null)
            _directObjRef = (T) dynSmartStub.getLocalObjImpl();
    }

    public PlatformLogicalVersion getServicePlatformLogicalVersion() {
        if (_directObjRef != null)
            return PlatformLogicalVersion.getLogicalVersion();
        return LRMIUtilities.getServicePlatformLogicalVersion(_dynamicProxy);
    }

    public boolean isRemote() {
        return !isDirect();
    }

    public StubId getStubId() {
        if (_dynamicProxy != null && _dynamicProxy instanceof ILRMIProxy)
            return ((ILRMIProxy) _dynamicProxy).getStubId();
        //Just for safety, _dynamicProxy should never be null and should always implement ILRMIProxy
        return null;
    }

    public long getGeneratedTraffic() {
        if (_dynamicProxy != null && _dynamicProxy instanceof ILRMIProxy)
            return ((ILRMIProxy) _dynamicProxy).getGeneratedTraffic();

        return 0;
    }

    public long getReceivedTraffic() {
        if (_dynamicProxy != null && _dynamicProxy instanceof ILRMIProxy)
            return ((ILRMIProxy) _dynamicProxy).getReceivedTraffic();

        return 0;
    }

    public String getConnectionUrl() {
        if (_dynamicProxy != null && _dynamicProxy instanceof ILRMIProxy)
            return ((ILRMIProxy) _dynamicProxy).getConnectionUrl();

        return null;
    }

    @Override
    public long getRemoteProcessId() {
        if (_dynamicProxy != null && _dynamicProxy instanceof ILRMIProxy)
            return ((ILRMIProxy) _dynamicProxy).getRemoteProcessId();

        return -1;
    }

    @Override
    public String getRemoteHostName() {
        if (_dynamicProxy != null && _dynamicProxy instanceof ILRMIProxy)
            return ((ILRMIProxy) _dynamicProxy).getRemoteHostName();

        return null;
    }

    @Override
    public String getRemoteHostAddress() {
        if (_dynamicProxy != null && _dynamicProxy instanceof ILRMIProxy)
            return ((ILRMIProxy) _dynamicProxy).getRemoteHostAddress();

        return null;
    }

    @Override
    public InetSocketAddress getRemoteNetworkAddress() {
        if (_dynamicProxy != null && _dynamicProxy instanceof ILRMIProxy)
            return ((ILRMIProxy) _dynamicProxy).getRemoteNetworkAddress();

        return null;
    }

    public void disable() throws RemoteException {
        if (_dynamicProxy != null && _dynamicProxy instanceof ILRMIProxy)
            ((ILRMIProxy) _dynamicProxy).disable();
    }

    public void enable() throws RemoteException {
        if (_dynamicProxy != null && _dynamicProxy instanceof ILRMIProxy)
            ((ILRMIProxy) _dynamicProxy).enable();
    }

    public void overrideMethodsMetadata(
            Map<String, LRMIMethodMetadata> methodsMetadata) {
        if (_dynamicProxy != null && _dynamicProxy instanceof ILRMIProxy)
            ((ILRMIProxy) _dynamicProxy).overrideMethodsMetadata(methodsMetadata);

    }

    @Override
    public void closeProxy() {
        if (_dynamicProxy != null && _dynamicProxy instanceof ILRMIProxy)
            ((ILRMIProxy) _dynamicProxy).closeProxy();
    }

    @Override
    public boolean isClosed() {
        if (_dynamicProxy != null && _dynamicProxy instanceof ILRMIProxy)
            return ((ILRMIProxy) _dynamicProxy).isClosed();
        return true;
    }
} // RemoteStub
