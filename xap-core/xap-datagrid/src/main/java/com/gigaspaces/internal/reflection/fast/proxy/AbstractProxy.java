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

package com.gigaspaces.internal.reflection.fast.proxy;

import com.gigaspaces.internal.reflection.IDynamicProxy;
import com.gigaspaces.internal.reflection.IMethod;
import com.gigaspaces.internal.reflection.MethodHolder;
import com.gigaspaces.internal.reflection.ProxyInvocationHandler;
import com.gigaspaces.internal.reflection.ReflectionUtil;
import com.gigaspaces.internal.stubcache.MissingCachedStubException;
import com.gigaspaces.internal.stubcache.StubId;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.ILRMIProxy;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.lrmi.LRMIInvocationContext.ProxyWriteType;
import com.gigaspaces.lrmi.LRMIRuntime;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.gigaspaces.serialization.SmartExternalizable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A base class for all the Proxies created by reflection.
 *
 * @author Guy
 * @since 7.0
 */
public abstract class AbstractProxy implements Serializable, IDynamicProxy {
    private static final long serialVersionUID = -7155145351229271987L;
    private static final Logger _stubCacheLogger = LoggerFactory.getLogger(Constants.LOGGER_LRMI_STUB_CACHE);

    final public static String INTERNAL_NAME = ReflectionUtil.getInternalName(AbstractProxy.class);
    final private static Method EQUAL_METHOD;
    final private static Method HASHCODE_METHOD;
    final private static Method TOSTRING_METHOD;

    static {
        try {
            EQUAL_METHOD = Object.class.getDeclaredMethod("equals", Object.class);
            HASHCODE_METHOD = Object.class.getDeclaredMethod("hashCode");
            TOSTRING_METHOD = Object.class.getDeclaredMethod("toString");
        } catch (Exception e) {// can't happen
            throw new RuntimeException(e);
        }
    }

    protected final ProxyInvocationHandler _handler;

    private final ProxyReplace _fullSerializationObjectUncached;
    private final boolean _allowCache;
    private final boolean _cacheable;

    private LightProxyReplace _lightSerializationObject;

    public AbstractProxy(ProxyInvocationHandler handler, boolean allowCache) {
        _handler = handler;

        _fullSerializationObjectUncached = new ProxyReplace(this.getClass().getInterfaces(), getInvocatioHandler(), false, allowCache, this);
        _allowCache = allowCache;
        _cacheable = _allowCache && handler instanceof ILRMIProxy;
    }

    @Override
    public ProxyInvocationHandler getInvocatioHandler() {
        return _handler;
    }

    /**
     * DONT REMOVE!!!! Used by the ObjectOutputStream on serialization.
     */
    public Object writeReplace() throws ObjectStreamException {
        //If not cacheable always use full object and its cache state is initialized to false
        if (!_cacheable)
            return _fullSerializationObjectUncached;
        LRMIInvocationContext currentContext = LRMIInvocationContext.getCurrentContext();
        ProxyWriteType proxyWriteType = currentContext.getProxyWriteType();
        if (proxyWriteType != ProxyWriteType.UNCACHED && !currentContext.isUseStubCache())
            proxyWriteType = ProxyWriteType.UNCACHED;

        switch (proxyWriteType) {
            case UNCACHED:
                if (_stubCacheLogger.isDebugEnabled())
                    _stubCacheLogger.debug("serializing full uncached stub, toString() = " + this.toString());

                return _fullSerializationObjectUncached;
            case CACHED_LIGHT:
                if (_lightSerializationObject != null) {
                    if (_stubCacheLogger.isTraceEnabled())
                        _stubCacheLogger.trace("serializing light stub with id = " + ((ILRMIProxy) _handler).getStubId() + ", stub toString() = " + this.toString());

                    return _lightSerializationObject;
                }
                //else fall through to CACHED_FULL state because this is the first time this object is written
            case CACHED_FULL:
                if (_stubCacheLogger.isDebugEnabled())
                    _stubCacheLogger.debug("serializing full stub for caching with id = " + ((ILRMIProxy) _handler).getStubId() + ", stub toString() = " + this.toString());

                return new ProxyReplace(this.getClass().getInterfaces(), getInvocatioHandler(), true, true, this);
            default:
                throw new RuntimeException("Unexpected ProxyWriteType received " + proxyWriteType);
        }
    }

    /**
     * Creates a set of IMethods from the current "this" interfaces.
     */
    protected static IMethod[] getIMethods(Class clazz) {
        MethodHolder[] uniqueIMethods = getUniqueMethodHolders(clazz.getInterfaces());
        IMethod[] methods = new IMethod[uniqueIMethods.length];
        for (int i = 0; i < uniqueIMethods.length; ++i) {
            // use the class classLoader to create the methods under
            methods[i] = ReflectionUtil.createMethod(clazz.getClassLoader(), uniqueIMethods[i].getMethod());
        }
        return methods;
    }

    /**
     * Retrieves a set of methods from the interfaces.
     */
    public static MethodHolder[] getUniqueMethodHolders(Class<?>[] interfaces) {
        HashSet<MethodHolder> set = new HashSet<MethodHolder>();
        for (Class inter : interfaces) {
            for (Method method : inter.getMethods()) {
                set.add(new MethodHolder(method));
            }
        }
        set.add(new MethodHolder(EQUAL_METHOD));
        set.add(new MethodHolder(HASHCODE_METHOD));
        set.add(new MethodHolder(TOSTRING_METHOD));
        return set.toArray(new MethodHolder[set.size()]);
    }

    public static class ProxyReplace implements SmartExternalizable {
        private static final long serialVersionUID = 1L;

        private Class[] _interfaces;
        private ProxyInvocationHandler _handler;
        private boolean _allowCache;
        private boolean _cacheProxy;

        private transient AbstractProxy _replacingProxy;

        /**
         * Default constructor for Externalizable
         */
        public ProxyReplace() {
        }

        public ProxyReplace(Class[] interfaces, ProxyInvocationHandler handler, boolean cacheProxy, boolean allowCache, AbstractProxy replacingProxy) {
            _handler = handler;
            _interfaces = interfaces;
            _cacheProxy = cacheProxy;
            _allowCache = allowCache;
            _replacingProxy = replacingProxy;
        }

        protected Object createInstance(ClassLoader cl) {
            return ReflectionUtil.createProxy(cl, _interfaces, _handler, _allowCache);
        }

        /**
         * DONT REMOVE!!!! Called by the ObjectInputStream on deserialization
         */
        public Object readResolve() throws ObjectStreamException {
            ClassLoader cl = ReflectionUtil.getClassTargetLoader(_interfaces[0]);
            Object proxyInstance = createInstance(cl);
            if (_cacheProxy && _handler instanceof ILRMIProxy) {
                StubId stubId = ((ILRMIProxy) _handler).getStubId();

                if (_stubCacheLogger.isDebugEnabled())
                    _stubCacheLogger.debug("adding stub to cache, id = " + stubId + ", stub toString() = " + proxyInstance);

                LRMIRuntime.getRuntime().getStubCache().addStub(stubId, proxyInstance);
            } else {
                if (_stubCacheLogger.isDebugEnabled())
                    _stubCacheLogger.debug("stub state is uncached, skipping cache insertion. toString() = " + proxyInstance);
            }
            return proxyInstance;
        }

        Class<?>[] filterInterface(Class<?>[] interfaces){
            List<Class<?>> res = new ArrayList<Class<?>>();
            PlatformLogicalVersion platformLogicalVersion = LRMIInvocationContext.getEndpointLogicalVersion();
            for (Class<?> anInterface : interfaces) {
                if(anInterface.getName().equals("com.gigaspaces.internal.cluster.node.impl.router.CallbackVerifier")
                        && platformLogicalVersion.lessThan(PlatformLogicalVersion.v12_0_1)){
                    continue;
                }
                res.add(anInterface);
            }
            return res.toArray(new Class[res.size()]);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            int arrayLength = in.readInt();
            _interfaces = new Class[arrayLength];
            for (int i = 0; i < arrayLength; ++i)
                _interfaces[i] = (Class) in.readObject();

            _handler = (ProxyInvocationHandler) in.readObject();
            _allowCache = in.readBoolean();
            _cacheProxy = in.readBoolean();
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            Class<?>[] actualInterfaces = filterInterface(_interfaces);
            out.writeInt(actualInterfaces.length);
            for (Class inter : actualInterfaces)
                out.writeObject(inter);

            out.writeObject(_handler);
            out.writeBoolean(_allowCache);
            out.writeBoolean(_cacheProxy);
            //DynamicSmartStub gets it id only after it was first writenExternally (this is when it gets exported)
            //That is why we can only create the _lightSerializationObject here
            if (_cacheProxy)
                _replacingProxy._lightSerializationObject = new LightProxyReplace(((ILRMIProxy) _handler).getStubId());
        }
    }

    /**
     * Light weight serialized proxy, will be deserialized successfully when the actual stub is in
     * the target cache
     *
     * @author eitany
     * @since 7.5
     */
    private static class LightProxyReplace implements SmartExternalizable {
        private static final long serialVersionUID = 1L;

        private StubId _stubId;

        //Externalizable
        public LightProxyReplace() {
        }

        public LightProxyReplace(StubId stubId) {
            _stubId = stubId;
        }

        /**
         * DONT REMOVE!!!! Called by the ObjectInputStream on deserialization
         */
        public Object readResolve() throws ObjectStreamException {
            Object stub = LRMIRuntime.getRuntime().getStubCache().getStub(_stubId);
            if (stub == null) {
                if (_stubCacheLogger.isTraceEnabled())
                    _stubCacheLogger.trace("attempt to get stub from cache by id " + _stubId + " no cached stub exist under that id");
                throw new MissingCachedStubException(_stubId);
            }

            if (_stubCacheLogger.isTraceEnabled())
                _stubCacheLogger.trace("got stub from cache by id " + _stubId + " result stub toString() = " + stub);

            return stub;
        }


        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {
            _stubId = (StubId) in.readObject();
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(_stubId);
        }
    }

}
