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

package com.gigaspaces.lrmi.classloading;

import com.gigaspaces.internal.utils.collections.CopyOnUpdateMap;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.logger.TraceableLogger;

import java.lang.ref.WeakReference;
import java.util.HashMap;

/**
 * Holds the {@link LRMIClassLoader}s that associate to a specific service class loader
 *
 * @author eitany
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class ServiceClassLoaderContext {
    final private static TraceableLogger _logger = TraceableLogger.getLogger(Constants.LOGGER_LRMI_CLASSLOADING);

    private final CopyOnUpdateMap<String, WeakReference<LRMIClassLoader>> _classLoaderByName = new CopyOnUpdateMap<String, WeakReference<LRMIClassLoader>>();

    private final HashMap<LRMIRemoteClassLoaderIdentifier, WeakReference<LRMIClassLoader>> _classLoaderByRemoteId = new HashMap<LRMIRemoteClassLoaderIdentifier, WeakReference<LRMIClassLoader>>();

    private final HashMap<String, byte[]> _classBytesByName = new HashMap<String, byte[]>();

    private final String _name;

    private final Object _stateLock = new Object();

    final private static boolean _disableDuplicateLoadProtection = Boolean.getBoolean("com.gs.transport_protocol.lrmi.classloading.disable-duplicate-protection");

    public ServiceClassLoaderContext(String name) {
        _name = name;
    }

    /**
     * @return gets a class loader by name, in a non blocking fashion
     */
    public LRMIClassLoader getClassLoaderByClassNameNonBlocking(String className) {
        WeakReference<LRMIClassLoader> lrmiClassLoaderRef = _classLoaderByName.get(className);
        if (lrmiClassLoaderRef == null)
            return null;

        return lrmiClassLoaderRef.get();
    }

    /**
     * @return the {@link LRMIClassLoader} that loaded the specified class name or null if none
     * exists
     */
    public LRMIClassLoader getClassLoaderByClassName(String className) {
        synchronized (_stateLock) {
            WeakReference<LRMIClassLoader> lrmiClassLoaderRef = _classLoaderByName.get(className);
            if (lrmiClassLoaderRef == null) {
                return null;
            }
            LRMIClassLoader loader = lrmiClassLoaderRef.get();
            if (loader == null) {
                if (_logger.isTraceEnabled())
                    _logger.trace("LRMIClassLoader of class: " + className + " had no strong reference and was weakly removed");
                //GS-7326: once there's no strong reference to the class loader that loaded this class
                //we should remove the stored class bytes to avoid getting to the protective code
                //exception that check that the same class is not loaded twice for the same service class
                //loader context
                removeClass(className);
            }

            return loader;
        }
    }

    /**
     * @return the {@link LRMIClassLoader} that is connected to the specified remote class loader or
     * null if none exists
     */
    public LRMIClassLoader getClassLoaderByRemoteId(LRMIRemoteClassLoaderIdentifier identifier) {
        synchronized (_stateLock) {
            WeakReference<LRMIClassLoader> lrmiClassLoaderRef = _classLoaderByRemoteId.get(identifier);
            if (lrmiClassLoaderRef == null)
                return null;
            LRMIClassLoader loader = lrmiClassLoaderRef.get();
            if (loader == null) {
                if (_logger.isTraceEnabled())
                    _logger.trace("LRMIClassLoader connected to: " + identifier + " had no strong reference and was weakly removed");
                _classLoaderByRemoteId.remove(identifier);
            }

            return loader;
        }
    }

    /**
     * Associate a {@link LRMIClassLoader} with the specified remote class loader id, if one
     * allready associated return it instead
     */
    public LRMIClassLoader putClassLoaderByRemoteId(LRMIRemoteClassLoaderIdentifier identifier, LRMIClassLoader lrmiClassLoader) {
        synchronized (_stateLock) {
            LRMIClassLoader previousClassLoader = getClassLoaderByRemoteId(identifier);
            if (previousClassLoader != null)
                return previousClassLoader;

            WeakReference<LRMIClassLoader> lrmiClassLoaderRef = new WeakReference<LRMIClassLoader>(lrmiClassLoader);
            _classLoaderByRemoteId.put(identifier, lrmiClassLoaderRef);
            return null;
        }
    }

    /**
     * Associate a {@link LRMIClassLoader} with the specified class name and definition
     *
     * @throws IllegalArgumentException if there's already a {@link LRMIClassLoader} associated with
     *                                  the specified class name
     */
    public LRMIClassLoader putClassBytesAndLoader(String className, LRMIClassLoader lrmiClassLoader, byte[] definition) {
        synchronized (_stateLock) {
            WeakReference<LRMIClassLoader> previousClassLoaderRef = _classLoaderByName.get(className);
            if (previousClassLoaderRef != null) {
                LRMIClassLoader previousLRMIClassLoader = previousClassLoaderRef.get();
                if (previousLRMIClassLoader != null)
                    return previousLRMIClassLoader;
                if (_logger.isTraceEnabled())
                    _logger.trace("previous LRMIClassLoader of class: " + className + " had no strong reference and was weakly removed");
            }

            WeakReference<LRMIClassLoader> lrmiClassLoaderRef = new WeakReference<LRMIClassLoader>(lrmiClassLoader);
            if (_logger.isTraceEnabled())
                _logger.trace("Associating class: " + className + " to class loader " + lrmiClassLoader);
            _classLoaderByName.put(className, lrmiClassLoaderRef);
            try {
                storeClassBytes(className, definition);
            } catch (RuntimeException e) {
                if (_logger.isTraceEnabled())
                    _logger.trace("error occurred while storing class bytes of class: " + className + " in " + this + ", removing class loader " + lrmiClassLoader + " association");
                _classLoaderByName.remove(className);
                throw e;
            }
            return null;
        }
    }

    /**
     * @return the class bytes for the specified class name or null if none exists
     */
    public byte[] getClassBytes(String className) {
        synchronized (_stateLock) {
            return _classBytesByName.get(className);
        }
    }

    /**
     * stores class bytes that represent the specified class name
     */
    public void storeClassBytes(String className, byte[] definition) {
        synchronized (_stateLock) {
            if (_classBytesByName.containsKey(className)) {
                String msg = toString() + ": attempting to add class bytes for class name " + className + " which already has mapped class bytes";
                if (_logger.isErrorEnabled()) {
                    _logger.error(msg);
                    _logger.showGlobalTrace();
                }
                if (_disableDuplicateLoadProtection)
                    _logger.warn(toString() + ": duplicate remote class loading protection is disabled, exception ignored");
                else
                    throw new IllegalArgumentException(msg);
            }
            if (_logger.isTraceEnabled())
                _logger.trace("Storing class bytes of class: " + className + " in " + this);
            _classBytesByName.put(className, definition);
        }
    }

    public void removeClass(String className) {
        synchronized (_stateLock) {
            if (_logger.isTraceEnabled())
                _logger.trace("Removing class: " + className + " from " + this);
            _classBytesByName.remove(className);
            _classLoaderByName.remove(className);
        }
    }

    @Override
    public String toString() {
        return "ServiceClassLoaderContext [" + _name + "]";
    }

    public void clear() {
        synchronized (_stateLock) {
            if (_logger.isTraceEnabled())
                _logger.trace("Clearing " + this);
            _classBytesByName.clear();
            _classLoaderByName.clear();
            _classLoaderByRemoteId.clear();
        }
    }
}
