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

import com.gigaspaces.internal.metadata.SpaceTypeInfo;
import com.gigaspaces.internal.metadata.SpaceTypeInfoRepository;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.logger.TraceableLogger;
import com.gigaspaces.lrmi.classloading.protocol.lrmi.LRMIConnection;
import com.j_spaces.kernel.SystemProperties;

import org.jini.rio.boot.LoggableClassLoader;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.rmi.ConnectException;
import java.rmi.RemoteException;
import java.util.logging.Level;

/**
 * Acts as a local class loader that is connected to a remote class loader using LRMI to load new
 * classes. Each LRMIClassLoader is connected to a specific LRMIRuntime and in that runtime to a
 * specific local ClassLoader
 *
 * @author eitany
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class LRMIClassLoader extends URLClassLoader implements LoggableClassLoader {
    final private static TraceableLogger _logger = TraceableLogger.getLogger(Constants.LOGGER_LRMI_CLASSLOADING);
    final static public boolean FAIL_TO_CURRENT_CONNECTION_CLASS_LOADER = Boolean.valueOf(System.getProperty(SystemProperties.LRMI_FAIL_TO_GET_CLASS_BYTES_FROM_ACTIVE_CONNECTION, String.valueOf(SystemProperties.LRMI_FAIL_TO_GET_CLASS_BYTES_FROM_ACTIVE_CONNECTION_DEFAULT)));

    private final IClassProvider _remoteClassProvider;
    private final long _remoteClassLoaderId;
    private final long _remoteRemoteLrmiId;

    private final ServiceClassLoaderContext _serviceClassLoaderContext;

    public LRMIClassLoader(IClassProvider classProvider, ClassLoader parent, ServiceClassLoaderContext serviceClassLoaderContext, long remoteRemoteLrmiId, long remoteClassLoaderId) {
        super(new URL[0], parent);
        _remoteClassProvider = classProvider;
        _serviceClassLoaderContext = serviceClassLoaderContext;
        _remoteRemoteLrmiId = remoteRemoteLrmiId;
        _remoteClassLoaderId = remoteClassLoaderId;
    }

    @Override
    protected Class<?> findClass(String className) throws ClassNotFoundException {
        synchronized (_serviceClassLoaderContext) {
            try {
                if (_logger.isLoggable(Level.FINE))
                    _logger.fine(this.toString() + " trying to find class: " + className);

                if (_logger.isLoggable(Level.FINEST))
                    _logger.finest(this.toString() + " trying to find class locally: " + className);

                Class<?> loadedClass = findLoadedClass(className);
                if (loadedClass != null) {
                    if (_logger.isLoggable(Level.FINE))
                        _logger.fine(this.toString() + " class found locally: " + className);
                    return loadedClass;
                }

                if (_logger.isLoggable(Level.FINEST))
                    _logger.finest(this.toString() + " failed previous attempt, trying to find class at the service context class loaders: " + className);
                LRMIClassLoader brotherClassLoader = _serviceClassLoaderContext.getClassLoaderByClassName(className);
                if (brotherClassLoader != null) {
                    if (_logger.isLoggable(Level.FINE))
                        _logger.fine(this.toString() + " class found at the service context class loader [" + brotherClassLoader + "]: " + className);
                    return brotherClassLoader.findClass(className);
                }

                if (_logger.isLoggable(Level.FINEST))
                    _logger.finest(this.toString() + " failed previous attempt, trying to retrieve class remotely using the class provider class: " + className);

                byte[] definition;
                try {
                    definition = _remoteClassProvider.getClassDefinition(_remoteClassLoaderId, className);
                } catch (ConnectException e) {
                    definition = loadBytesFromCurrentConnection(className);
                }
                if (definition == null) {
                    if (_logger.isLoggable(Level.FINE))
                        _logger.fine(this.toString() + " failed to get class definition from its remote class provider: " + className);
                    throw new ClassNotFoundException("class " + className + " not found");
                }


                if (_logger.isLoggable(Level.FINE))
                    _logger.fine(this.toString() + " class found remotely using the class provider class: " + className);

                return defineClass(className, definition);
            } catch (RemoteException e) {
                if (_logger.isLoggable(Level.FINE))
                    _logger.log(Level.FINE, this.toString() + " exception caught while retrieving class definition from remote class provider: " + className, e);
                throw new ClassNotFoundException("class " + className + " not found", e);
            } catch (ClassNotFoundException e) {
                if (_logger.isLoggable(Level.FINE))
                    _logger.log(Level.FINE, this.toString() + " ClassNotFoundException caught while retrieving class definition from remote class provider: " + className, e);
                throw e;
            }
        }
    }

    private byte[] loadBytesFromCurrentConnection(String className) {
        try {
            if (!FAIL_TO_CURRENT_CONNECTION_CLASS_LOADER) {
                return null;
            }
            if (_logger.isLoggable(Level.FINE))
                _logger.fine(this.toString() + " attempting to load class: " + className + " from current connection remote class provider");
            IRemoteClassProviderProvider classProviderProvider = LRMIConnection.getClassProviderProvider();
            LRMIRemoteClassLoaderIdentifier id = classProviderProvider.getRemoteClassLoaderIdentifier();
            IClassProvider classProvider = classProviderProvider.getClassProvider();
            return classProvider.getClassDefinition(id.getRemoteClassLoaderId(), className);
        } catch (Exception e) {
            if (_logger.isLoggable(Level.FINE))
                _logger.fine(this.toString() + " failed to get class definition from current remote class provider: " + className);
        }
        return null;
    }

    /**
     * Defines class from bytecode, in the current classloader
     */
    private Class<?> defineClass(String className, byte[] definition)
            throws ClassFormatError {
        if (_logger.isLoggable(Level.FINE))
            _logger.fine(this.toString() + " defining class: " + className);

        try {
            Class<?> defineClass = defineClass(className, definition, 0, definition.length);
            LRMIClassLoader previousClassLoader = _serviceClassLoaderContext.putClassBytesAndLoader(className, this, definition);
            if (previousClassLoader != null)
                throw new IllegalStateException("Class: " + className + " is already loaded in this service by LRMIClassLoader " + previousClassLoader);

            if(_logger.isLoggable(Level.FINEST)){
                _logger.finest("Defined class ["+className+"], it's class-loader hierarchy is "
                        + ClassLoaderUtility.getClassLoaderHierarchy(defineClass.getClassLoader()));
            }
            return defineClass;
        } catch (ClassFormatError e) {
            if (_logger.isLoggable(Level.SEVERE))
                _logger.log(Level.SEVERE, this.toString() + " class format error caught while defining class: " + className, e);
            throw e;
        }

    }

    @Override
    public InputStream getResourceAsStream(String resourceName) {
        synchronized (_serviceClassLoaderContext) {
            try {
                if (_logger.isLoggable(Level.FINE))
                    _logger.fine(this.toString() + " trying to get resource as stream from service context: " + resourceName);
                byte[] resource = _serviceClassLoaderContext.getClassBytes(resourceName);
                if (resource == null) {
                    if (_logger.isLoggable(Level.FINE))
                        _logger.fine(this.toString() + " failed previous attempt, trying to get resource remotely from class provider class: " + resourceName);
                    resource = _remoteClassProvider.getResource(_remoteClassLoaderId, resourceName);
                    if (resource != null) {
                        if (_logger.isLoggable(Level.FINE))
                            _logger.fine(this.toString() + " resource stream found at service context: " + resourceName);
                        _serviceClassLoaderContext.storeClassBytes(resourceName, resource);
                    }
                }

                if (resource != null) {
                    if (_logger.isLoggable(Level.FINE))
                        _logger.fine(this.toString() + " resource stream found locally: " + resourceName);
                    return new ByteArrayInputStream(resource);
                }
                resource = loadResourceFromCurrentConnection(resourceName);
                if (resource != null) {
                    if (_logger.isLoggable(Level.FINE)) {
                        _logger.fine(this.toString() + " resource stream found using current lrmi connection: " + resourceName);
                    }
                    _serviceClassLoaderContext.storeClassBytes(resourceName, resource);
                    return new ByteArrayInputStream(resource);
                } else if (_logger.isLoggable(Level.FINE)) {
                    _logger.fine(this.toString() + " Failed getting resource from remote class provider: " + resourceName);
                }


            } catch (RemoteException e) {
                if (_logger.isLoggable(Level.FINE))
                    _logger.log(Level.FINE, this.toString() + " Exception caught while getting resource from remote class provider: " + resourceName, e);
            }
            return null;
        }
    }

    private byte[] loadResourceFromCurrentConnection(String resourceName) {
        try {
            if (!FAIL_TO_CURRENT_CONNECTION_CLASS_LOADER) {
                return null;
            }
            if (_logger.isLoggable(Level.FINE))
                _logger.fine(this.toString() + " attempting to load resource: " + resourceName + " from current connection remote class provider");
            IRemoteClassProviderProvider classProviderProvider = LRMIConnection.getClassProviderProvider();
            LRMIRemoteClassLoaderIdentifier id = classProviderProvider.getRemoteClassLoaderIdentifier();
            IClassProvider classProvider = classProviderProvider.getClassProvider();
            return classProvider.getResource(id.getRemoteClassLoaderId(), resourceName);
        } catch (Exception e) {
            if (_logger.isLoggable(Level.FINE))
                _logger.fine(this.toString() + " failed to get resource definition from current remote class provider: " + resourceName);
        }
        return null;

    }

    @Override
    public String toString() {
        return getLogName();
    }

    public String getLogName() {
        return "LRMIClassLoader [remote lrmi runtime id = " + _remoteRemoteLrmiId + ", remote class loader id = " + _remoteClassLoaderId + "] " + super.toString();
    }

    /**
     * This is not guarded by a lock to a void a deadlock since this is called from a parent service
     * class loader
     *
     * @param className the name of the class to load.
     * @return the loaded class.
     */
    public Class<?> getLoadedClass(String className) throws ClassNotFoundException {
        Class<?> loadedClass = null;
        for (int i = 0; i < 10; i++) {
            try {
                loadedClass = findLoadedClass(className);
                break;
            } catch (Throwable t) {
                //We are not sure if findLoadedClass is thread safe, use this hack to retry if an exception is thrown
                _logger.log(Level.WARNING, this.toString() + " Failed to find loaded class [" +
                        className + "] attempt = " + i, t);
            }
        }
        if (loadedClass != null)
            return loadedClass;

        throw new ClassNotFoundException("Attempting to get a loaded class which was not previously loaded [" + className + "]");
    }

}