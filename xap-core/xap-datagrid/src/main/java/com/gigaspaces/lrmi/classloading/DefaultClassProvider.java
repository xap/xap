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

import com.gigaspaces.internal.classloader.ClassLoaderCache;
import com.gigaspaces.internal.reflection.ReflectionUtil;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.logger.TraceableLogger;
import com.j_spaces.kernel.ClassLoaderHelper;
import com.j_spaces.kernel.SystemProperties;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.rmi.RemoteException;
import java.util.logging.Level;

/**
 * The default remote implementation used to retrieve class definition or resources from a remote
 * JVM.
 *
 * @author assafr
 * @since 6.6
 */
@com.gigaspaces.api.InternalApi
public class DefaultClassProvider implements IClassProvider {
    final private static TraceableLogger _logger = TraceableLogger.getLogger(Constants.LOGGER_LRMI_CLASSLOADING);

    private static final long EXPORT_DISABLED_MARKER = -1;

    private static final long NULL_CLASS_LOADER_MARKER = -2;

    private final String _identifier;

    final private boolean _enabled;


    public DefaultClassProvider(String identifier) {
        _identifier = identifier;

        _enabled = Boolean.parseBoolean(System.getProperty(SystemProperties.LRMI_CLASSLOADING, "true")) && Boolean.parseBoolean(System.getProperty(SystemProperties.LRMI_CLASSLOADING_EXPORT, "true"));

        if (_enabled) {
            if (_logger.isLoggable(Level.FINE))
                _logger.fine(toString() + " LRMI class exporting enabled");
        } else {
            if (_logger.isLoggable(Level.FINE))
                _logger.fine(toString() + " LRMI class exporting disabled");
        }

        if (_enabled) {
            if (_logger.isLoggable(Level.FINEST))
                _logger.finest(toString() + " class provider initialized");
        }
    }

    public byte[] getClassDefinition(long id, String className) throws ClassNotFoundException {
        try {
            if (!_enabled)
                throw new ClassNotFoundException(toString() + " LRMI class exporting is disabled");

            if (_logger.isLoggable(Level.FINE))
                _logger.fine(toString() + " retrieving class definition [" + className + "] from class loader id " + id);
            ClassLoader loader = getClassLoaderInternal(id);

            if (loader == null)
                throw new ClassNotFoundException(toString() + " unknown class loader id [" + id + "]");

            String resourceName = className.replace('.', '/').concat(".class");
            InputStream stream = loader.getResourceAsStream(resourceName);
            if (stream != null) {
                if (_logger.isLoggable(Level.FINE))
                    _logger.fine(toString() + " class definition [" + className + "] found at class loader id " + id);
                return toByteArray(stream);
            }

            if (_logger.isLoggable(Level.FINEST))
                _logger.finest(toString() + " class definition [" + className + "] not found at class loader id " + id + ", trying lrmi class loaders that belong to the specified class loader");

            ServiceClassLoaderContext serviceClassLoaderContext = LRMIClassLoadersHolder.getServiceClassLoaderContext(loader);
            if (serviceClassLoaderContext != null) {
                byte[] classBytes = serviceClassLoaderContext.getClassBytes(className);

                if (classBytes != null) {
                    if (_logger.isLoggable(Level.FINE))
                        _logger.fine(toString() + " class definition [" + className + "] found at LRMIClassLoaders decedents of class loader id " + id);
                    return classBytes;
                }
                if (_logger.isLoggable(Level.FINE))
                    _logger.fine(toString() + " could not locate required class [" + className + "] at the specified class loader [" + id + "]");
            } else if (_logger.isLoggable(Level.FINE))
                _logger.fine(toString() + " could not locate required class [" + className + "], no service context class loader exists for the specified class loader id [" + id + "]");

            throw new ClassNotFoundException(toString() + " could not locate required class [" + className + "] at the specified class loader [" + id + "]");
        } catch (IOException e) {
            throw new ClassNotFoundException(toString() + " class definition of " + className + " was not found at the specified class loader [" + id + "]", e);
        }
    }

    public byte[] getResource(long id, String resourceName) throws RemoteException {
        try {
            if (!_enabled)
                throw new RemoteException(toString() + " LRMI class exporting is disabled");

            if (_logger.isLoggable(Level.FINE))
                _logger.fine(toString() + " retrieving resource [" + resourceName + "] class loader id " + id);
            ClassLoader loader = getClassLoaderInternal(id);

            if (loader == null)
                throw new RemoteException(toString() + " unknown class loader id [" + id + "]");

            InputStream stream = loader.getResourceAsStream(resourceName);
            if (stream != null) {
                if (_logger.isLoggable(Level.FINE))
                    _logger.fine(toString() + " resource [" + resourceName + "] found at class loader id " + id);
                return toByteArray(stream);
            }

            if (_logger.isLoggable(Level.FINEST))
                _logger.finest(toString() + " resource [" + resourceName + "] not found at class loader id " + id + ", trying lrmi class loaders that belong to the specified class loader");

            ServiceClassLoaderContext serviceClassLoaderContext = LRMIClassLoadersHolder.getServiceClassLoaderContext(loader);
            if (serviceClassLoaderContext != null) {
                byte[] classBytes = serviceClassLoaderContext.getClassBytes(resourceName);

                if (classBytes != null) {
                    if (_logger.isLoggable(Level.FINE))
                        _logger.fine(toString() + " resource [" + resourceName + "] found at LRMIClassLoader's descendant of class loader id " + id);
                    return classBytes;
                }
                if (_logger.isLoggable(Level.FINE))
                    _logger.fine(toString() + " could not locate required resource [" + resourceName + "] at the specified class loader [" + id);
            } else if (_logger.isLoggable(Level.FINE))
                _logger.fine(toString() + " could not locate required resource [" + resourceName + "], no service context class loader exists for the specified class loader id [" + id + "]");

            return null;
        } catch (IOException e) {
            if (_logger.isLoggable(Level.FINE))
                _logger.log(Level.FINE, toString() + " exception caught while locating resource [" + resourceName + "] at the specified class loader [" + id + "]", e);
            return null;
        }
    }

    public long putClassLoader(ClassLoader classLoader) {
        if (!_enabled)
            return EXPORT_DISABLED_MARKER;
        if (classLoader == null)
            return NULL_CLASS_LOADER_MARKER;

        return ClassLoaderCache.getCache().putClassLoader(classLoader);
    }

    @Override
    public String toString() {
        return "DefaultClassProvider [" + _identifier + "]";
    }

    private ClassLoader getClassLoaderInternal(long id) {
        if (!_enabled) {
            if (_logger.isLoggable(Level.FINEST))
                _logger.finest(toString() + " class exporting is disabled");
            return null;
        }

        if (id == NULL_CLASS_LOADER_MARKER) {
            if (_logger.isLoggable(Level.FINEST))
                _logger.finest(toString() + " using DefaultClassProvider class loading class loader as the class loader");
            return DefaultClassProvider.class.getClassLoader();
        }

        ClassLoader classLoader = ClassLoaderCache.getCache().getClassLoader(id);
        if (classLoader != null)
            return classLoader;
        if (_logger.isLoggable(Level.FINEST))
            _logger.finest(toString() + " no class loader found with id [" + id + "], using context class loader instead");
        ClassLoader contextClassLoader = ClassLoaderHelper.getContextClassLoader();
        if (contextClassLoader != null) {
            return contextClassLoader;
        }
        if (_logger.isLoggable(Level.FINEST))
            _logger.finest(toString() + " no context class loader found, using default class loader");
        return ReflectionUtil.class.getClassLoader();
    }

    private byte[] toByteArray(InputStream stream) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(4096);
        try {
            byte[] buffer = new byte[4096];
            int bytesRead = -1;
            while ((bytesRead = stream.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
            out.flush();
        } finally {
            try {
                stream.close();
            } catch (IOException ex) {
                // ignore this exception
            }
            try {
                out.close();
            } catch (IOException ex) {
                // ignore this exception
            }
        }

        return out.toByteArray();
    }

    public void clearClassLoader(ClassLoader classLoader)
            throws RemoteException {
        //Do nothing, here due to BW compatibility with 7.0
    }

}
