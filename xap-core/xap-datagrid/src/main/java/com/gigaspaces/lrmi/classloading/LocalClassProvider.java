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
public class LocalClassProvider {
    final private static TraceableLogger _logger = TraceableLogger.getLogger(Constants.LOGGER_LRMI_CLASSLOADING);
    final public static long EXPORT_DISABLED_MARKER = -1;
    final public static long NULL_CLASS_LOADER_MARKER = -2;

    public static byte[] getClassDefinitionLocally(long id, String className, Class<? extends IClassProvider> iClassProviderClass) throws ClassNotFoundException {
        try {
            if (_logger.isLoggable(Level.FINE))
                _logger.fine("LocalClassProvider retrieving class definition [" + className + "] from class loader id " + id);
            ClassLoader loader = getClassLoaderInternal(id, iClassProviderClass);
            if (loader == null)
                throw new ClassNotFoundException("LocalClassProvider unknown class loader id [" + id + "]");
            String resourceName = className.replace('.', '/').concat(".class");
            InputStream stream = loader.getResourceAsStream(resourceName);
            if (stream != null) {
                if (_logger.isLoggable(Level.FINE))
                    _logger.fine("LocalClassProvider class definition [" + className + "] found at class loader id " + id);
                return toByteArray(stream);
            }
            throw new ClassNotFoundException("LocalClassProvider could not locate required class [" + className + "] at the specified class loader [" + id + "]");
        } catch (IOException e) {
            throw new ClassNotFoundException("LocalClassProvider class definition of " + className + " was not found at the specified class loader [" + id + "]", e);
        }
    }

    public static ClassLoader getClassLoaderById(long id, Class<? extends IClassProvider> iClassProviderClass){
        return getClassLoaderInternal(id, iClassProviderClass);
    }

    public static byte[] getResourceLocally(long id, String resourceName, Class<? extends IClassProvider> iClassProviderClass) throws ClassNotFoundException {
        try {
            if (_logger.isLoggable(Level.FINE))
                _logger.fine("LocalClassProvider retrieving resource [" + resourceName + "] class loader id " + id);
            ClassLoader loader = getClassLoaderInternal(id, iClassProviderClass);
            if (loader == null)
                throw new ClassNotFoundException("LocalClassProvider unknown class loader id [" + id + "]");
            InputStream stream = loader.getResourceAsStream(resourceName);
            if (stream != null) {
                if (_logger.isLoggable(Level.FINE))
                    _logger.fine("LocalClassProvider resource [" + resourceName + "] found at class loader id " + id);
                return toByteArray(stream);
            }

            if (_logger.isLoggable(Level.FINEST))
                _logger.finest("LocalClassProvider resource [" + resourceName + "] not found at class loader id " + id + ", trying lrmi class loaders that belong to the specified class loader");

            ServiceClassLoaderContext serviceClassLoaderContext = LRMIClassLoadersHolder.getServiceClassLoaderContext(loader);
            if (serviceClassLoaderContext != null) {
                byte[] classBytes = serviceClassLoaderContext.getClassBytes(resourceName);

                if (classBytes != null) {
                    if (_logger.isLoggable(Level.FINE))
                        _logger.fine("LocalClassProvider resource [" + resourceName + "] found at LRMIClassLoader's descendant of class loader id " + id);
                    return classBytes;
                }
                if (_logger.isLoggable(Level.FINE))
                    _logger.fine("LocalClassProvider could not locate required resource [" + resourceName + "] at the specified class loader [" + id);
            } else if (_logger.isLoggable(Level.FINE))
                _logger.fine("LocalClassProvider could not locate required resource [" + resourceName + "], no service context class loader exists for the specified class loader id [" + id + "]");

            return null;
        } catch (IOException e) {
            if (_logger.isLoggable(Level.FINE))
                _logger.log(Level.FINE, "LocalClassProvider caught exception while locating resource [" + resourceName + "] at the specified class loader [" + id + "]", e);
            return null;
        }
    }

    public static long putClassLoader(ClassLoader classLoader) {
        if (classLoader == null)
            return NULL_CLASS_LOADER_MARKER;

        return ClassLoaderCache.getCache().putClassLoader(classLoader);
    }
    
    private static ClassLoader getClassLoaderInternal(long id, Class<? extends IClassProvider> iClassProviderClass) {
        if (id == NULL_CLASS_LOADER_MARKER) {
            if (_logger.isLoggable(Level.FINEST))
                _logger.finest("LocalClassProvider using " + iClassProviderClass.getName() + " class loading class loader as the class loader");
            return iClassProviderClass.getClassLoader();
        }
        ClassLoader classLoader = ClassLoaderCache.getCache().getClassLoader(id);
        if (classLoader != null)
            return classLoader;
        if (_logger.isLoggable(Level.FINEST))
            _logger.finest("LocalClassProvider found no class loader with id [" + id + "], using context class loader instead");
        ClassLoader contextClassLoader = ClassLoaderHelper.getContextClassLoader();
        if (contextClassLoader != null) {
            return contextClassLoader;
        }
        if (_logger.isLoggable(Level.FINEST))
            _logger.finest("LocalClassProvider found no context class loader, using default class loader");
        return ReflectionUtil.class.getClassLoader();
    }

    private static byte[] toByteArray(InputStream stream) throws IOException {
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
}
