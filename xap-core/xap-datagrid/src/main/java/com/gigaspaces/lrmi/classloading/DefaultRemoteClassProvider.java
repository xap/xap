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
public class DefaultRemoteClassProvider implements IClassProvider {
    final private static TraceableLogger _logger = TraceableLogger.getLogger(Constants.LOGGER_LRMI_CLASSLOADING);
    private final String _identifier;
    final private boolean _enabled;

    public DefaultRemoteClassProvider(String identifier) {
        _identifier = identifier;
        _enabled = Boolean.parseBoolean(System.getProperty(SystemProperties.LRMI_CLASSLOADING, SystemProperties.LRMI_CLASSLOADING_DEFAULT))
                && Boolean.parseBoolean(System.getProperty(SystemProperties.LRMI_CLASSLOADING_EXPORT, SystemProperties.LRMI_CLASSLOADING_EXPORT_DEFAULT));

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
        if (!_enabled)
            throw new ClassNotFoundException(toString() + " LRMI class exporting is disabled");
        byte[] result;
        try {
            result = LocalClassProvider.getClassDefinitionLocally(id, className, getClass());
            if (result != null) {
                return result;
            }
        }  catch(ClassNotFoundException e){
            //do nothing here
        }
        if (_logger.isLoggable(Level.FINEST))
            _logger.finest(toString() +  "  class definition [" + className + "] not found at class loader id " + id + ", trying lrmi class loaders that belong to the specified class loader");
        result = getClassFromServiceClassLoaderContext(id,className);
        if (result != null) {
            return result;
        }
        throw new ClassNotFoundException(toString() + " could not locate required class [" + className + "] at the specified class loader [" + id + "]");
    }

    public byte[] getResource(long id, String resourceName) throws RemoteException {
        if (!_enabled)
            throw new RemoteException(toString() + " LRMI class exporting is disabled");
        byte[] result;
        try {
            result = LocalClassProvider.getResourceLocally(id, resourceName, getClass());
            if (result != null) {
                return result;
            }
        }  catch(ClassNotFoundException e){
            //do nothing here
        }
        if (_logger.isLoggable(Level.FINEST)) {
            _logger.finest(toString() +  "  resource [" + resourceName + "] not found at class loader id " + id + ", trying lrmi class loaders that belong to the specified class loader");
        }
        result = getResourceFromServiceClassLoaderContext(id, resourceName);
        if (result != null) {
            return result;
        }
        return null;
    }

    public long putClassLoader(ClassLoader classLoader) {
        if (!_enabled)
            return LocalClassProvider.EXPORT_DISABLED_MARKER;
        return LocalClassProvider.putClassLoader(classLoader);
    }

    private byte[] getClassFromServiceClassLoaderContext(long id, String className){
        ClassLoader loader = LocalClassProvider.getClassLoaderById(id, getClass());
        if(loader == null) {
            if (_logger.isLoggable(Level.FINE))
                _logger.fine(toString() +  " unknown class loader id [" + id + "]");
            return null;
        }
        ServiceClassLoaderContext serviceClassLoaderContext = LRMIClassLoadersHolder.getServiceClassLoaderContext(loader);
        if (serviceClassLoaderContext != null) {
            byte[] classBytes = serviceClassLoaderContext.getClassBytes(className);

            if (classBytes != null) {
                if (_logger.isLoggable(Level.FINE))
                    _logger.fine(toString() +  "  class definition [" + className + "] found at LRMIClassLoaders descendants of class loader id " + id);
                return classBytes;
            }
            if (_logger.isLoggable(Level.FINE))
                _logger.fine(toString() +  "  could not locate required class [" + className + "] at the specified class loader [" + id + "]");
        } else if (_logger.isLoggable(Level.FINE))
            _logger.fine(toString() +  "  could not locate required class [" + className + "], no service context class loader exists for the specified class loader id [" + id + "]");
        return null;
    }

    private byte[] getResourceFromServiceClassLoaderContext(long id, String resourceName) {
        ClassLoader loader = LocalClassProvider.getClassLoaderById(id, getClass());
        if (loader == null) {
            if (_logger.isLoggable(Level.FINE))
                _logger.fine(toString() +  " unknown class loader id [" + id + "]");
            return null;
        }
        ServiceClassLoaderContext serviceClassLoaderContext = LRMIClassLoadersHolder.getServiceClassLoaderContext(loader);
        if (serviceClassLoaderContext != null) {
            byte[] classBytes = serviceClassLoaderContext.getClassBytes(resourceName);

            if (classBytes != null) {
                if (_logger.isLoggable(Level.FINE))
                    _logger.fine(toString() +  "  resource [" + resourceName + "] found at LRMIClassLoader's descendant of class loader id " + id);
                return classBytes;
            }
            if (_logger.isLoggable(Level.FINE))
                _logger.fine(toString() +  "  could not locate required resource [" + resourceName + "] at the specified class loader [" + id);
        } else if (_logger.isLoggable(Level.FINE))
            _logger.fine(toString() +  " could not locate required resource [" + resourceName + "], no service context class loader exists for the specified class loader id [" + id + "]");
        return null;
    }

    @Override
    public String toString() {
        return "DefaultRemoteClassProvider [" + _identifier + "]";
    }
}
