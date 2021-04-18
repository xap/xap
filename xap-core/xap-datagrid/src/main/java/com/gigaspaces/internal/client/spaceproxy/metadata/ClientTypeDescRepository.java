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

package com.gigaspaces.internal.client.spaceproxy.metadata;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.reflection.ReflectionUtil;
import com.gigaspaces.internal.server.space.tiered_storage.error.TieredStorageMetadataException;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITransportPacket;
import com.gigaspaces.internal.utils.ClassLoaderUtils;
import com.gigaspaces.internal.utils.collections.CopyOnUpdateMap;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.j_spaces.core.UidQueryPacket;
import com.j_spaces.core.UnknownTypeException;
import com.j_spaces.core.client.ExternalEntry;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.kernel.ClassLoaderHelper;
import net.jini.core.entry.Entry;
import org.jini.rio.boot.CodeChangeClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.Map;

@com.gigaspaces.api.InternalApi
public class ClientTypeDescRepository {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_METADATA);

    private final IDirectSpaceProxy _spaceProxy;
    private final Map<String, ITypeDesc> _typeMap;
    private final TypeDescFactory _typeDescFactory;
    private final Object _lock;
    private final String _defaultCodebase;

    public ClientTypeDescRepository(IDirectSpaceProxy spaceProxy) {
        logEnter("ClientTypeDescRepository.ctor", "spaceProxy.getClientID()", spaceProxy.getClientID());

        this._spaceProxy = spaceProxy;
        this._typeMap = new CopyOnUpdateMap<String, ITypeDesc>();
        this._typeDescFactory = new TypeDescFactory(spaceProxy);
        this._lock = new Object();
        this._defaultCodebase = _spaceProxy.getProxySettings().getCodebase();

        logExit("ClientTypeDescRepository.ctor", "spaceProxy.getClientID()", spaceProxy.getClientID());
    }

    public void registerTypeDesc(ITypeDesc typeDesc) {
        if (typeDesc == null)
            throw new IllegalArgumentException("Argument cannot be null - 'typeDesc'.");

        final String typeName = typeDesc.getTypeName();
        logEnter("ClientTypeDescRepository.registerTypeDesc", "typeName", typeName);

        final ITypeDesc oldTypeDesc = _typeMap.get(typeName);
        if (oldTypeDesc != typeDesc) {
            synchronized (_lock) {
                registerTypeDesc(typeName, typeDesc, false /*ignoreException*/);
            }
        }

        logExit("ClientTypeDescRepository.registerTypeDesc", "typeName", typeName);
    }

    public void remove(String typeName) {
        logEnter("ClientTypeDescRepository.remove", "typeName", typeName);

        synchronized (_lock) {
            _typeMap.remove(typeName);
            if (_logger.isDebugEnabled())
                _logger.debug("Removed type '" + typeName + "' from proxy type manager.");
        }

        logExit("ClientTypeDescRepository.remove", "typeName", typeName);
    }

    public void clear() {
        logEnter("ClientTypeDescRepository.clear", "", "");

        synchronized (_lock) {
            _typeMap.clear();
            if (_logger.isDebugEnabled())
                _logger.debug("Cleared all types from proxy type manager.");
        }

        logExit("ClientTypeDescRepository.clear", "", "");
    }

    public ITypeDesc getTypeDescIfExistsInProxy(String typeName) {
        logEnter("ClientTypeDescRepository.getTypeDescIfExistsInProxy", "typeName", typeName);

        final ITypeDesc typeDesc = _typeMap.get(typeName);

        if (typeDesc == null && _logger.isDebugEnabled())
            _logger.debug("Proxy type manager does not contain type descriptor for type '" + typeName + "'.");

        logExit("ClientTypeDescRepository.getTypeDescIfExistsInProxy", "typeName", typeName);
        return typeDesc;
    }

    public ITypeDesc getTypeDescIfExistsInServer(String typeName) {
        logEnter("ClientTypeDescRepository.getTypeDescIfExistsInServer", "typeName", typeName);

        try {
            // Get type descriptor from cache, if available:
            ITypeDesc typeDesc = _typeMap.get(typeName);
            if (isValid(typeDesc))
                return typeDesc;

            // Get a lock to protect type loading:
            synchronized (_lock) {
                // Check cache again, now that we're in a synchronized context:
                typeDesc = _typeMap.get(typeName);
                if (isValid(typeDesc))
                    return typeDesc;
                if (_logger.isDebugEnabled())
                    _logger.debug("Proxy type manager does not contain type descriptor for type '" + typeName + "', searching server.");

                // Get type descriptor from server:
                typeDesc = _spaceProxy.getTypeDescFromServer(typeName);
                // If type descriptor is valid, register it in cache:
                if (isValid(typeDesc))
                    _typeMap.put(typeName, typeDesc);
                else if (_logger.isDebugEnabled())
                    _logger.debug("Server type manager does not contain type descriptor for type '" + typeName + "'.");

                // return result:
                return typeDesc;
            }
        } finally {
            logExit("ClientTypeDescRepository.getTypeDescIfExistsInServer", "typeName", typeName);
        }
    }

    public ITypeDesc getTypeDescByName(String typeName, String codebase) {
        logEnter("ClientTypeDescRepository.getTypeDescByName", "typeName", typeName);

        try {
            // Get type descriptor from cache, if available:
            ITypeDesc typeDesc = _typeMap.get(typeName);
            if (isValid(typeDesc))
                return typeDesc;

            // Get a lock to protect type loading:
            synchronized (_lock) {
                // Check cache again, now that we're in a synchronized context:
                typeDesc = _typeMap.get(typeName);
                if (isValid(typeDesc))
                    return typeDesc;

                // Load type descriptor:
                typeDesc = loadTypeDesc(typeName, codebase, null /*externalEntry*/);
                // Register type descriptor in cache:
                registerTypeDesc(typeName, typeDesc, true /*ignoreException*/);
                // return result:
                return typeDesc;
            }
        } finally {
            logExit("ClientTypeDescRepository.getTypeDescByName", "typeName", typeName);
        }
    }

    @SuppressWarnings("deprecation")
    public ITypeDesc getTypeDescByExternalEntry(ExternalEntry externalEntry) {
        final String typeName = externalEntry.getClassName();
        if (typeName == null || typeName.length() == 0)
            throw new IllegalArgumentException("Cannot get type descriptor from external entry because the class name was not set.");

        logEnter("ClientTypeDescRepository.getTypeDescByExternalEntry", "typeName", typeName);

        try {
            // Get type descriptor from cache, if available:
            ITypeDesc typeDesc = _typeMap.get(typeName);
            if (isValid(typeDesc))
                return typeDesc;

            // Get a lock to protect type loading:
            synchronized (_lock) {
                // Check cache again, now that we're in a synchronized context:
                typeDesc = _typeMap.get(typeName);
                if (isValid(typeDesc))
                    return typeDesc;

                // Load type descriptor:
                typeDesc = loadTypeDesc(typeName, externalEntry.getCodebase(), externalEntry);
                // Cache loaded type descriptor:
                registerTypeDesc(typeName, typeDesc, true /*ignoreException*/);
                // return result:
                return typeDesc;
            }
        } finally {
            logExit("ClientTypeDescRepository.getTypeDescByExternalEntry", "typeName", typeName);
        }
    }

    public ITypeDesc getTypeDescByJavaObject(Object object, ObjectType objectType) {
        final Class<?> type = object.getClass();
        final String typeName = type.getName();

        logEnter("ClientTypeDescRepository.getTypeDescByJavaObject", "typeName", typeName);

        try {
            // Get type descriptor from cache, if available:
            ITypeDesc typeDesc = _typeMap.get(typeName);
            if (isValid(typeDesc) && hasRequiredIntrospector(typeDesc, objectType))
                return typeDesc;

            // Get a lock to protect type loading:
            synchronized (_lock) {
                // Check cache again, now that we're in a synchronized context:
                typeDesc = _typeMap.get(typeName);
                if (isValid(typeDesc) && hasRequiredIntrospector(typeDesc, objectType))
                    return typeDesc;

                if (_logger.isDebugEnabled())
                    _logger.debug("Proxy type manager does not contain type descriptor for type '" + typeName + "', creating type descriptor from " + objectType + ".");

                final String codebase = _defaultCodebase;
                switch (objectType) {
                    case POJO:
                        typeDesc = loadPojoTypeDesc(type, codebase);
                        break;
                    case ENTRY:
                        typeDesc = _typeDescFactory.createEntryTypeDesc((Entry) object, type.getName(), codebase, type);
                        break;
                    default:
                        throw new UnsupportedOperationException("This operation is not supported for ObjectType " + objectType);
                }

                registerTypeDesc(typeName, typeDesc, true /*ignoreException*/);
                return typeDesc;
            }
        } finally {
            logExit("ClientTypeDescRepository.getTypeDescByJavaObject", "typeName", typeName);
        }
    }

    public ITypeDesc getTypeDescByObject(Object object, ObjectType objectType) {
        logEnter("ClientTypeDescRepository.getTypeDescByObject", "objectType", objectType);

        try {
            if (objectType.isConcrete())
                return getTypeDescByJavaObject(object, objectType);

            switch (objectType) {
                case DOCUMENT:
                    return getTypeDescByName(((SpaceDocument) object).getTypeName(), null);
                case EXTERNAL_ENTRY:
                    return getTypeDescByExternalEntry((ExternalEntry) object);
                case ENTRY_PACKET:
                case TEMPLATE_PACKET:
                    IEntryPacket packet = (IEntryPacket) object;
                    loadTypeDescToPacket(packet);
                    return packet.getTypeDescriptor();
                default:
                    throw new IllegalArgumentException("Unsupported object type: " + objectType);
            }
        } finally {
            logExit("ClientTypeDescRepository.getTypeDescByObject", "objectType", objectType);
        }
    }

    public ITypeDesc getTypeDescBySQLQuery(SQLQuery<?> sqlQuery) {
        final String typeName = sqlQuery.getTypeName();
        logEnter("ClientTypeDescRepository.getTypeDescBySQLQuery", "typeName", typeName);

        try {
            final Object template = sqlQuery.getObject();
            if (template == null)
                return getTypeDescByName(sqlQuery.getTypeName(), null);
            return getTypeDescByObject(template, ObjectType.fromObject(template));
        } finally {
            logExit("ClientTypeDescRepository.getTypeDescBySQLQuery", "typeName", sqlQuery.getTypeName());
        }
    }

    public void loadTypeDescToPacket(ITransportPacket packet) {
        final String typeName = packet.getTypeName();

        logEnter("ClientTypeDescRepository.loadTypeDescToPacket", "typeName", typeName);

        try {
            if (typeName == null) {
                if (packet instanceof UidQueryPacket)
                    return;
                throw new IllegalArgumentException("packet.getClassName() cannot be null. packet type: " + packet.getClass().getName());
            }

            final ITypeDesc packetTypeDesc = packet.getTypeDescriptor();
            ITypeDesc typeDesc = null;

            try {
                // Get type descriptor from cache, if available:
                typeDesc = _typeMap.get(typeName);
                if (isValid(typeDesc))
                    return;

                // Get a lock to protect type loading:
                synchronized (_lock) {
                    // Check cache again, now that we're in a synchronized context:
                    typeDesc = _typeMap.get(typeName);
                    if (isValid(typeDesc))
                        return;

                    // Get type descriptor from packet, if available:
                    if (_logger.isDebugEnabled())
                        _logger.debug("Proxy type manager does not contain type descriptor for type '" + typeName + "', using packet's type descriptor.");
                    typeDesc = packetTypeDesc;
                    // Otherwise, load by type name:
                    if (typeDesc == null) {
                        if (_logger.isDebugEnabled())
                            _logger.debug("Packet's type descriptor is null, loading type '" + typeName + "' by name.");
                        typeDesc = getTypeDescByName(typeName, null);
                    }

                    // Register new type descriptor:
                    registerTypeDesc(typeName, typeDesc, true /*ignoreException*/);
                }
            } finally {
                // If type descriptor is not the same as packet's type descriptor, update:
                if (typeDesc != null && typeDesc != packetTypeDesc) {
                    if (packetTypeDesc != null) {
                        // 	TODO: Verify typeDesc does not contradict packetTypeDesc.
                    }
                    packet.setTypeDesc(typeDesc, packet.isSerializeTypeDesc());
                }
            }
        } finally {
            logExit("ClientTypeDescRepository.loadTypeDescToPacket", "typeName", typeName);
        }
    }

    public String getCommonSuperTypeName(ITypeDesc typeDesc1, ITypeDesc typeDesc2) {
        if (typeDesc1 == null || typeDesc2 == null)
            return null;
        if (typeDesc1 == typeDesc2 || typeDesc1.getTypeName().equals(typeDesc2.getTypeName()))
            return typeDesc1.getTypeName();

        String[] types1 = typeDesc1.getSuperClassesNames();
        String[] types2 = typeDesc2.getSuperClassesNames();
        String result = null;
        for (int i1 = types1.length - 1, i2 = types2.length - 1; i1 >= 0 && i2 >= 0 && types1[i1].equals(types2[i2]); i1--, i2--)
            result = types1[i1];

        return result;
    }

    private ITypeDesc loadPojoTypeDesc(Class<?> type, String codebase) {
        final String typeName = type.getName();
        logEnter("ClientTypeDescRepository.loadPojoTypeDesc", "typeName", typeName);

        try {
            // Check if type already has a typeDesc in cache:
            ITypeDesc typeDesc = _typeMap.get(type.getName());
            if (isValid(typeDesc) && typeDesc.isConcreteType())
                return typeDesc;

            if (_logger.isDebugEnabled())
                _logger.debug("Proxy type manager does not contain type descriptor for POJO class '" + typeName + "', creating it.");


            if (_spaceProxy.isEmbedded() &&
                    ClassLoaderUtils.isUnderClassLoader(type, CodeChangeClassLoader.class) &&
                    _spaceProxy.getSpaceImplIfEmbedded().getEngine().getTypeManager().getServerTypeDesc(type.getName()) == null) {
                throw new UnsupportedOperationException("Cannot introduce Class [" + type.getName() + "] to the space because it was loaded via a class annotated with @SupportCodeChange");
            }

            // Get super type:
            Class<?> superType = type.getSuperclass();
            // Get super type desc recursively:
            ITypeDesc superTypeDesc = superType == null ? null : loadPojoTypeDesc(superType, codebase);
            // Create type desc from type with super type desc:
            typeDesc = _typeDescFactory.createPojoTypeDesc(type, codebase, superTypeDesc);
            // Cache created type desc:
            registerTypeDesc(typeDesc.getTypeName(), typeDesc, true /*ignoreException*/);

            return typeDesc;
        } finally {
            logExit("ClientTypeDescRepository.loadPojoTypeDesc", "typeName", typeName);
        }
    }

    @SuppressWarnings("deprecation")
    private ITypeDesc loadTypeDesc(String typeName, String codebase, ExternalEntry externalEntry) {
        logEnter("ClientTypeDescRepository.loadTypeDesc", "typeName", typeName);

        try {
            if (codebase == null)
                codebase = _defaultCodebase;

            if (_logger.isDebugEnabled())
                _logger.debug("Proxy type manager does not contain type descriptor for type '" + typeName + "', searching for a java class by the same name.");

            // If type represents a concrete java class, create a type descriptor based on the class:
            final Class<?> realClass = getRealClass(typeName, codebase);
            if (realClass != null) {
                if (_logger.isDebugEnabled())
                    _logger.debug("Java class '" + typeName + "' was found, creating space type descriptor.");

                if (Entry.class.isAssignableFrom(realClass))
                    return _typeDescFactory.createEntryTypeDesc(null, typeName, codebase, realClass);
                return loadPojoTypeDesc(realClass, codebase);
            }

            if (_logger.isDebugEnabled())
                _logger.debug("Java class '" + typeName + "' was not found, searching server.");

            // Otherwise, get type descriptor from server, if available:
            final ITypeDesc serverTypeDesc = _spaceProxy.getTypeDescFromServer(typeName);
            if (isValid(serverTypeDesc))
                return serverTypeDesc;

            // Otherwise, create type descriptor from external entry, if available:
            if (externalEntry != null) {
                if (_logger.isDebugEnabled())
                    _logger.debug("Server type manager does not contain type descriptor for type '" + typeName + "', loading type descriptor from ExternalEntry.");

                return _typeDescFactory.createExternalEntryTypeDesc(externalEntry, codebase);
            }

            if (_logger.isDebugEnabled())
                _logger.debug("Server type manager does not contain type descriptor for type '" + typeName + "'.");

            // Otherwise, throw an exception:
            String message = "Failed to load space type descriptor for type '" + typeName + "'.";
            throw new SpaceMetadataException(message, new UnknownTypeException(message, typeName));
        } finally {
            logExit("ClientTypeDescRepository.loadTypeDesc", "typeName", typeName);
        }
    }

    private void registerTypeDesc(String typeName, ITypeDesc typeDesc, boolean ignoreException) {
        logEnter("ClientTypeDescRepository.registerTypeDesc", "typeName", typeName);

        try {
            if (typeDesc == null)
                throw new SpaceMetadataException("Failed to load space type descriptor for type '" + typeName + "'.");
            // Check current type descriptor if exists, to prevent redundant updates:
            ITypeDesc oldTypeDesc = _typeMap.get(typeDesc.getTypeName());
            if (oldTypeDesc != typeDesc) {
                if (oldTypeDesc != null) {
                    // TODO: Validate new typeDesc does not contradict oldTypeDesc.
                }

                try {
                    //check if type already exists in the space
                    ITypeDesc typeDescFromServer = _spaceProxy.getTypeDescFromServer(typeName);

                    // Register new type descriptor in server(s) BEFORE caching:
                    if (typeDescFromServer == null)
                        _spaceProxy.registerTypeDescInServers(typeDesc);
                } catch (TieredStorageMetadataException e) {
                    throw e;
                } catch (SpaceMetadataException e) {

                    if (ignoreException) {
                        if (_logger.isDebugEnabled())
                            _logger.debug("Failed to register type descriptor in server", e);
                    } else
                        throw e;
                }

                // TODO add version check
                // Cache loaded type descriptor:
                _typeMap.put(typeDesc.getTypeName(), typeDesc);

                if (_logger.isDebugEnabled())
                    _logger.debug("Created type descriptor " + typeDesc);
            }
        } finally {
            logExit("ClientTypeDescRepository.registerTypeDesc", "typeName", typeName);
        }
    }

    @SuppressWarnings("deprecation")
    public static Class<?> getRealClass(String className, String codebase) {
        logEnter("ClientTypeDescRepository.getRealClass", "className", className);

        if (className == null || className.length() == 0)
            throw new IllegalArgumentException("Argument 'className' cannot be null or empty.");

        Class<?> realClass = null;

        try {
            // Load real class:
            realClass = ClassLoaderHelper.loadClass(codebase, className, null /*classLoader*/, false /*localOnly*/);

            // If real class extends ExternalEntry, ignore it:
            if (ExternalEntry.class.isAssignableFrom(realClass)) {
                realClass = null;
                if (_logger.isDebugEnabled())
                    _logger.debug("Class '" + className + "'' was loaded successfully but ignored since it extends ExternalEntry.");
            } else {
                if (_logger.isDebugEnabled())
                    _logger.debug("Class '" + className + "'' was loaded successfully.");
            }
        } catch (ClassNotFoundException e) {
            if (_logger.isDebugEnabled())
                _logger.debug("Class '" + className + "'' could not be loaded since it was not found.", e);
        } catch (MalformedURLException e) {
            if (_logger.isDebugEnabled())
                _logger.debug("Class '" + className + "'' could not be loaded since the codebase [" + codebase + "] is malformed.", e);
        }

        // If real class was located, validate it:
        if (realClass != null)
            ReflectionUtil.assertIsPublic(realClass);

        logExit("ClientTypeDescRepository.getRealClass", "className", className);
        return realClass;
    }

    private static boolean isValid(ITypeDesc typeDesc) {
        return (typeDesc != null && !typeDesc.isInactive());
    }

    private static boolean hasRequiredIntrospector(ITypeDesc typeDesc, ObjectType objectType) {
        return typeDesc.isConcreteType() || !objectType.isConcrete();
    }

    private static void logEnter(String methodName, String argName, Object argValue) {
        if (_logger.isDebugEnabled())
            _logger.debug("Entered " + methodName + ", " + argName + "=[" + argValue + "].");
    }

    private static void logExit(String methodName, String argName, Object argValue) {
        if (_logger.isDebugEnabled())
            _logger.debug("Finished " + methodName + ", " + argName + "=[" + argValue + "].");
    }
}
