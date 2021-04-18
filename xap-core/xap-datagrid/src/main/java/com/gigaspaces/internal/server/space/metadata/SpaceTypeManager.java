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

package com.gigaspaces.internal.server.space.metadata;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.client.spaceproxy.metadata.ClientTypeDescRepository;
import com.gigaspaces.internal.client.spaceproxy.metadata.ObjectType;
import com.gigaspaces.internal.client.spaceproxy.metadata.TypeDescFactory;
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.server.metadata.AddTypeDescResult;
import com.gigaspaces.internal.server.metadata.AddTypeDescResultType;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceConfigReader;
import com.gigaspaces.internal.server.space.SpaceInstanceConfig;
import com.gigaspaces.internal.server.space.tiered_storage.*;
import com.gigaspaces.internal.server.space.tiered_storage.error.TieredStorageMetadataException;
import com.gigaspaces.internal.transport.ITransportPacket;
import com.gigaspaces.internal.utils.GsEnv;
import com.gigaspaces.logger.LogLevel;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.index.ISpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.j_spaces.core.Constants;
import com.j_spaces.core.DetailedUnusableEntryException;
import com.j_spaces.core.DropClassException;
import com.j_spaces.core.UnknownTypeException;
import com.j_spaces.core.client.ExternalEntry;
import com.j_spaces.core.client.SpaceURL;
import com.j_spaces.core.exception.internal.DirectoryInternalSpaceException;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.SystemProperties;
import net.jini.core.entry.Entry;
import net.jini.core.entry.UnusableEntryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Niv Ingberg
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceTypeManager {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_SPACE_TYPEMANAGER);
    private static final boolean SUPPORT_CHECKSUM = Boolean.parseBoolean(
            System.getProperty(SystemProperties.TYPE_CHECKSUM_VALIDATION, SystemProperties.TYPE_CHECKSUM_VALIDATION_DEFAULT));
    private static final boolean DELETE_ON_DROP = GsEnv.propertyBoolean(SystemProperties.TYPE_DELETE_ON_DROP).get(true);

    private final TypeDescFactory _typeDescFactory;
    private volatile Map<String, IServerTypeDesc> _typeMap;
    private final Object _typeDescLock = new Object();
    private final AtomicInteger _typeIdGenerator;
    private final Set<IServerTypeDescListener> _typeDescListeners;
    private final TieredStorageManager tieredStorageManager;

    public SpaceTypeManager(SpaceConfigReader configReader) {
        this(new TypeDescFactory(), configReader, null);
    }

    public SpaceTypeManager(TypeDescFactory typeDescFactory, SpaceConfigReader configReader, TieredStorageManager tieredStorageManager) {
        logEnter("SpaceTypeManager.ctor", "spaceName", configReader.getFullSpaceName());

        this._typeDescFactory = typeDescFactory;
        this._typeMap = new HashMap<String, IServerTypeDesc>();
        this._typeIdGenerator = new AtomicInteger();
        this._typeDescListeners = new HashSet<IServerTypeDescListener>();

        ITypeDesc objectTypeDesc = _typeDescFactory.createPojoTypeDesc(Object.class, null, null);
        IServerTypeDesc rootTypeDesc = createServerTypeDescInstance(IServerTypeDesc.ROOT_TYPE_NAME, objectTypeDesc, null, _typeMap);
        _typeMap.put(null, rootTypeDesc);
        createServerTypeDescInstance(IServerTypeDesc.ROOT_SYSTEM_TYPE_NAME, null, null, _typeMap);
        this.tieredStorageManager = tieredStorageManager;
        logExit("SpaceTypeManager.ctor", "spaceName", configReader.getFullSpaceName());
    }

    public void registerTypeDescListener(IServerTypeDescListener listener) {
        synchronized (_typeDescLock) {
            // Register listener:
            this._typeDescListeners.add(listener);
            // Invoke listener for each existing type descriptor:
            for (IServerTypeDesc typeDesc : _typeMap.values())
                listener.onTypeAdded(typeDesc);
        }
    }

    public Object getTypeDescLock() {
        return _typeDescLock;
    }

    public ITypeDesc[] addIndexes(String typeName, SpaceIndex[] newIndexes) {
        synchronized (_typeDescLock) {
            // Validate type is registered in server:
            IServerTypeDesc typeDesc = _typeMap.get(typeName);
            if (typeDesc == null)
                throw new SpaceMetadataException("Cannot add indexes to type '" + typeName + "' because it is not registered in the server.");

            // Generate an updated type descriptor with new indexes for each type assignable from the specified type (inclusive):
            ITypeDesc[] updatedTypeDescriptors = generateUpdatedTypeDescriptors(typeDesc.getAssignableTypes(), newIndexes);
            // If there are no types to update, return:
            if (updatedTypeDescriptors.length == 0)
                return updatedTypeDescriptors;

            // Clone the type map to isolate changes:
            Map<String, IServerTypeDesc> localTypeMap = cloneTypeMap(_typeMap, typeDesc.getTypeDesc().isSystemType());

            for (ITypeDesc updatedTypeDesc : updatedTypeDescriptors) {
                // Get cloned server type desc from local map:
                IServerTypeDesc updatedServerTypeDesc = localTypeMap.get(updatedTypeDesc.getTypeName());
                // Update type descriptor:
                updatedServerTypeDesc.setTypeDesc(updatedTypeDesc);
                // Notify listeners on new type:
                for (IServerTypeDescListener listener : _typeDescListeners)
                    listener.onTypeIndexAdded(updatedServerTypeDesc);
            }

            // Flush local changes to volatile global reference:
            _typeMap = localTypeMap;

            return updatedTypeDescriptors;
        }
    }

    private static ITypeDesc[] generateUpdatedTypeDescriptors(IServerTypeDesc[] serverTypes, SpaceIndex[] newIndexes)
            throws SpaceMetadataException {
        List<ITypeDesc> result = new ArrayList<ITypeDesc>();

        for (IServerTypeDesc serverType : serverTypes) {
            // Validate type is active:
            if (serverType.isInactive())
                throw new SpaceMetadataException("Cannot add indexes to type '" + serverType.getTypeName() + "' because it is inactive in the server.");

            // Init list of indexes to add for this type:
            List<SpaceIndex> newIndexesList = new ArrayList<SpaceIndex>(newIndexes.length);
            // Evaluate each new index in the context of this type:
            for (int i = 0; i < newIndexes.length; i++) {
                // Check if this subtype has an index by the same name:
                SpaceIndex currIndex = serverType.getTypeDesc().getIndexes().get(newIndexes[i].getName());
                // If no such index add index to new indexes list, otherwise validate:
                if (currIndex == null)
                    newIndexesList.add(newIndexes[i]);
                else
                    validateIndexIsEquivalent(newIndexes[i], currIndex, serverType.getTypeName());
            }
            // If type has new indexes, add it to the result set:
            if (!newIndexesList.isEmpty())
                result.add(generateUpdatedTypeDescriptor(serverType.getTypeDesc(), newIndexesList));
        }

        return result.toArray(new ITypeDesc[result.size()]);
    }

    private static ITypeDesc generateUpdatedTypeDescriptor(ITypeDesc currTypeDesc, List<SpaceIndex> newIndexesList) {
        // Create a clone of the current type descriptor:
        ITypeDesc newTypeDesc = currTypeDesc.clone();
        // Add new indexes to the new type descriptor:
        for (SpaceIndex newIndex : newIndexesList)
            newTypeDesc.getIndexes().put(newIndex.getName(), newIndex);
        // Return new type descriptor:
        return newTypeDesc;
    }

    private static void validateIndexIsEquivalent(SpaceIndex newIndex, SpaceIndex currIndex, String typeName)
            throws SpaceMetadataException {
        if (newIndex.getIndexType() != currIndex.getIndexType())
            throw new SpaceMetadataException("Cannot add index '" + newIndex.getName() +
                    "' of type " + newIndex.getIndexType() +
                    " to space type '" + typeName +
                    "' because it already contains an index with the same name of type " + currIndex.getIndexType() + ".");
    }


    public IServerTypeDesc getServerTypeDesc(String typeName) {
        return _typeMap.get(typeName);
    }

    public ITypeDesc getTypeDesc(String typeName) {
        IServerTypeDesc serverTypeDesc = _typeMap.get(typeName);
        return serverTypeDesc == null ? null : serverTypeDesc.getTypeDesc();
    }

    public Map<String, IServerTypeDesc> getSafeTypeTable() {
        return _typeMap;
    }

    public IServerTypeDesc loadServerTypeDesc(ITransportPacket packet)
            throws UnusableEntryException, UnknownTypeException {
        final String typeName = packet.getTypeName();
        logEnter("loadServerTypeDesc", "typeName", typeName);

        IServerTypeDesc serverTypeDesc = _typeMap.get(typeName);
        if (requiresRegistration(serverTypeDesc, packet.getEntryType())) {
            ITypeDesc typeDesc = packet.getTypeDescriptor();
            if (typeDesc == null) {
                if (typeName != null)
                    throw new UnknownTypeException("Insufficient Data In Class : " + typeName, typeName);
                serverTypeDesc = getTypeDescriptorByObject(new Object(), ObjectType.POJO);
            } else {
                final AddTypeDescResult result = createOrUpdateServerTypeDesc(typeName,
                        typeDesc,
                        packet.getEntryType());
                serverTypeDesc = result.getServerTypeDesc();
            }
        }

        validateChecksum(typeName, packet, serverTypeDesc.getTypeDesc());
        packet.setTypeDesc(serverTypeDesc.getTypeDesc(), false);

        logExit("loadServerTypeDesc", "typeName", typeName);
        return serverTypeDesc;
    }

    public AddTypeDescResult addTypeDesc(ITypeDesc typeDesc)
            throws DetailedUnusableEntryException {
        final String typeName = typeDesc.getTypeName();
        logEnter("addTypeDesc", "typeName", typeName);

        IServerTypeDesc serverTypeDesc = _typeMap.get(typeName);

        AddTypeDescResult result = null;
        AddTypeDescResultType action = getAddTypeDescAction(serverTypeDesc, typeDesc.getObjectType());

        if (action != AddTypeDescResultType.NONE)
            result = createOrUpdateServerTypeDesc(typeName, typeDesc, typeDesc.getObjectType());
        else
            result = new AddTypeDescResult(serverTypeDesc, action);

        validateChecksum(typeName, typeDesc, result.getServerTypeDesc().getTypeDesc());

        logExit("addTypeDesc", "typeName", typeName);
        return result;
    }

    private static boolean requiresRegistration(IServerTypeDesc serverTypeDesc, EntryType entryType) {
        return getAddTypeDescAction(serverTypeDesc, entryType) != AddTypeDescResultType.NONE;
    }

    private static AddTypeDescResultType getAddTypeDescAction(IServerTypeDesc serverTypeDesc, EntryType entryType) {
        if (serverTypeDesc == null)
            return AddTypeDescResultType.CREATED;
        if (serverTypeDesc.isInactive())
            return AddTypeDescResultType.ACTIVATED;
        if (!serverTypeDesc.getTypeDesc().supports(entryType))
            return AddTypeDescResultType.UPDATED;
        return AddTypeDescResultType.NONE;
    }

    private AddTypeDescResult createOrUpdateServerTypeDesc(String typeName, ITypeDesc typeDesc, EntryType requestType) {
        synchronized (_typeDescLock) {
            logEnter("createOrUpdateServerTypeDesc", "typeName", typeName);

            // Get type from global map
            IServerTypeDesc serverTypeDesc = _typeMap.get(typeName);
            // Check if type requires registration:
            AddTypeDescResultType action = getAddTypeDescAction(serverTypeDesc, requestType);
            if (action != AddTypeDescResultType.NONE) {
                // Create a local clone of the global type map and use it to make the required changes:
                Map<String, IServerTypeDesc> localTypeMap = cloneTypeMap(_typeMap, typeDesc.isSystemType());
                // Get type from local map to verify local changes do not affect global view:
                serverTypeDesc = localTypeMap.get(typeName);

                if (action == AddTypeDescResultType.CREATED) {
                    serverTypeDesc = createServerTypeDesc(typeDesc, localTypeMap);
                    if(tieredStorageManager != null){
                        validateTieredStorage(serverTypeDesc.getTypeDesc());
                        if(!tieredStorageManager.isTransient(typeDesc.getTypeName())){
                            try {
                                tieredStorageManager.getInternalStorage().createTable(typeDesc);
                            } catch (SAException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }

                } else if (action == AddTypeDescResultType.ACTIVATED)
                    activateServerTypeDesc(serverTypeDesc, typeDesc, localTypeMap);
                else if (action == AddTypeDescResultType.UPDATED)
                    serverTypeDesc = updateServerTypeDesc(serverTypeDesc, typeDesc);

                // Flush updated local type map back onto global volatile type map:
                _typeMap = localTypeMap;
            }

            logExit("createOrUpdateServerTypeDesc", "typeName", typeName);
            return new AddTypeDescResult(serverTypeDesc, action);
        }
    }

    private void validateTieredStorage(ITypeDesc typeDesc) {
        String typeName = typeDesc.getTypeName();
        if (tieredStorageManager.isTransient(typeName)) {
            return; // Transient Types are never written to InternalRDBMS , supports all features
        }

        if (typeDesc.supportsDynamicProperties()) {
            throw new TieredStorageMetadataException("Unsupported type " + typeName + ": dynamic properties not supported in tiered storage " +
                    "(set com.gigaspaces.metadata.SpaceTypeDescriptorBuilder.supportsDynamicProperties(false) ");
        }

        String[] superClassesNames = typeDesc.getSuperClassesNames();

        for (String superClassesName : superClassesNames) {
            if(!superClassesName.equals(typeName) && !superClassesName.equals(Object.class.getName())){
                throw new TieredStorageMetadataException("Unsupported type " + typeName + ": inheritance is not supported in tiered storage " +
                        "(has non java.lang.Object super type : " + superClassesName + ")");
            }
        }

        if (typeDesc.isFifoSupported() || typeDesc.getFifoGroupingPropertyPath() != null ||
                (typeDesc.getFifoGroupingIndexesPaths() != null && !typeDesc.getFifoGroupingIndexesPaths().isEmpty())) {
            throw new TieredStorageMetadataException("Unsupported type " + typeName + ": FIFO not supported in tiered storage");
        }

        TieredStorageTableConfig config = tieredStorageManager.getTableConfig(typeName);
        if (config != null && config.getTimeColumn() != null && config.getPeriod() != null) {
            Class<?> timeColumnType = typeDesc.getFixedProperty(config.getTimeColumn()).getType();
            if (!TieredStorageUtils.isSupportedTimeColumn(timeColumnType)) {
                throw new TieredStorageMetadataException("Unsupported type " + typeName + ": unsupported timeColumn type [" + config.getTimeColumn() + ": " + timeColumnType.getName() + "]");
            }
        }

        for (SpaceIndex index : typeDesc.getIndexes().values()) {
            if(((ISpaceIndex) index).isMultiValuePerEntryIndex() || ((ISpaceIndex) index).isCompoundIndex()){
                throw new TieredStorageMetadataException("Unsupported type " + typeName + ": unsupported index type - " + index.getName() + "]");
            }
        }

        for (PropertyInfo propertyInfo : typeDesc.getProperties()) {
            if (!TieredStorageUtils.isSupportedPropertyType(propertyInfo.getType())) {
                throw new TieredStorageMetadataException("Unsupported type " + typeName + ": unsupported property type ['" + propertyInfo.getName() + "': " + propertyInfo.getType().getName() + "]");
            }
        }
    }

    private IServerTypeDesc createServerTypeDesc(ITypeDesc typeDesc, Map<String, IServerTypeDesc> localTypeMap) {
        final String typeName = typeDesc.getTypeName();
        logEnter("createServerTypeDesc", "typeName", typeName);

        final String[] superClasses = typeDesc.getRestrictSuperClassesNames();
        final String rootTypeName = typeDesc.isSystemType()
                ? IServerTypeDesc.ROOT_SYSTEM_TYPE_NAME
                : IServerTypeDesc.ROOT_TYPE_NAME;

        for (int i = superClasses.length - 1; i >= 0; i--) {
            String currTypeName = superClasses[i];
            // If type does not exist, create inactive:
            if (!localTypeMap.containsKey(currTypeName)) {
                String superTypeName = i != superClasses.length - 1 ? superClasses[i + 1] : rootTypeName;
                createServerTypeDescInstance(currTypeName, null, superTypeName, localTypeMap);
            }
        }

        String superTypeName = superClasses.length != 0 ? superClasses[0] : rootTypeName;
        IServerTypeDesc serverTypeDesc = createServerTypeDescInstance(typeName, typeDesc, superTypeName, localTypeMap);

        logExit("createServerTypeDesc", "typeName", typeName);
        return serverTypeDesc;
    }

    private IServerTypeDesc createServerTypeDescInstance(String typeName, ITypeDesc typeDesc, String superTypeName, Map<String, IServerTypeDesc> localTypeMap) {
        logEnter("createServerTypeDescInstance", "typeName", typeName);

        // Get super type desc of this type, if any:
        IServerTypeDesc superTypeDesc = superTypeName != null ? localTypeMap.get(superTypeName) : null;
        // Create server type desc for this type:
        ServerTypeDesc serverTypeDesc = new ServerTypeDesc(_typeIdGenerator.incrementAndGet(), typeName, typeDesc, superTypeDesc);
        // Add new server type descriptor to type table:
        localTypeMap.put(typeName, serverTypeDesc);

        // Log:
        if (_logger.isDebugEnabled()) {
            _logger.debug("Created ServerTypeDesc for type [" + typeName + "]." + getClientAddressAddition());
            if (_logger.isTraceEnabled())
                logTypeMap(localTypeMap);
            else if (_logger.isDebugEnabled())
                logServerTypeDesc(serverTypeDesc, LogLevel.DEBUG);
        }

        // Notify listeners on new type:
        for (IServerTypeDescListener listener : _typeDescListeners)
            listener.onTypeAdded(serverTypeDesc);

        logExit("createServerTypeDescInstance", "typeName", typeName);
        return serverTypeDesc;
    }

    private void activateServerTypeDesc(IServerTypeDesc serverTypeDesc, ITypeDesc typeDesc, Map<String, IServerTypeDesc> localTypeMap) {
        final String typeName = serverTypeDesc.getTypeName();
        logEnter("activateServerTypeDesc", "typeName", typeName);

        serverTypeDesc.setTypeDesc(typeDesc);

        if (_logger.isDebugEnabled()) {
            _logger.debug("Activated ServerTypeDesc [" + typeName + "]." + getClientAddressAddition());
            if (_logger.isTraceEnabled())
                logTypeMap(localTypeMap);
            else if (_logger.isDebugEnabled())
                logServerTypeDesc(serverTypeDesc, LogLevel.DEBUG);
        }

        // Notify listeners on activated type:
        for (IServerTypeDescListener listener : _typeDescListeners)
            listener.onTypeActivated(serverTypeDesc);

        logExit("activateServerTypeDesc", "typeName", typeName);
    }

    private static String getClientAddressAddition() {
        InetSocketAddress clientEndPointAddress = LRMIInvocationContext.getEndpointAddress();
        return " [clientAddress=" + (clientEndPointAddress != null ? clientEndPointAddress : "colocated") + "]";

    }

    private IServerTypeDesc updateServerTypeDesc(IServerTypeDesc serverTypeDesc, ITypeDesc typeDesc) {
        logEnter("updateServerTypeDesc", "typeName", serverTypeDesc.getTypeName());

        // Update server type desc:
        serverTypeDesc.setTypeDesc(typeDesc);

        logExit("updateServerTypeDesc", "typeName", serverTypeDesc.getTypeName());
        return serverTypeDesc;
    }

    /**
     * Drop all Class entries and all its templates from the space. Calling this method will remove
     * all internal meta data related to this class stored in the space. When using persistent
     * spaced the relevant RDBMS table will be dropped. It is the caller responsibility to ensure
     * that no entries from this class are written to the space while this method is called. This
     * method is protected through the space Default Security Filter. Admin permissions required to
     * execute this request successfully.
     *
     * @param typeName name of class to delete.
     */
    public void dropClass(String typeName) throws DropClassException {
        if (typeName == null || typeName.equals(IServerTypeDesc.ROOT_TYPE_NAME))
            throw new DropClassException("Invalid class name specified", typeName);

        synchronized (_typeDescLock) {
            IServerTypeDesc typeDesc = _typeMap.get(typeName);
            if (typeDesc == null || typeDesc.isInactive())
                return;

            //if the class has active sub-classes - dont allow
            for (IServerTypeDesc subTypeDesc : typeDesc.getAssignableTypes()) {
                if (subTypeDesc.getTypeName().equals(typeName))
                    continue;
                if (subTypeDesc.isActive())
                    throw new DropClassException("Drop class failed. " + typeName + " class has an active subclass = " + subTypeDesc.getTypeName(), typeName);
            }

            // copy type table to local variable and work on the local copy.
            Map<String, IServerTypeDesc> typeMapCopy = cloneTypeMap(_typeMap, typeDesc.getTypeDesc().isSystemType());
            typeDesc = typeMapCopy.get(typeName);
            if (typeDesc == null)
                throw new DirectoryInternalSpaceException("internal error - dropClass " + typeName + " class does not exist");
            if (typeDesc.isInactive())
                throw new DirectoryInternalSpaceException("internal error - dropClass " + typeName + " class not active");

            typeDesc.inactivateType();

            for (IServerTypeDescListener listener : _typeDescListeners)
                listener.onTypeDeactivated(typeDesc);

            if (DELETE_ON_DROP) {
                typeMapCopy.remove(typeDesc.getTypeName());
                typeDesc.getSuperTypes()[1].removeSubType(typeDesc);
            }

            // volatile - exchange references to the new updated type table
            _typeMap = typeMapCopy;

            if (_logger.isDebugEnabled())
                _logger.debug("setting " + typeDesc.getTypeName() + " to inActive");
        }
    }

    private static void validateChecksum(String typeName, ITransportPacket packet, ITypeDesc serverTypeDesc)
            throws DetailedUnusableEntryException, UnknownTypeException {
        if (typeName == null || !SUPPORT_CHECKSUM || !packet.supportsTypeDescChecksum())
            return;
        if (serverTypeDesc.getChecksum() == packet.getTypeDescChecksum())
            return;

        final ITypeDesc clientTypeDesc = packet.getTypeDescriptor();
        if (clientTypeDesc == null)
            throw new UnknownTypeException("Client checksum " + packet.getTypeDescChecksum() + " for type [" + typeName + "] does not match server checksum " + serverTypeDesc.getChecksum(),
                    typeName);

        validateChecksum(typeName, clientTypeDesc, serverTypeDesc);
    }


    private static void validateChecksum(String typeName, ITypeDesc clientTypeDesc, ITypeDesc serverTypeDesc)
            throws DetailedUnusableEntryException {
        if (serverTypeDesc.getChecksum() == clientTypeDesc.getChecksum() || !SUPPORT_CHECKSUM)
            return;

        final StringBuilder sb = new StringBuilder();
        sb.append("The operation's type description is incompatible with the type description stored in the space.\n");
        sb.append(String.format("Type=[%s], Server checksum=[%d], Operation checksum=[%d].%n",
                typeName, serverTypeDesc.getChecksum(), clientTypeDesc.getChecksum()));
        printTypeDesc(sb, serverTypeDesc, "Server type description:\n");
        sb.append("\n");
        printTypeDesc(sb, clientTypeDesc, "Operation type description:\n");

        final String message = sb.toString();
        if (_logger.isErrorEnabled())
            _logger.error(message);

        throw new DetailedUnusableEntryException(message);
    }

    private static void printTypeDesc(StringBuilder sb, ITypeDesc typeDesc, String header) {
        final String[] superClasses = typeDesc.getSuperClassesNames();
        final PropertyInfo[] properties = typeDesc.getProperties();

        sb.append(header);
        //sb.append(String.format("Code base: %s%n", typeDesc.getCodeBase()));
        sb.append(String.format("Super classes: %d%n", superClasses.length));
        for (int i = 0; i < superClasses.length; i++)
            sb.append(String.format("%4d: Type=[%s]%n", i + 1, superClasses[i]));

        sb.append(String.format("Properties: %d%n", properties.length));
        for (int i = 0; i < properties.length; i++)
            sb.append(String.format("%4d: Name=[%s], Type=[%s]%n", i + 1, properties[i].getName(), properties[i].getTypeName()));

        sb.append(String.format("Checksum: %d.%n", typeDesc.getChecksum()));
    }

    private Map<String, IServerTypeDesc> cloneTypeMap(Map<String, IServerTypeDesc> typeMap, boolean isSystem) {
        // If map is empty clone is not required:
        if (typeMap.isEmpty())
            return typeMap;

        // Get root type descriptors:
        IServerTypeDesc rootTypeDesc = typeMap.get(IServerTypeDesc.ROOT_TYPE_NAME);
        IServerTypeDesc rootSystemTypeDesc = typeMap.get(IServerTypeDesc.ROOT_SYSTEM_TYPE_NAME);

        // Clone requested root:
        if (isSystem)
            rootSystemTypeDesc = rootSystemTypeDesc.createCopy(null);
        else
            rootTypeDesc = rootTypeDesc.createCopy(null);

        // Create a new map:
        Map<String, IServerTypeDesc> typeMapCopy = new HashMap<String, IServerTypeDesc>(typeMap.size());
        // Index root type descriptors in new map:
        typeMapCopy.put(null, rootTypeDesc);
        for (IServerTypeDesc subType : rootTypeDesc.getAssignableTypes())
            typeMapCopy.put(subType.getTypeName(), subType);
        for (IServerTypeDesc subType : rootSystemTypeDesc.getAssignableTypes())
            typeMapCopy.put(subType.getTypeName(), subType);

        return typeMapCopy;
    }

    public IServerTypeDesc getTypeDescriptorByObject(Object obj, ObjectType objectType)
            throws UnknownTypeException {
        EntryType entryType;
        String typeName;

        if (objectType == ObjectType.EXTERNAL_ENTRY) {
            entryType = EntryType.EXTERNAL_ENTRY;
            typeName = ((ExternalEntry) obj).getClassName();
        } else if (objectType == ObjectType.DOCUMENT) {
            entryType = EntryType.DOCUMENT_JAVA;
            typeName = ((SpaceDocument) obj).getTypeName();
        } else {
            entryType = EntryType.OBJECT_JAVA;
            typeName = obj.getClass().getName();
        }

        IServerTypeDesc serverTypeDesc = _typeMap.get(typeName);
        if (requiresRegistration(serverTypeDesc, entryType)) {
            ITypeDesc typeDesc = createTypeDescByObject(obj, objectType);
            final AddTypeDescResult result = createOrUpdateServerTypeDesc(typeName,
                    typeDesc,
                    entryType);
            serverTypeDesc = result.getServerTypeDesc();
        }

        return serverTypeDesc;
    }

    private ITypeDesc createTypeDescByObject(Object obj, ObjectType objectType)
            throws UnknownTypeException {
        switch (objectType) {
            case POJO:
                return createPojoTypeDesc(obj.getClass(), null /*codebase*/);
            case ENTRY:
                return _typeDescFactory.createEntryTypeDesc((Entry) obj, obj.getClass().getName(), null /*codebase*/, obj.getClass());
            case DOCUMENT:
                final String typeName = ((SpaceDocument) obj).getTypeName();
                final Class<?> type = ClientTypeDescRepository.getRealClass(typeName, null /*codebase*/);
                if (type != null)
                    return createPojoTypeDesc(type, null /*codebase*/);
                throw new UnknownTypeException("Type '" + typeName + "' is not registered in server.", typeName);
            case EXTERNAL_ENTRY:
                ExternalEntry externalEntry = (ExternalEntry) obj;
                return _typeDescFactory.createExternalEntryTypeDesc(externalEntry, externalEntry.getClassName());
            case ENTRY_PACKET:
            case TEMPLATE_PACKET:
                return ((ITransportPacket) obj).getTypeDescriptor();
            default:
                throw new IllegalArgumentException("Unsupported object type: " + objectType);
        }
    }

    private ITypeDesc createPojoTypeDesc(Class<?> type, String codebase) {
        IServerTypeDesc serverTypeDesc = _typeMap.get(type.getName());
        if (!requiresRegistration(serverTypeDesc, EntryType.OBJECT_JAVA))
            return serverTypeDesc.getTypeDesc();

        // Get super type:
        Class<?> superType = type.getSuperclass();
        // Get super type desc recursively:
        ITypeDesc superTypeDesc = superType == null ? null : createPojoTypeDesc(superType, codebase);
        // Create type desc from type with super type desc:
        ITypeDesc typeDesc = _typeDescFactory.createPojoTypeDesc(type, codebase, superTypeDesc);
        // Create server type descriptor:
        createOrUpdateServerTypeDesc(type.getName(), typeDesc, EntryType.OBJECT_JAVA);
        return typeDesc;
    }

    public boolean isFifoType(IServerTypeDesc serverTypeDesc) {
        return serverTypeDesc.isActive() && serverTypeDesc.isFifoSupported();
    }

    private static void logServerTypeDesc(IServerTypeDesc serverTypeDesc, LogLevel level) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Type name: [%s]%s%n", serverTypeDesc.getTypeName(), getClientAddressAddition()));
        sb.append(String.format("IsActive: %s%n", serverTypeDesc.isActive()));

        IServerTypeDesc[] superTypes = serverTypeDesc.getSuperTypes();
        sb.append(String.format("Super types: %d%n", superTypes.length));
        for (int i = 0; i < superTypes.length; i++)
            sb.append(String.format("%4d: Type=[%s]%n", i + 1, superTypes[i].getTypeName()));

        IServerTypeDesc[] subTypes = serverTypeDesc.getAssignableTypes();
        sb.append(String.format("Sub types: %d%n", subTypes.length));
        for (int i = 0; i < subTypes.length; i++)
            sb.append(String.format("%4d: Type=[%s]%n", i + 1, subTypes[i].getTypeName()));

        if (serverTypeDesc.isActive()) {
            ITypeDesc typeDesc = serverTypeDesc.getTypeDesc();
            final PropertyInfo[] properties = typeDesc.getProperties();
            sb.append(String.format("Properties: %d%n", properties.length));
            for (int i = 0; i < properties.length; i++)
                sb.append(String.format("%4d: Name=[%s], Type=[%s]%n", i + 1, properties[i].getName(), properties[i].getTypeName()));
            sb.append(String.format("Checksum: %d.%n", typeDesc.getChecksum()));
        }

        level.log(_logger, sb.toString());
    }

    private static void logTypeMap(Map<String, IServerTypeDesc> typeMap) {
        for (IServerTypeDesc typeDesc : typeMap.values())
            logServerTypeDesc(typeDesc, LogLevel.TRACE);
    }

    private static void logEnter(String methodName, String argName, Object argValue) {
        if (_logger.isTraceEnabled())
            _logger.trace("Entered " + methodName + ", " + argName + "=[" + argValue + "].");
    }

    private static void logExit(String methodName, String argName, Object argValue) {
        if (_logger.isTraceEnabled())
            _logger.trace("Finished " + methodName + ", " + argName + "=[" + argValue + "].");
    }

    public void loadSpaceTypes(SpaceURL url)
            throws DetailedUnusableEntryException {
        //load space predefined types
        if (url != null) {
            SpaceInstanceConfig spaceInstanceConfig = (SpaceInstanceConfig) url.getCustomProperties().get(Constants.Space.SPACE_CONFIG);
            SpaceTypeDescriptor[] typeDescriptors = spaceInstanceConfig != null ? spaceInstanceConfig.getTypeDescriptors() : null;
            if (typeDescriptors != null)
                for (SpaceTypeDescriptor typeDesc : typeDescriptors)
                    addTypeDesc((ITypeDesc) typeDesc);
        }
    }


}
