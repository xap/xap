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

import com.gigaspaces.annotation.pojo.FifoSupport;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.metadata.*;
import com.gigaspaces.internal.metadata.converter.ConversionException;
import com.gigaspaces.internal.reflection.IField;
import com.gigaspaces.internal.reflection.ReflectionUtil;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.gigaspaces.metadata.StorageType;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.gigaspaces.metadata.index.SpacePropertyIndex;
import com.gigaspaces.query.extension.metadata.TypeQueryExtensions;
import com.gigaspaces.query.extension.metadata.impl.TypeQueryExtensionsImpl;
import com.j_spaces.core.client.ExternalEntry;
import com.j_spaces.core.client.IReplicatable;
import com.j_spaces.core.client.MetaDataEntry;
import com.j_spaces.core.client.ReadModifiers;

import net.jini.core.entry.Entry;

import java.io.Externalizable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class TypeDescFactory {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_CLIENT);
    private static final Logger _deprecationLogger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_METADATA + ".deprecation");

    private final IDirectSpaceProxy _spaceProxy;
    private final StorageType _storageType;

    public TypeDescFactory() {
        this(null);
    }

    public TypeDescFactory(IDirectSpaceProxy spaceProxy) {
        this._spaceProxy = spaceProxy;
        this._storageType = spaceProxy != null ? StorageType.fromCode(spaceProxy.getProxySettings().getSerializationType()) : StorageType.OBJECT;
    }

    public ITypeDesc createPojoTypeDesc(Class<?> type, String codeBase, ITypeDesc superTypeDesc) {
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);
        if(typeInfo.getSpaceClassStorageAdapter() != null && typeInfo.isBlobstoreEnabled() && isBlobStoreCachePolicy()){
            throw new SpaceMetadataException("Cannot set ClassBinaryStorageAdapter for types that are blob store enabled");
        }
        // calculate the storage type for properties without storage type (or DEFAULT).
        StorageType defaultStorageType = typeInfo.getStorageType();
        if (defaultStorageType == StorageType.DEFAULT)
            defaultStorageType = _storageType;

        final PropertyInfo[] properties = new PropertyInfo[typeInfo.getNumOfSpaceProperties()];
        boolean binaryClass = typeInfo.getSpaceClassStorageAdapter() != null;
        Set<String> indexesNames = typeInfo.getIndexes().keySet();
        for (int i = 0; i < properties.length; i++) {
            final SpacePropertyInfo property = typeInfo.getProperty(i);
            properties[i] = PropertyInfo.builder(property.getName())
                    .type(property.getType())
                    .documentSupport(property.getDocumentSupport())
                    .storageType(property.getStorageType())
                    .storageAdapter(property.getStorageAdapterClass())
                    .defaultStorageType(defaultStorageType, binaryClass, indexesNames)
                    .build();
        }
        final Map<String, SpaceIndex> indexes = new HashMap<String, SpaceIndex>(typeInfo.getIndexes());
        String fifoGroupingName = typeInfo.getFifoGroupingName();
        Set<String> fifoGroupingIndexes = typeInfo.getFifoGroupingIndexes();
        final boolean supportsDynamicProperties = typeInfo.getDynamicPropertiesProperty() != null;
        final boolean supportsOptimisticLocking = typeInfo.getVersionProperty() != null;
        final String idPropertyName = typeInfo.getIdProperty() != null ? typeInfo.getIdProperty().getName() : null;
        final String defaultPropertyName = null;
        final String routingPropertyName = typeInfo.getRoutingProperty() != null ? typeInfo.getRoutingProperty().getName() : null;
        final FifoSupport fifoSupport = isFifoProxy() ? FifoSupport.ALL : typeInfo.getFifoSupport();
        final boolean blobstoreEnabled = typeInfo.isBlobstoreEnabled();
        final boolean broadcast = typeInfo.isBroadcast();
        final String sequenceNumberPropertyName = typeInfo.getSequenceNumberPropertyName();
        TypeQueryExtensions queryExtensionsInfo = new TypeQueryExtensionsImpl(typeInfo);

        ITypeDesc typeDesc = new TypeDesc(typeInfo.getName(), codeBase, typeInfo.getSuperClasses(),
                properties, supportsDynamicProperties, indexes, idPropertyName, typeInfo.getIdAutoGenerate(),
                defaultPropertyName, routingPropertyName, fifoGroupingName, fifoGroupingIndexes, typeInfo.isSystemClass(), fifoSupport,
                typeInfo.isReplicate(), supportsOptimisticLocking, defaultStorageType,
                EntryType.OBJECT_JAVA, type, ExternalEntry.class, SpaceDocument.class, null, DotNetStorageType.NULL,
                blobstoreEnabled, sequenceNumberPropertyName, queryExtensionsInfo, typeInfo.getSpaceClassStorageAdapter(), broadcast);

        if (typeDesc.isExternalizable() && shouldWarnExternalizable(typeInfo) && _deprecationLogger.isWarnEnabled())
            _deprecationLogger.warn("Current class [" + type.getName() + "] implements " + Externalizable.class + ", usage of Externalizable in order to serialize it to a space is deprecated, Use SpaceExclude, StorageType and nested object serialization where relevant instead."
                    + "If you use Externalizable for other purposes which are not serializing it into a space you can turn off this logger. The side effect of externalizable when it comes to serializing object to a space will be ignored in future version");

        return typeDesc;
    }

    public boolean isBlobStoreCachePolicy() {
        if(_spaceProxy != null) {
            Object cachePolicy = _spaceProxy.getProxySettings().getCustomProperties().get("space-config.engine.cache_policy");
            return cachePolicy != null && cachePolicy.equals("3");
        } else {
            return false;
        }
    }

    private static boolean shouldWarnExternalizable(SpaceTypeInfo typeInfo) {
        return true;
    }

    public ITypeDesc createEntryTypeDesc(Entry obj, String className, String codeBase, Class<?> realClass) {
        /**
         * Build up the per-field and superclass information through
         * the reflection API.  These variables are declared here so
         * they can be used in the catch clause.
         **/
        final List<IField> fieldsList = ReflectionUtil.getCanonicalSortedFields(realClass);
        final IField<?, ?>[] fields = fieldsList.toArray(new IField[fieldsList.size()]);
        final int length = fields.length;
        final PropertyInfo[] properties = new PropertyInfo[length];
        final String[] fieldsNames = new String[length];
        final String[] fieldsTypes = new String[length];
        final SpaceIndexType[] fieldsIndexes = new SpaceIndexType[length];

        for (int i = 0; i < length; i++) {
            fieldsNames[i] = fields[i].getName();
            fieldsTypes[i] = fields[i].getType().getName();
            properties[i] = PropertyInfo.builder(fields[i].getName())
                    .type(fields[i].getType())
                    .defaultStorageType(_storageType)
                    .build();
        }
        final String defaultPropertyName = getEntryIndices(realClass, fieldsNames, fieldsTypes, fieldsIndexes);

        final Map<String, SpaceIndex> indexes = new HashMap<String, SpaceIndex>();
        for (int i = 0; i < length; i++) {
            if (fieldsIndexes[i] != null && fieldsIndexes[i].isIndexed()) {
                SpaceIndex index = new SpacePropertyIndex(properties[i].getName(), fieldsIndexes[i], false, i);
                indexes.put(index.getName(), index);
            }
        }

        // Generate super types names array:
        ArrayList<String> superClassesList = new ArrayList<String>();
        for (Class<?> c = realClass; c != null; c = c.getSuperclass())
            superClassesList.add(c.getName());
        final String[] superClasses = superClassesList.toArray(new String[superClassesList.size()]);

        // Check if fifo:
        FifoSupport fifoMode = FifoSupport.OFF;
        if (MetaDataEntry.class.isAssignableFrom(realClass)) {
            boolean isFifo = (obj != null && ((MetaDataEntry) obj).isFifo()) || isFifoProxy() || isFifoProxyModifiers();
            fifoMode = isFifo ? FifoSupport.OPERATION : FifoSupport.OFF;
        }
        if (isFifoProxy())
            fifoMode = FifoSupport.ALL;

        final boolean supportsDynamicProperties = false;
        final boolean isSystemType = false;
        final boolean replicable = IReplicatable.class.isAssignableFrom(realClass);
        final String idPropertyName = null;
        final boolean idAutoGenerate = false;
        final String routingPropertyName = null;

        return new TypeDesc(className, codeBase, superClasses, properties, supportsDynamicProperties,
                indexes, idPropertyName, idAutoGenerate, defaultPropertyName, routingPropertyName, null, null, isSystemType,
                fifoMode, replicable, isVersionedProxy(), _storageType,
                EntryType.OBJECT_JAVA, realClass, ExternalEntry.class, SpaceDocument.class, null, DotNetStorageType.NULL,
                PojoDefaults.BLOBSTORE_ENABLED, null /*sequence number*/, null, null, PojoDefaults.BROADCAST);
    }

    public ITypeDesc createExternalEntryTypeDesc(ExternalEntry externalEntry, String codeBase) {
        if (externalEntry.getClassName() == null)
            throw new RuntimeException("ExternalEntry problem, Class Name is NULL.");

        String[] fieldsNames = externalEntry.getFieldsNames();
        String[] fieldsTypes = externalEntry.getFieldsTypes();

        if (fieldsNames == null || fieldsTypes == null) {
            fieldsNames = fieldsNames == null ? new String[0] : fieldsNames;
            fieldsTypes = fieldsTypes == null ? new String[fieldsNames.length] : fieldsTypes;

            // FIXME see GS-2273 : trying to locate the cause of a NPE.
            // we suspect that one of the fields in the array is null.
            if (fieldsTypes.length > 0 && fieldsTypes[0] == null)
                throw new RuntimeException(
                        "Internal exception: supplied type information is insufficient. Dump info:"
                                + "\n\t class-name: " + externalEntry.getClassName()
                                + "\n\t field-names: " + Arrays.toString(fieldsNames)
                                + "\n\t field-types: " + Arrays.toString(fieldsTypes)
                                + "\n\t ref: GS-2273\n");
        }

        if (fieldsNames.length != fieldsTypes.length) {
            throw new RuntimeException("ExternalEntry : " + externalEntry.getClassName() + " field types and field names size mismatch:" +
                    "\nfieldsTypes=" + Arrays.toString(fieldsTypes) + " fieldsNames=" + Arrays.toString(fieldsNames));
        }

        // ensure existence of field types
        if (externalEntry.getFieldsTypes() == null)
            throw new IllegalArgumentException("Insufficient field-type information in ExternalEntry for class: "
                    + externalEntry.getClassName()
                    + ";\nTo appropriately introduce the ExternalEntry, construct it with it's field-types"
                    + " or use the ExternalEntry.setFieldsTypes(String[]) method.");

        final SpaceIndexType[] indices = getExternalEntryIndices(externalEntry);
        String[] superClasses = externalEntry.getSuperClassesNames();
        if (superClasses == null || superClasses.length == 0) {
            superClasses = new String[2];
            superClasses[0] = externalEntry.getClassName();
            superClasses[1] = Object.class.getName();
        } else {
            int length = superClasses.length;
            boolean addClassName = false, addObject = false;
            if (!superClasses[0].equals(externalEntry.getClassName())) {
                addClassName = true;
                length++;
            }

            if (!superClasses[superClasses.length - 1].equals(Object.class.getName())) {
                addObject = true;
                length++;
            }

            String[] temp = new String[length];
            if (addClassName)
                temp[0] = externalEntry.getClassName();

            System.arraycopy(superClasses, 0, temp, addClassName ? 1 : 0, superClasses.length);

            if (addObject)
                temp[temp.length - 1] = Object.class.getName();
        }

        final PropertyInfo[] properties = new PropertyInfo[fieldsNames.length];
        final Map<String, SpaceIndex> indexes = new HashMap<String, SpaceIndex>();
        for (int i = 0; i < properties.length; i++) {
            properties[i] = PropertyInfo.builder(fieldsNames[i]).type(fieldsTypes[i]).defaultStorageType(_storageType).build();
            if (indices[i] != null && indices[i].isIndexed()) {
                SpaceIndex index = new SpacePropertyIndex(fieldsNames[i], indices[i], false, i);
                indexes.put(index.getName(), index);
            }
        }

        FifoSupport fifoMode = externalEntry.isFifo() ? FifoSupport.OPERATION : FifoSupport.OFF;
        if (isFifoProxy())
            fifoMode = FifoSupport.ALL;

        final boolean supportsDynamicProperties = false;
        final boolean isSystemType = false;
        final String idPropertyName = null;
        final boolean idAutoGenerate = false;
        final String defaultPropertyName = null;

        return new TypeDesc(externalEntry.getClassName(), codeBase, superClasses,
                properties, supportsDynamicProperties, indexes, idPropertyName, idAutoGenerate, defaultPropertyName,
                externalEntry.getRoutingFieldName(), null, null, isSystemType, fifoMode, externalEntry.isReplicatable(),
                true, _storageType, EntryType.EXTERNAL_ENTRY, null, externalEntry.getClass(), SpaceDocument.class, null,
                DotNetStorageType.NULL, PojoDefaults.BLOBSTORE_ENABLED, null, null, null, PojoDefaults.BROADCAST);
    }

    public static ITypeDesc createPbsTypeDesc(EntryType entryType, String className, String codeBase, String[] superClassesNames,
                                              String[] fieldsNames, String[] fieldsTypes, SpaceIndexType[] fieldsIndexes,
                                              String idPropertyName, boolean idAutoGenerate, String routingPropertyName,
                                              FifoSupport fifoMode, boolean isReplicable, boolean supportsOptimisticLocking, boolean supportsDynamicProperties) {
        final boolean blobstoreEnabled = true;
        // Create properties:
        final PropertyInfo[] properties = new PropertyInfo[fieldsNames.length];
        final Map<String, SpaceIndex> indexes = new HashMap<String, SpaceIndex>();
        for (int i = 0; i < properties.length; i++) {
            properties[i] = PropertyInfo.builder(fieldsNames[i])
                    .type(fieldsTypes[i])
                    .storageType(StorageType.OBJECT)
                    .build();
            if (fieldsIndexes[i] != SpaceIndexType.NONE) {
                boolean isUnique = fieldsNames[i].equals(idPropertyName) && !idAutoGenerate;
                SpaceIndex index = new SpacePropertyIndex(fieldsNames[i], fieldsIndexes[i], isUnique, i);
                indexes.put(index.getName(), index);
            }
        }

        // Do not set default property - the typeDesc ctor will calculate it.
        final String defaultPropertyName = null;
        final boolean isSystemType = false;
        //TODO FG : add fifo grouping property and indexes
        // Create type descriptor:
        return new TypeDesc(className, codeBase, superClassesNames,
                properties, supportsDynamicProperties, indexes, idPropertyName, idAutoGenerate, defaultPropertyName, routingPropertyName,
                null, null, isSystemType, fifoMode, isReplicable, supportsOptimisticLocking, StorageType.OBJECT,
                entryType, null, ExternalEntry.class, SpaceDocument.class, null, DotNetStorageType.NULL,
                blobstoreEnabled, null, null, null, PojoDefaults.BROADCAST);
    }

    public static ITypeDesc createPbsExplicitTypeDesc(EntryType entryType, String className, String[] superClassesNames,
                                                      PropertyInfo[] properties, Map<String, SpaceIndex> indexes,
                                                      String idPropertyName, boolean idAutoGenerate, String routingPropertyName,
                                                      String fifoGroupingPropertyPath, Set<String> fifoGroupingIndexPaths, FifoSupport fifoMode, boolean isReplicable, boolean supportsOptimisticLocking, boolean supportsDynamicProperties, byte dynamicPropertiesStorageType,
                                                      String documentWrapperType, boolean blobstoreEnabled) {

        // Create properties:

        final String defaultPropertyName = null;
        final boolean isSystemType = false;
        return new TypeDesc(className, null, superClassesNames,
                properties, supportsDynamicProperties, indexes, idPropertyName, idAutoGenerate, defaultPropertyName, routingPropertyName,
                fifoGroupingPropertyPath, fifoGroupingIndexPaths, isSystemType, fifoMode, isReplicable, supportsOptimisticLocking, StorageType.OBJECT,
                entryType, null, ExternalEntry.class, SpaceDocument.class, documentWrapperType,
                dynamicPropertiesStorageType, blobstoreEnabled, null, null, null, PojoDefaults.BROADCAST);
    }

    private String getEntryIndices(Class<?> realClass, String[] fieldsNames, String[] fieldTypes, SpaceIndexType[] indexTypes) {
        String firstIndexName = null;

        try {
            final Method method = realClass.getMethod("__getSpaceIndexedFields");
            if (!Modifier.isStatic(method.getModifiers()))
                throw new SpaceMetadataException("Entry Class: " + realClass.getName() + " contains a Non-Static, overloaded __getSpaceIndexedFields() method.\n Such method should be defined as static.");

            final String[] indicators = (String[]) method.invoke(null);
            if (indicators == null)
                return null;

            for (int i = 0; i < indicators.length; i++) {
                String indexName = indicators[i];
                int pos = getPosition(fieldsNames, indexName);
                if (pos != -1) {
                    indexTypes[pos] = SpaceIndexType.EQUAL;
                    if (firstIndexName == null)
                        firstIndexName = indexName;
                } else
                    _logger.error("Field: " + indexName + " is not found or not usable in class: "
                            + realClass.getName() + " although it is returned by __getSpaceIndexedFields()");
            }
        } catch (NoSuchMethodException e) {
        } catch (InvocationTargetException e) {
            throw new ConversionException(e);
        } catch (IllegalAccessException e) {
            throw new ConversionException(e);
        }

        return firstIndexName;
    }

    private SpaceIndexType[] getExternalEntryIndices(ExternalEntry entry) {
        SpaceIndexType[] indexTypes = IndexTypeHelper.fromOld(entry.getIndexIndicators());
        if (indexTypes != null)
            return indexTypes;

        final String[] fieldsNames = entry.getFieldsNames();
        indexTypes = new SpaceIndexType[fieldsNames.length];

        /*
        * Explicit or Implicit indexing.
        * For explicit, associate user indicated PrimaryKey or first defined index.
        * For Implicit, take the first Basic type.
        */
        String field = entry.getRoutingFieldName() == null ? entry.getPrimaryKeyName() : entry.getRoutingFieldName();
        if (field != null) {
            for (int i = 0; i < fieldsNames.length; i++) {
                if (fieldsNames[i].equals(field)) {
                    indexTypes[i] = SpaceIndexType.EQUAL;
                    break;
                }
            }

            return indexTypes;
        }

        return indexTypes;
    }

    private static <T> int getPosition(T[] array, T item) {
        for (int i = 0; i < array.length; i++)
            if (array[i].equals(item))
                return i;

        return -1;
    }

    private boolean isFifoProxy() {
        return _spaceProxy != null ? _spaceProxy.isFifo() : false;
    }

    private boolean isFifoProxyModifiers() {
        return _spaceProxy != null ? ReadModifiers.isFifo(_spaceProxy.getReadModifiers()) : false;
    }

    private boolean isVersionedProxy() {
        return _spaceProxy != null ? _spaceProxy.isOptimisticLockingEnabled() : false;
    }
}
