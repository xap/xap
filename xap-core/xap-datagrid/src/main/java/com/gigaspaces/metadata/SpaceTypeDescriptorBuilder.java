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


package com.gigaspaces.metadata;

import com.gigaspaces.annotation.pojo.FifoSupport;
import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.client.storage_adapters.PropertyStorageAdapter;
import com.gigaspaces.client.storage_adapters.class_storage_adapters.ClassBinaryStorageAdapter;
import com.gigaspaces.client.storage_adapters.class_storage_adapters.SequentialClassBinaryStorageAdapter;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.metadata.*;
import com.gigaspaces.internal.utils.ObjectUtils;
import com.gigaspaces.metadata.index.ISpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexFactory;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.gigaspaces.metadata.index.SpacePropertyIndex;
import com.gigaspaces.query.extension.SpaceQueryExtension;
import com.gigaspaces.query.extension.metadata.QueryExtensionPathInfo;
import com.gigaspaces.query.extension.metadata.impl.TypeQueryExtensionsImpl;
import com.j_spaces.core.client.ExternalEntry;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A builder class for creating {@link SpaceTypeDescriptor} instances.
 *
 * For example, to create a type descriptor with type name 'foo' and id property 'bar' use the
 * following code: <code>new SpaceTypeDescriptorBuilder("foo").setIdProperty("bar").create()</code>
 *
 * @author Niv Ingberg
 * @see com.gigaspaces.metadata.SpaceTypeDescriptor
 * @since 8.0
 */

public class SpaceTypeDescriptorBuilder {
    private static final String ROOT_TYPE_NAME = Object.class.getName();
    private static final String DEFAULT_ID_PROPERTY_NAME = "_spaceId";

    private final String _typeName;
    private final SpaceTypeDescriptor _superTypeDescriptor;
    private final SortedMap<String, PropertyInfo.Builder> _fixedProperties;
    private final Map<String, SpaceIndex> _indexes;
    private TypeQueryExtensionsImpl _queryExtensionsInfo;
    private Class<? extends Object> _objectClass;
    private Class<? extends SpaceDocument> _documentWrapperClass;
    private FifoSupport _fifoSupport;
    private Boolean _replicable;
    private Boolean _systemType;
    private String _idPropertyName;
    private boolean _idAutoGenerate;
    private String _routingPropertyName;
    private String _fifoGroupingPropertyPath;
    private Set<String> _fifoGroupingIndexes;
    private Boolean _supportsDynamicProperties;
    private Boolean _supportsOptimisticLocking;

    private StorageType _storageType;
    private Boolean _blobstoreEnabled;
    private String _sequenceNumberPropertyName;
    private boolean _sequenceNumberFromDocumentBuilder;
    private Class<? extends ClassBinaryStorageAdapter> binaryStorageAdapterClass;
    private Boolean _broadcast;

    /**
     * Initialize a type descriptor builder using the specified type name.
     *
     * @param typeName Name of type.
     */
    public SpaceTypeDescriptorBuilder(String typeName) {
        this(typeName, null);
    }

    /**
     * Initialize a type descriptor builder using the specified type name and super type
     * descriptor.
     *
     * @param typeName            Name of type.
     * @param superTypeDescriptor Type descriptor of super type.
     */
    public SpaceTypeDescriptorBuilder(String typeName, SpaceTypeDescriptor superTypeDescriptor) {
        if (typeName == null || typeName.length() == 0)
            throw new IllegalArgumentException("Argument cannot be null or empty - 'typeName'.");
        if (typeName.equals(ROOT_TYPE_NAME))
            throw new IllegalArgumentException("Argument 'typeName' cannot be '" + ROOT_TYPE_NAME + "' - it is reserved for internal usage.");

        this._typeName = typeName;
        this._superTypeDescriptor = superTypeDescriptor;
        this._fixedProperties = new TreeMap<>();
        this._indexes = new HashMap<>();
        this._fifoGroupingIndexes = new HashSet<>();
        this._storageType = StorageType.DEFAULT;
        this._blobstoreEnabled = PojoDefaults.BLOBSTORE_ENABLED;
        this._broadcast = PojoDefaults.BROADCAST;
    }

    /*
    public SpaceTypeDescriptorBuilder(Class<?> type)
    {
        this(assertNotNull(type, "type"), type.getSuperclass() == null ? null : new SpaceTypeDescriptorBuilder(type.getSuperclass()).create());
    }
     */
    public SpaceTypeDescriptorBuilder(Class<?> type, SpaceTypeDescriptor superTypeDescriptor) {

        this(ObjectUtils.assertArgumentNotNull(type, "type").getName(), superTypeDescriptor);

        // Validations:
        if (type.isInterface())
            throw new IllegalArgumentException("Creating SpaceTypeDescriptor for interfaces is not supported.");
        if (type.isArray())
            throw new IllegalArgumentException("Creating SpaceTypeDescriptor for arrays is not supported.");
        if (type.isEnum())
            throw new IllegalArgumentException("Creating SpaceTypeDescriptor for enumerations is not supported.");
        if (type.isPrimitive())
            throw new IllegalArgumentException("Creating SpaceTypeDescriptor for primitive types is not supported.");
        if (net.jini.core.entry.Entry.class.isAssignableFrom(type))
            throw new IllegalArgumentException("Creating SpaceTypeDescriptor for types implementing 'net.jini.core.entry.Entry' is not supported.");

        Class<?> superType = type.getSuperclass();
        if (superType != null && superType.getName().equals(ROOT_TYPE_NAME))
            superType = null;
        if (superTypeDescriptor != null && superTypeDescriptor.getTypeName().equals(ROOT_TYPE_NAME))
            superTypeDescriptor = null;
        if (superType == null && superTypeDescriptor != null)
            throw new IllegalArgumentException("Type '" + type.getName() + "' has no super class, but superTypeDescriptor is not null.");
        if (superType != null) {
            if (superTypeDescriptor == null)
                throw new IllegalArgumentException("Type '" + type.getName() + "' has super class '" + superType.getName() + "', but superTypeDescriptor is null.");
            if (!superType.equals(superTypeDescriptor.getObjectClass()))
                throw new IllegalArgumentException("Type '" + type.getName() + "' has super class '" + superType.getName() + "', but superTypeDescriptor is of type '"
                        + superTypeDescriptor.getTypeName() + "'.");
        }

        // Get POJO type info:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        // Add fixed properties defined in this type:
        int numOfFixedProperties = typeInfo.getNumOfSpaceProperties();
        int numOfSuperFixedProperties = superTypeDescriptor == null ? 0 : superTypeDescriptor.getNumOfFixedProperties();
        for (int i = numOfSuperFixedProperties; i < numOfFixedProperties; i++) {
            final SpacePropertyInfo property = typeInfo.getProperty(i);
            addFixedProperty(property.getName(), property.getType().getName());
        }

        // Copy indexes which do not appear in super type:
        for (SpaceIndex index : typeInfo.getIndexes().values()) {
            if (superTypeDescriptor == null || !superTypeDescriptor.getIndexes().containsKey(index.getName()))
                addIndex(index);
        }

        _objectClass = type;
        _systemType = typeInfo.isSystemClass();
        _fifoSupport = typeInfo.getFifoSupport();
        _replicable = typeInfo.isReplicate();
        _supportsDynamicProperties = typeInfo.getDynamicPropertiesProperty() != null;
        _supportsOptimisticLocking = typeInfo.getVersionProperty() != null;
        _idPropertyName = typeInfo.getIdProperty() != null ? typeInfo.getIdProperty().getName() : null;
        _idAutoGenerate = typeInfo.getIdAutoGenerate();
        _routingPropertyName = typeInfo.getRoutingProperty() != null ? typeInfo.getRoutingProperty().getName() : null;
        _blobstoreEnabled = typeInfo.isBlobstoreEnabled();
        _broadcast = typeInfo.isBroadcast();
    }

    /**
     * Sets the document wrapper class for this type. A document wrapper class is a java class which
     * extends {@link SpaceDocument} and can be used as a surrogate for a specific type.
     *
     * @param documentWrapperClass The document wrapper class for this type.
     */
    public SpaceTypeDescriptorBuilder documentWrapperClass(Class<? extends SpaceDocument> documentWrapperClass) {
        _documentWrapperClass = assertNotNull(documentWrapperClass, "documentWrapperClass");
        return this;
    }

    /**
     *
     * @since 15.8
     * @see com.gigaspaces.annotation.pojo.SpaceClassBinaryStorage
     */
    public SpaceTypeDescriptorBuilder binaryStorage() {
        return binaryStorage(ClassBinaryStorageLayout.DEFAULT);
    }

    /**
     *
     * @since 15.8
     * @see com.gigaspaces.annotation.pojo.SpaceClassBinaryStorage
     */
    public SpaceTypeDescriptorBuilder binaryStorage(ClassBinaryStorageLayout layout) {
        return binaryStorage(layout.getAdapterClass());
    }

    /**
     *
     * @since 15.8
     * @see com.gigaspaces.annotation.pojo.SpaceClassBinaryStorage
     */
    @ExperimentalApi
    private SpaceTypeDescriptorBuilder binaryStorage(Class<? extends ClassBinaryStorageAdapter> binaryStorageAdapterClass) {
        this.binaryStorageAdapterClass = assertNotNull(binaryStorageAdapterClass, "binaryStorageAdapterClass");
        storageType(StorageType.BINARY);
        return this;
    }

    /**
     * Sets this type FIFO support.
     *
     * @param fifoSupport Desired FIFO support.
     */
    public SpaceTypeDescriptorBuilder fifoSupport(FifoSupport fifoSupport) {
        this._fifoSupport = assertNotNull(fifoSupport, "fifoSupport");
        return this;
    }

    /**
     * Sets whether this type is replicable or not.
     *
     * @param replicable true if this type is replicable, false otherwise.
     */
    public SpaceTypeDescriptorBuilder replicable(boolean replicable) {
        this._replicable = replicable;
        return this;
    }

    /**
     * Sets whether for this type blobstore data is enabled when cache policy is set for blobstore.
     *
     * @param blobstoreEnabled true if this type blobstore data is enabled, false otherwise.
     */
    public SpaceTypeDescriptorBuilder setBlobstoreEnabled(boolean blobstoreEnabled) {
        this._blobstoreEnabled = blobstoreEnabled;
        return this;
    }

    /**
     * Sets whether this type should be distributed across partitions.
     *
     * @param broadcast true if this type is broadcast, false otherwise.
     */
    public SpaceTypeDescriptorBuilder broadcast(boolean broadcast) {
        this._broadcast = broadcast;
        return this;
    }


    /**
     * Sets whether or not this type supports dynamic properties.
     *
     * @param supportsDynamicProperties true if this type supports dynamic properties, false
     *                                  otherwise.
     */
    public SpaceTypeDescriptorBuilder supportsDynamicProperties(boolean supportsDynamicProperties) {
        this._supportsDynamicProperties = supportsDynamicProperties;
        return this;
    }

    /**
     * Sets whether or not this type supports optimistic locking.
     *
     * @param supportsOptimisticLocking true if this type supports optimistic locking, false
     *                                  otherwise.
     */
    public SpaceTypeDescriptorBuilder supportsOptimisticLocking(boolean supportsOptimisticLocking) {
        this._supportsOptimisticLocking = supportsOptimisticLocking;
        return this;
    }

    /**
     * Sets type's storage type
     */
    public SpaceTypeDescriptorBuilder storageType(StorageType storageType) {
        assertNotNull(storageType, "storageType");
        if (this._storageType != null && this._storageType != StorageType.DEFAULT && this._storageType != storageType)
            throw new IllegalStateException("Cannot set storage type to '" + storageType
                    + "' - it was already set to '" + _storageType + "'.");
        this._storageType = storageType;
        return this;
    }

    /**
     * Adds a property to the fixed properties set.
     *
     * @param propertyName Name of property.
     * @param propertyType Type of property.
     */
    public SpaceTypeDescriptorBuilder addFixedProperty(String propertyName, Class<?> propertyType) {
        return addFixedProperty(property(propertyName, propertyType));
    }

    /**
     * Adds a property to the fixed properties set.
     *
     * @param propertyName    Name of property.
     * @param propertyType    Type of property.
     * @param documentSupport Document support of property.
     */
    public SpaceTypeDescriptorBuilder addFixedProperty(String propertyName, Class<?> propertyType, SpaceDocumentSupport documentSupport) {
        return addFixedProperty(property(propertyName, propertyType).documentSupport(documentSupport));
    }

    /**
     * Adds a property to the fixed properties set.
     *
     * @param propertyName Name of property.
     * @param propertyType Type of property.
     * @param storageType  StorageType of property
     * @since 9.0.0
     */
    public SpaceTypeDescriptorBuilder addFixedProperty(String propertyName, Class<?> propertyType, StorageType storageType) {
        return addFixedProperty(property(propertyName, propertyType).storageType(storageType));
    }

    /**
     * Adds a property to the fixed properties set.
     *
     * @param propertyName Name of property.
     * @param propertyType Type of property.
     * @param propertyStorageAdapter  PropertyStorageAdapter class
     * @since 15.2.0
     */
    public SpaceTypeDescriptorBuilder addFixedProperty(String propertyName, Class<?> propertyType, Class<? extends PropertyStorageAdapter> propertyStorageAdapter) {
        return addFixedProperty(property(propertyName, propertyType).storageAdapter(propertyStorageAdapter));
    }

    /**
     * Adds a property to the fixed properties set.
     *
     * @param propertyName    Name of property.
     * @param propertyType    Type of property.
     * @param documentSupport Document support of property.
     * @param storageType     StorageType of property
     * @since 9.0.0
     */
    public SpaceTypeDescriptorBuilder addFixedProperty(String propertyName, Class<?> propertyType, SpaceDocumentSupport documentSupport, StorageType storageType) {
        return addFixedProperty(property(propertyName, propertyType).documentSupport(documentSupport).storageType(storageType));
    }

    /**
     * Adds a property to the fixed properties set.
     *
     * @param propertyName    Name of property.
     * @param propertyType    Type of property.
     * @param documentSupport Document support of property.
     * @param propertyStorageAdapter  PropertyStorageAdapter class
     * @since 9.0.0
     */
    public SpaceTypeDescriptorBuilder addFixedProperty(String propertyName, Class<?> propertyType, SpaceDocumentSupport documentSupport, Class<? extends PropertyStorageAdapter> propertyStorageAdapter) {
        return addFixedProperty(property(propertyName, propertyType).documentSupport(documentSupport).storageAdapter(propertyStorageAdapter));
    }

    private static PropertyInfo.Builder property(String propertyName, Class<?> propertyType) {
        return PropertyInfo.builder(assertNotNull(propertyName, "propertyName"))
                .type(assertNotNull(propertyType, "propertyType"));
    }

    /**
     * Adds a property to the fixed properties set.
     *
     * @param propertyName     Name of property.
     * @param propertyTypeName Name of type of property.
     */
    public SpaceTypeDescriptorBuilder addFixedProperty(String propertyName, String propertyTypeName) {
        return addFixedProperty(property(propertyName, propertyTypeName));
    }

    public SpaceTypeDescriptorBuilder addFixedProperty(String propertyName, String propertyTypeName, SpaceDocumentSupport documentSupport) {
        return addFixedProperty(property(propertyName, propertyTypeName).documentSupport(documentSupport));
    }

    public SpaceTypeDescriptorBuilder addFixedProperty(String propertyName, String propertyTypeName, SpaceDocumentSupport documentSupport, StorageType storageType) {
        return addFixedProperty(property(propertyName, propertyTypeName).documentSupport(documentSupport).storageType(storageType));
    }

    public SpaceTypeDescriptorBuilder addFixedProperty(String propertyName, String propertyTypeName, SpaceDocumentSupport documentSupport, Class<? extends PropertyStorageAdapter> propertyStorageAdapter) {
        return addFixedProperty(property(propertyName, propertyTypeName).documentSupport(documentSupport).storageAdapter(propertyStorageAdapter));
    }

    private static PropertyInfo.Builder property(String propertyName, String propertyTypeName) {
        return PropertyInfo.builder(assertNotNull(propertyName, "propertyName"))
                .type(assertNotNull(propertyTypeName, "propertyTypeName"));
    }

    private static <T> T assertNotNull(T obj, String name) {
        if (obj == null)
            throw new IllegalArgumentException("Argument cannot be null - '" + name + "'.");
        return obj;
    }

    /**
     * Adds a property to the fixed properties set.
     */
    private SpaceTypeDescriptorBuilder addFixedProperty(PropertyInfo.Builder builder) {
        String name = builder.getName();
        // Validate property is not a duplicate:
        if (_fixedProperties.containsKey(name))
            throw new IllegalArgumentException("Cannot add fixed property '" + name + "' - a property with the same name is already defined.");
        // Validate property does not exist in super type:
        if (_superTypeDescriptor != null && _superTypeDescriptor.getFixedPropertyPosition(name) != -1)
            throw new IllegalArgumentException("Cannot add fixed property '" + name + "' - a property with the same name is defined in the super type.");

        _fixedProperties.put(name, builder);
        return this;
    }

    /**
     * Sets the ID property.
     *
     * @param idPropertyName Name of ID property.
     */
    public SpaceTypeDescriptorBuilder idProperty(String idPropertyName) {
        return idProperty(idPropertyName, false);
    }

    /**
     * Sets the ID property.
     *
     * @param idPropertyName Name of ID property.
     * @param autoGenerateId false if the uid is generated using the id value, true if the uid is
     *                       automatically generated.
     */
    public SpaceTypeDescriptorBuilder idProperty(String idPropertyName, boolean autoGenerateId) {
        final SpaceIndexType indexType = autoGenerateId ? SpaceIndexType.NONE : SpaceIndexType.EQUAL;
        return idProperty(idPropertyName, autoGenerateId, indexType);
    }

    /**
     * Sets the ID property.
     *
     * @param idPropertyName Name of ID property.
     * @param autoGenerateId false if the uid is generated using the id value, true if the uid is
     *                       automatically generated.
     * @param indexType      Type of index.
     */
    public SpaceTypeDescriptorBuilder idProperty(String idPropertyName, boolean autoGenerateId, SpaceIndexType indexType) {
        // Validate id not already set:
        if (_idPropertyName != null)
            throw new IllegalStateException("Cannot set id property to '" + idPropertyName + "' - it was already set to '" + _idPropertyName + "'.");

        this._idPropertyName = assertNotNull(idPropertyName, "idPropertyName");
        this._idAutoGenerate = autoGenerateId;
        addIndexIfNotExists(idPropertyName, assertNotNull(indexType, "indexType"));
        return this;
    }

    /**
     * Sets the routing property.
     *
     * @param routingPropertyName Name of routing property.
     */
    public SpaceTypeDescriptorBuilder routingProperty(String routingPropertyName) {
        return routingProperty(routingPropertyName, SpaceIndexType.EQUAL);
    }

    /**
     * Sets the routing property.
     *
     * @param routingPropertyName Name of routing property.
     * @param indexType           Routing property index type.
     */
    public SpaceTypeDescriptorBuilder routingProperty(String routingPropertyName, SpaceIndexType indexType) {
        // Validate id not already set:
        if (_routingPropertyName != null)
            throw new IllegalStateException("Cannot set routing property to '" + routingPropertyName + "' - it was already set to '" + _routingPropertyName + "'.");

        this._routingPropertyName = assertNotNull(routingPropertyName, "routingPropertyName");
        addIndexIfNotExists(routingPropertyName, assertNotNull(indexType, "indexType"));
        return this;
    }

    /**
     * Sets the fifo grouping property
     *
     * @param fifoGroupingPropertyPath Path of fifo grouping property
     * @since 9.0.0
     */
    public SpaceTypeDescriptorBuilder fifoGroupingProperty(String fifoGroupingPropertyPath) {
        // Validate:
        assertNotNull(fifoGroupingPropertyPath, "fifoGroupingPropertyPath");

        // Validate fifo grouping not already set:
        if (_fifoGroupingPropertyPath != null)
            throw new IllegalStateException("Cannot set fifo grouping to '" + fifoGroupingPropertyPath
                    + "' - it was already set to '" + _fifoGroupingPropertyPath + "'.");

        // validate fifo grouping not on collection
        validateNoCollectionPath(fifoGroupingPropertyPath);

        this._fifoGroupingPropertyPath = fifoGroupingPropertyPath;
        return this;
    }

    public SpaceTypeDescriptorBuilder sequenceNumberProperty(String sequenceNumberPropertyName, boolean sequenceNumberFromDocumentBuilder) {
        // Validate:
        if (sequenceNumberPropertyName == null || sequenceNumberPropertyName.length() == 0)
            throw new IllegalArgumentException("Argument cannot be null/empty - 'sequenceNumberPropertyName'.");

        // Validate not already set:
        if (_sequenceNumberPropertyName != null && !sequenceNumberPropertyName.equals(_sequenceNumberPropertyName))
            throw new IllegalStateException("Cannot set sequenceNumberPropertyName to '" + sequenceNumberPropertyName
                    + "' - it was already set to '" + _sequenceNumberPropertyName + "'.");

        // validate
        validateBasic(sequenceNumberPropertyName);

        this._sequenceNumberPropertyName = sequenceNumberPropertyName;
        this._sequenceNumberFromDocumentBuilder = sequenceNumberFromDocumentBuilder;
        return this;
    }

    /**
     * Sets a fifo grouping index
     *
     * @param fifoGroupingIndexPath Path of fifo grouping
     * @since 9.0.0
     */
    public SpaceTypeDescriptorBuilder addFifoGroupingIndex(String fifoGroupingIndexPath) {
        // Validate:
        assertNotNull(fifoGroupingIndexPath, "fifoGroupingIndexPath");

        // validate fifo grouping not on collection
        validateNoCollectionPath(fifoGroupingIndexPath);

        this._fifoGroupingIndexes.add(fifoGroupingIndexPath);

        return this;
    }

    /**
     * Adds an index of the specified type for the specified property.
     *
     * @param propertyName Name of property to index.
     * @param indexType    Type of index.
     */
    public SpaceTypeDescriptorBuilder addPropertyIndex(String propertyName, SpaceIndexType indexType) {
        return addPropertyIndex(propertyName, indexType, false);
    }

    /**
     * Adds an index of the specified type for the specified property.
     *
     * @param propertyName Name of property to index.
     * @param indexType    Type of index.
     * @param unique       is it a unique index
     */
    public SpaceTypeDescriptorBuilder addPropertyIndex(String propertyName, SpaceIndexType indexType, boolean unique) {
        return addIndex(SpaceIndexFactory.createPropertyIndex(propertyName, indexType, unique));
    }

    /**
     * Adds an index of the specified type for the specified path.
     *
     * @param path      Path to index
     * @param indexType Type of index.
     */
    public SpaceTypeDescriptorBuilder addPathIndex(String path, SpaceIndexType indexType) {
        return addPathIndex(path, indexType, false);
    }

    /**
     * Adds an index of the specified type for the specified path.
     *
     * @param path      Path to index
     * @param indexType Type of index.
     * @param unique    is it a unique index
     */
    public SpaceTypeDescriptorBuilder addPathIndex(String path, SpaceIndexType indexType, boolean unique) {
        return addIndex(SpaceIndexFactory.createPathIndex(path, indexType, unique));
    }

    /**
     * Adds an index of the specified type for the specified path.
     *
     * @param paths     Paths to index
     * @param indexType Type of index.
     */
    @Deprecated
    public SpaceTypeDescriptorBuilder addCompoundIndex(String[] paths, SpaceIndexType indexType) {
        if (indexType != SpaceIndexType.BASIC && indexType != SpaceIndexType.EQUAL)
            throw new UnsupportedOperationException("only EQUAL index type is supported for compoundindex");
        return addCompoundIndex(paths, indexType, false);
    }

    /**
     * Adds an index of the specified type for the specified path.
     *
     * @param paths     Paths to index
     */
    public SpaceTypeDescriptorBuilder addCompoundIndex(String[] paths) {
        return addCompoundIndex(paths, SpaceIndexType.EQUAL, false);
    }

    /**
     * Adds an index of the specified type for the specified path.
     *
     * @param paths     Paths to index
     * @param indexType Type of index.
     * @param unique    is it a unique index
     */
    @Deprecated
    public SpaceTypeDescriptorBuilder addCompoundIndex(String[] paths, SpaceIndexType indexType, boolean unique) {
        if (indexType != SpaceIndexType.BASIC && indexType != SpaceIndexType.EQUAL)
            throw new UnsupportedOperationException("only BASIC index type is supported for compoundindex");
        return addIndex(SpaceIndexFactory.createCompoundIndex(paths, indexType, null, unique));
    }

    /**
     * Adds an index of the specified type for the specified path.
     *
     * @param paths     Paths to index
     * @param unique    is it a unique index
     */
    public SpaceTypeDescriptorBuilder addCompoundIndex(String[] paths, boolean unique) {
        return addIndex(SpaceIndexFactory.createCompoundIndex(paths, SpaceIndexType.EQUAL, null, unique));
    }

    /**
     * Adds the specified index to the type's index set.
     *
     * @param index Index to add.
     */
    public SpaceTypeDescriptorBuilder addIndex(SpaceIndex index) {
        // Validate:
        assertNotNull(index, "index");
        // Validate index is not a duplicate:
        if (_indexes.containsKey(index.getName()))
            throw new IllegalArgumentException("Cannot add index '" + index.getName() + "' - an index with the same name is already defined.");
        // Validate index is not defined in super type:
        if (_superTypeDescriptor != null && _superTypeDescriptor.getIndexes().containsKey(index.getName()))
            throw new IllegalArgumentException("Cannot add index '" + index.getName() + "' - an index with the same name is defined in the super type.");

        _indexes.put(index.getName(), index);
        return this;
    }

    private void addIndexIfNotExists(String propertyName, SpaceIndexType indexType) {
        if (indexType != SpaceIndexType.NONE) {
            // Check if an index is already defined for this property:
            SpaceIndex index = _indexes.get(propertyName);
            if (index == null)
                addPropertyIndex(propertyName, indexType);
            else if (!isEquivalent(index.getIndexType(), indexType))
                throw new IllegalArgumentException("Cannot add index '" + index.getName() + "' as " + indexType + " - it's already indexed as " + index.getIndexType());
        }
    }

    private static boolean isEquivalent(SpaceIndexType a, SpaceIndexType b) {
        if (a == b)
            return true;
        if ((a == SpaceIndexType.BASIC || a == SpaceIndexType.EQUAL) && (b == SpaceIndexType.BASIC || b == SpaceIndexType.EQUAL))
            return true;
        if ((a == SpaceIndexType.EXTENDED || a == SpaceIndexType.EQUAL_AND_ORDERED) && (b == SpaceIndexType.EXTENDED || b == SpaceIndexType.EQUAL_AND_ORDERED))
            return true;
        return false;
    }


    /**
     * Adds a QueryExtension information for the specified path
     *
     * @param path                     Path to decorate
     * @param queryExtensionAnnotation Query Extension annotation encapsulating mapping info
     */
    public SpaceTypeDescriptorBuilder addQueryExtensionInfo(String path, Class<? extends Annotation> queryExtensionAnnotation) {
        if(!queryExtensionAnnotation.isAnnotationPresent(SpaceQueryExtension.class))
            throw new IllegalArgumentException("Annotation " + queryExtensionAnnotation + " is not a space query extension annotation");
        createQueryExtensionInfoIfNeeded();
        _queryExtensionsInfo.add(path, queryExtensionAnnotation);
        return this;
    }

    /**
     * Adds a QueryExtension information for the specified path
     *
     * @param path                     Path to decorate
     * @param pathInfo                 Query Extension encapsulating mapping info
     */
    public SpaceTypeDescriptorBuilder addQueryExtensionInfo(String path, QueryExtensionPathInfo pathInfo) {
        createQueryExtensionInfoIfNeeded();
        _queryExtensionsInfo.add(path, pathInfo);
        return this;
    }

    private void createQueryExtensionInfoIfNeeded() {
        if (_queryExtensionsInfo == null)
            _queryExtensionsInfo = new TypeQueryExtensionsImpl();
    }

    /**
     * Create the space type descriptor using the gathered information.
     */
    public SpaceTypeDescriptor create() {
        applyDefaults();

        boolean binaryClassStorage = binaryStorageAdapterClass != null;
        final String[] superTypesNames = getSuperTypesNames(_typeName, _superTypeDescriptor);
        final PropertyInfo[] fixedProperties = initFixedProperties(_fixedProperties, _superTypeDescriptor, _storageType, binaryClassStorage, _indexes.keySet());
        final Map<String, SpaceIndex> indexes = initIndexes(_indexes, fixedProperties, _idPropertyName, _superTypeDescriptor);
        final String codeBase = null;                            // TODO: What about pojo?
        final EntryType entryType = _objectClass == null ? EntryType.DOCUMENT_JAVA : EntryType.OBJECT_JAVA;

        // If dynamic properties are not supported, validate id and routing properties are defined:
        if (!_supportsDynamicProperties) {
            validatePropertyExists(_idPropertyName, fixedProperties);
            validatePropertyExists(_routingPropertyName, fixedProperties);
        }

        if(_broadcast)
            validateBroadcast();

        return new TypeDesc(
                _typeName,
                codeBase,
                superTypesNames,
                fixedProperties,
                _supportsDynamicProperties,
                indexes,
                _idPropertyName,
                _idAutoGenerate,
                null /*defaultPropertyId*/,
                _routingPropertyName,
                _fifoGroupingPropertyPath,
                _fifoGroupingIndexes,
                _systemType,
                _fifoSupport,
                _replicable,
                _supportsOptimisticLocking,
                _storageType,
                entryType,
                _objectClass,
                ExternalEntry.class,
                _documentWrapperClass,
                null,
                DotNetStorageType.NULL,
                _blobstoreEnabled,
                _sequenceNumberPropertyName,
                _queryExtensionsInfo,
                binaryStorageAdapterClass,
                _broadcast);
    }

    private void applyDefaults() {
        if (_systemType == null) {
            if (_superTypeDescriptor != null)
                _systemType = getInternalTypeDesc(_superTypeDescriptor).isSystemType();
            if (_systemType == null)
                _systemType = false;
        }
        if (_fifoSupport == null) {
            if (_superTypeDescriptor != null)
                this._fifoSupport = _superTypeDescriptor.getFifoSupport();
            if (_fifoSupport == null)
                _fifoSupport = PojoDefaults.FIFO_SUPPORT;
        }

        if (_replicable == null) {
            if (_superTypeDescriptor != null)
                this._replicable = _superTypeDescriptor.isReplicable();
            if (_replicable == null)
                _replicable = PojoDefaults.REPLICATE;
        }

        if (_blobstoreEnabled == null) {
            if (_superTypeDescriptor != null)
                this._blobstoreEnabled = _superTypeDescriptor.isBlobstoreEnabled();
            if (_blobstoreEnabled == null)
                _blobstoreEnabled = PojoDefaults.BLOBSTORE_ENABLED;
        }

        if (_broadcast == null) {
            if (_superTypeDescriptor != null)
                this._broadcast = _superTypeDescriptor.isBroadcast();
            if (_broadcast == null)
                _broadcast = PojoDefaults.BROADCAST;
        }

        if (_supportsDynamicProperties == null) {
            if (_superTypeDescriptor != null)
                _supportsDynamicProperties = _superTypeDescriptor.supportsDynamicProperties();
            if (_supportsDynamicProperties == null)
                _supportsDynamicProperties = true;
        }

        if (_supportsOptimisticLocking == null) {
            if (_superTypeDescriptor != null)
                _supportsOptimisticLocking = _superTypeDescriptor.supportsOptimisticLocking();
            if (_supportsOptimisticLocking == null)
                _supportsOptimisticLocking = false;
        }

        if (_documentWrapperClass == null) {
            if (_superTypeDescriptor != null)
                _documentWrapperClass = _superTypeDescriptor.getDocumentWrapperClass();
            if (_documentWrapperClass == null)
                _documentWrapperClass = SpaceDocument.class;
        }


        if (_idPropertyName == null) {
            if (_superTypeDescriptor != null) {
                _idPropertyName = _superTypeDescriptor.getIdPropertyName();
                _idAutoGenerate = getInternalTypeDesc(_superTypeDescriptor).isAutoGenerateId();
            }

            // Add autogenerated id property for virtual types if needed:
            if (_idPropertyName == null && _objectClass == null) {
                _idPropertyName = DEFAULT_ID_PROPERTY_NAME;
                _idAutoGenerate = true;
            }
        }

        if (_idPropertyName != null && !isFixedProperty(_idPropertyName)) {
            if (_idAutoGenerate) {
                addFixedProperty(_idPropertyName, String.class);
            } else {
                addFixedProperty(_idPropertyName, Object.class);
            }
        }

        if (_routingPropertyName == null) {
            if (_superTypeDescriptor != null)
                _routingPropertyName = _superTypeDescriptor.getRoutingPropertyName();
        }

        if (_routingPropertyName != null && !isFixedProperty(_routingPropertyName))
            addFixedProperty(_routingPropertyName, Object.class);

        if (_superTypeDescriptor != null) {
            if (_storageType == StorageType.DEFAULT)
                _storageType = _superTypeDescriptor.getStorageType();
            else
                throw new IllegalStateException("Cannot declare class's storage type [" + _storageType + "] if one has already been defined in the super class [" + _superTypeDescriptor.getStorageType() + "].");

            if (_fifoGroupingPropertyPath != null && _superTypeDescriptor.getFifoGroupingPropertyPath() != null)
                throw new IllegalStateException("Cannot declare a fifo grouping property if one has already been defined in the super class [" + _superTypeDescriptor.getFifoGroupingPropertyPath() + "].");
            if (_fifoGroupingPropertyPath == null)
                _fifoGroupingPropertyPath = _superTypeDescriptor.getFifoGroupingPropertyPath();
            for (String fifoGroupingIndexName : _superTypeDescriptor.getFifoGroupingIndexesPaths())
                _fifoGroupingIndexes.add(fifoGroupingIndexName);

            String superSN = null;
            if (_superTypeDescriptor.hasSequenceNumber())
                superSN = _superTypeDescriptor.getFixedProperty(_superTypeDescriptor.getSequenceNumberFixedPropertyID()).getName();
            if (_sequenceNumberPropertyName != null && superSN != null)
                throw new IllegalStateException("Cannot declare a sequence number property if one has already been defined in the super class [" + superSN + "].");

            if (_sequenceNumberPropertyName == null)
                _sequenceNumberPropertyName = superSN;

        } else if (_storageType == StorageType.DEFAULT)
            _storageType = StorageType.OBJECT;
        if (_sequenceNumberPropertyName != null && _sequenceNumberFromDocumentBuilder && !isFixedProperty(_sequenceNumberPropertyName)) {
            addFixedProperty(_sequenceNumberPropertyName, Long.class);
        }
    }

    private boolean isFixedProperty(String propertyName) {
        if (_fixedProperties.containsKey(propertyName))
            return true;
        if (_superTypeDescriptor != null && _superTypeDescriptor.getFixedProperty(propertyName) != null)
            return true;

        return false;
    }

    private static void validatePropertyExists(String propertyName, PropertyInfo[] properties) {
        if (propertyName == null || propertyName.length() == 0)
            return;

        for (PropertyInfo property : properties)
            if (property.getName().equals(propertyName))
                return;

        throw new IllegalArgumentException("No such property - '" + propertyName + "'.");
    }

    private void validateBroadcast() {
        if(_routingPropertyName != null && !_routingPropertyName.equals(_idPropertyName))
            throw new IllegalArgumentException("Routing property and broadcast table cannot be used together.");
        if(_idAutoGenerate)
            throw new IllegalArgumentException("Auto generated id and broadcast table cannot be used together.");
    }



    private static String[] getSuperTypesNames(String typeName, SpaceTypeDescriptor superTypeDescriptor) {
        if (typeName.equals(ROOT_TYPE_NAME))
            return new String[]{ROOT_TYPE_NAME, ROOT_TYPE_NAME};

        if (superTypeDescriptor == null || superTypeDescriptor.getTypeName().equals(ROOT_TYPE_NAME))
            return new String[]{typeName, ROOT_TYPE_NAME};

        final String[] superSuperTypesNames = getInternalTypeDesc(superTypeDescriptor).getSuperClassesNames();
        final String[] superTypesNames = new String[superSuperTypesNames.length + 1];
        superTypesNames[0] = typeName;
        for (int i = 1; i < superTypesNames.length; i++)
            superTypesNames[i] = superSuperTypesNames[i - 1];

        return superTypesNames;
    }

    private void validateNoCollectionPath(String path) {
        if (path != null && path.length() != 0 && path.indexOf(SpaceCollectionIndex.COLLECTION_INDICATOR) != -1)
            throw new IllegalArgumentException("[" + path + "] collection index cannot be fifo grouping index");
    }

    private void validateBasic(String name) {
        if (name.indexOf(SpaceCollectionIndex.COLLECTION_INDICATOR) != -1 || name.indexOf(".") != -1)
            throw new IllegalArgumentException("[" + name + "] collection/path cannot be sequence number property");
    }


    private static PropertyInfo[] initFixedProperties(SortedMap<String, PropertyInfo.Builder> properties,
                                                      SpaceTypeDescriptor superTypeDesc, StorageType defaultStorageType,
                                                      boolean binaryClassStorage, Set<String> indexesNames) {
        final int numOfSuperFixedProerties = superTypeDesc != null ? superTypeDesc.getNumOfFixedProperties() : 0;
        final int numOfFixedProerties = properties != null ? properties.size() : 0;
        final PropertyInfo[] mergedProperties = new PropertyInfo[numOfSuperFixedProerties + numOfFixedProerties];
        int pos = 0;

        // Copy super properties (if any) to the head:
        if (superTypeDesc != null)
            for (int i = 0; i < numOfSuperFixedProerties; i++)
                mergedProperties[pos++] = (PropertyInfo) superTypeDesc.getFixedProperty(i);

        // Copy properties (if any) to the tail:
        if (properties != null)
            for (Entry<String, PropertyInfo.Builder> pair : properties.entrySet()) {
                PropertyInfo.Builder builder = pair.getValue();
                builder.defaultStorageType(defaultStorageType, binaryClassStorage, indexesNames);
                mergedProperties[pos++] = builder.build();
            }

        return mergedProperties;
    }

    private static Map<String, SpaceIndex> initIndexes(Map<String, SpaceIndex> indexes,
                                                       PropertyInfo[] fixedProperties, String idPropertyName, SpaceTypeDescriptor superTypeDescriptor) {
        Map<String, SpaceIndex> result = new HashMap<String, SpaceIndex>(indexes.size());

        // Transform indexes to property indexes where possible:
        for (SpaceIndex index : indexes.values()) {
            final int position = getPositionOf(index.getName(), fixedProperties);
            if (position != -1) {
                final boolean isUnique = ((ISpaceIndex) index).isUnique() || index.getName().equals(idPropertyName);
                index = new SpacePropertyIndex(index.getName(), index.getIndexType(), isUnique, position);
            }
            result.put(index.getName(), index);
        }

        // Copy indexes from superTypeDescriptor to result:
        if (superTypeDescriptor != null)
            for (SpaceIndex index : superTypeDescriptor.getIndexes().values())
                result.put(index.getName(), index);

        return result;
    }

    private static int getPositionOf(String propertyName, SpacePropertyDescriptor[] properties) {
        for (int i = 0; i < properties.length; i++)
            if (properties[i].getName().equals(propertyName))
                return i;

        return -1;
    }

    // TODO: Reduce usages of this method until it can be removed.
    private static ITypeDesc getInternalTypeDesc(SpaceTypeDescriptor typeDesc) {
        return (ITypeDesc) typeDesc;
    }
}
