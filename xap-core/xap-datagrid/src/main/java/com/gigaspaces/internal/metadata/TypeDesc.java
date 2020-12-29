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

package com.gigaspaces.internal.metadata;

import com.gigaspaces.annotation.pojo.FifoSupport;
import com.gigaspaces.client.storage_adapters.class_storage_adapters.ClassBinaryStorageAdapter;
import com.gigaspaces.client.storage_adapters.class_storage_adapters.ClassBinaryStorageAdapterRegistry;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.io.CustomClassLoaderObjectInputStream;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.space.SpaceUidFactory;
import com.gigaspaces.internal.utils.ReflectionUtils;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.gigaspaces.metadata.SpaceMetadataValidationException;
import com.gigaspaces.metadata.StorageType;
import com.gigaspaces.metadata.index.ISpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexFactory;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.gigaspaces.query.extension.metadata.TypeQueryExtensions;
import com.j_spaces.core.client.ExternalEntry;
import com.j_spaces.core.client.MetaDataEntry;
import com.j_spaces.kernel.ClassLoaderHelper;
import com.j_spaces.kernel.SystemProperties;
import net.jini.core.entry.Entry;

import java.io.*;
import java.util.*;
import java.util.stream.IntStream;

/**
 * @author Niv Ingberg
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class TypeDesc implements ITypeDesc {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;

    public static final int NO_SUCH_PROPERTY = -1;
    private static final boolean ENABLE_EXTERNALIZABLE = Boolean.getBoolean(SystemProperties.SERIALIZE_USING_EXTERNALIZABLE);

    // Serializable content:
    private String _typeName;
    private String _codeBase;
    private String[] _superTypesNames;
    private PropertyInfo[] _fixedProperties;
    private boolean _supportsDynamicProperties;
    private boolean _supportsOptimisticLocking;
    private String _idPropertyName;
    private boolean _autoGenerateId;
    private String _defaultPropertyName;
    private String _routingPropertyName;
    private String _fifoGroupingName;
    private Set<String> _fifoGroupingIndexes;
    private FifoSupport _fifoSupport;
    private boolean _systemType;
    private boolean _replicable;
    private EntryType _objectType;
    private StorageType _storageType;
    boolean _isAllPropertiesObjectStorageType;
    private boolean _blobstoreEnabled;
    private Class<? extends Object> _objectClass;
    private ITypeIntrospector<?> _objectIntrospector;
    private Map<String, SpaceIndex> _indexes;
    private TypeQueryExtensions queryExtensionsInfo;
    private ClassBinaryStorageAdapter classBinaryStorageAdapter;
    private int[] positionsForScanning;
    private boolean _broadcast;

    private int _sequenceNumberFixedPropertyPos;  //-1  if none

    private transient EntryTypeDesc[] _entryTypeDescs;
    private transient String _typeSimpleName;
    private transient boolean _isExternalizable;
    private transient Map<String, Integer> _fixedPropertiesMap;
    private transient int _idPropertyPos;
    private transient int _defaultPropertyPos;
    private transient int _routingPropertyPos;

    private transient Class<? extends ExternalEntry> _externalEntryWrapperClass;
    private transient ITypeIntrospector<? extends ExternalEntry> _externalEntryIntrospector;
    private transient boolean _autoGenerateRouting;
    private transient String _typeUidPrefix;


    //  wrapper class name is kept on the space so it can be passed to the proxies, even if the class is not available on the space side
    // note: document class name and class can be different if the class can't be loaded by the space
    // in this case the class will be SpaceDocument and class name will hold the actual type
    private String _documentWrapperClassName;
    private transient Class<? extends SpaceDocument> _documentWrapperClass;

    private transient ITypeIntrospector<? extends SpaceDocument> _documentIntrospector;
    private transient String[] _restrictedSuperClasses;
    private transient int _checksum;
    private transient int _numOfIndexedProperties;
    private transient int[] _indexedPropertiesIDs;

    private transient String _description;

    private String _dotnetDocumentWrapperTypeName;
    private byte _dotnetDynamicPropertiesStorageType;

    private transient List<SpaceIndex> _compoundIndexes;
    private transient String _primitivePropertiesWithoutNullValues;

    private transient PropertyInfo[] _serializedProperties;
    private transient PropertyInfo[] _nonSerializedProperties;

    /**
     * Default constructor for Externalizable.
     */
    public TypeDesc() {
    }

    public TypeDesc(String typeName, String codeBase, String[] superTypesNames,
                    PropertyInfo[] properties, boolean supportsDynamicProperties, Map<String, SpaceIndex> indexes,
                    String idPropertyName, boolean idAutoGenerate, String defaultPropertyName, String routingPropertyName,
                    String fifoGroupingName, Set<String> fifoGroupingIndexes,
                    boolean systemType, FifoSupport fifoMode, boolean replicable, boolean supportsOptimisticLocking,
                    StorageType storageType, EntryType entryType, Class<? extends Object> objectClass,
                    Class<? extends ExternalEntry> externalEntryClass, Class<? extends SpaceDocument> documentWrapperClass,
                    String dotnetDocumentWrapperType, byte dotnetStorageType, boolean blobstoreEnabled, String sequenceNumberPropertyName,
                    TypeQueryExtensions queryExtensionsInfo, Class<? extends ClassBinaryStorageAdapter> binaryStorageAdapter,boolean broadcast) {
        _typeName = typeName;
        _codeBase = codeBase;
        _superTypesNames = superTypesNames;
        _fixedProperties = properties;
        _supportsDynamicProperties = supportsDynamicProperties;
        _indexes = indexes;
        _idPropertyName = idPropertyName;
        _autoGenerateId = idAutoGenerate;
        _documentWrapperClassName = documentWrapperClass == null ? null : documentWrapperClass.getName();
        _dotnetDocumentWrapperTypeName = dotnetDocumentWrapperType;
        _dotnetDynamicPropertiesStorageType = dotnetStorageType;
        _defaultPropertyName = calcDefaultPropertyName(defaultPropertyName, _idPropertyName, properties, indexes);
        _routingPropertyName = routingPropertyName != null ? routingPropertyName : _defaultPropertyName;
        _fifoGroupingName = fifoGroupingName;
        _fifoGroupingIndexes = fifoGroupingIndexes != null ? fifoGroupingIndexes : new HashSet<String>();
        _systemType = systemType;
        _fifoSupport = fifoMode;
        _replicable = replicable;
        _supportsOptimisticLocking = supportsOptimisticLocking;
        _storageType = storageType;
        _objectType = entryType;
        _objectClass = objectClass;
        _objectIntrospector = initObjectIntrospector();
        _externalEntryWrapperClass = externalEntryClass;
        _blobstoreEnabled = blobstoreEnabled;
        this.queryExtensionsInfo = queryExtensionsInfo;

        if (_documentWrapperClassName == null) {
            if (_dotnetDocumentWrapperTypeName == null)
                _dotnetDocumentWrapperTypeName = SpaceDocument.class.getName();
            _documentWrapperClassName = _dotnetDocumentWrapperTypeName;
        } else {
            if (_dotnetDocumentWrapperTypeName == null)
                _dotnetDocumentWrapperTypeName = _documentWrapperClassName;
        }


        if(binaryStorageAdapter != null) {
            this.classBinaryStorageAdapter = ClassBinaryStorageAdapterRegistry.getInstance().getOrCreate(binaryStorageAdapter);
        }
        initHybridProperties();
        validate();
        validateAndUpdateSequenceNumberInfo(sequenceNumberPropertyName);
        initializeV9_0_0();
        addFifoGroupingIndexesIfNeeded(_indexes, _fifoGroupingName, _fifoGroupingIndexes);
        _broadcast = broadcast;
    }

    private void initHybridProperties() {
        int serializedFieldsCount = (int) Arrays.stream(_fixedProperties).filter(propertyInfo -> propertyInfo.isBinarySpaceProperty(this)).count();
        _nonSerializedProperties = new PropertyInfo[_fixedProperties.length - serializedFieldsCount];
        _serializedProperties = new PropertyInfo[serializedFieldsCount];
        int nonSerializedFieldsIndex = 0;
        int serializedFieldsIndex = 0;
        for (int i = 0; i < _fixedProperties.length; i++) {
            if(_fixedProperties[i].getStorageType() != null && _fixedProperties[i].isBinarySpaceProperty(this)){
                _serializedProperties[serializedFieldsIndex] = _fixedProperties[i];
                _serializedProperties[serializedFieldsIndex].setHybridIndex(serializedFieldsIndex);
                _serializedProperties[serializedFieldsIndex].setOriginalIndex(i);
                serializedFieldsIndex++;
            } else {
                _nonSerializedProperties[nonSerializedFieldsIndex] = _fixedProperties[i];
                _nonSerializedProperties[nonSerializedFieldsIndex].setHybridIndex(nonSerializedFieldsIndex);
                _nonSerializedProperties[nonSerializedFieldsIndex].setOriginalIndex(i);
                nonSerializedFieldsIndex++;
            }
        }

        positionsForScanning = new int[_fixedProperties.length];
        int positionsIndex = 0;
        for (PropertyInfo nonSerializedProperty : _nonSerializedProperties) {
            positionsForScanning[positionsIndex] = nonSerializedProperty.getOriginalIndex();
            positionsIndex++;

        }
        for (PropertyInfo serializedProperty : _serializedProperties) {
            positionsForScanning[positionsIndex] = serializedProperty.getOriginalIndex();
            positionsIndex++;
        }
    }

    public TypeDesc cloneWithoutObjectClass( TypeDesc typeDesc, EntryType entryType ) {

        TypeDesc newTypeDesc = new TypeDesc();
        newTypeDesc._objectType = entryType;

        newTypeDesc._typeName = typeDesc.getTypeName();
        newTypeDesc._codeBase = typeDesc.getCodeBase();
        newTypeDesc._superTypesNames = typeDesc.getSuperClassesNames();
        newTypeDesc._fixedProperties = typeDesc.getProperties();
        newTypeDesc._supportsDynamicProperties = typeDesc.supportsDynamicProperties();
        newTypeDesc._indexes = typeDesc.getIndexes();
        newTypeDesc._idPropertyName = typeDesc.getIdPropertyName();
        newTypeDesc._autoGenerateId = typeDesc.isAutoGenerateId();
        newTypeDesc._documentWrapperClassName = typeDesc.getDocumentWrapperClassName();
        newTypeDesc._dotnetDocumentWrapperTypeName = typeDesc.getDotnetDocumentWrapperTypeName();
        newTypeDesc._dotnetDynamicPropertiesStorageType = typeDesc.getDotnetDynamicPropertiesStorageType();
        newTypeDesc._defaultPropertyName = typeDesc.getDefaultPropertyName();
        newTypeDesc._routingPropertyName = typeDesc.getRoutingPropertyName();
        newTypeDesc._fifoGroupingName = typeDesc.getFifoGroupingPropertyPath();
        newTypeDesc._fifoGroupingIndexes = typeDesc.getFifoGroupingIndexesPaths();

        newTypeDesc._systemType = typeDesc.isSystemType();
        newTypeDesc._fifoSupport = typeDesc.getFifoSupport();
        newTypeDesc._replicable = typeDesc.isReplicable();
        newTypeDesc._supportsOptimisticLocking = typeDesc.supportsOptimisticLocking();
        newTypeDesc._storageType = typeDesc.getStorageType();

        newTypeDesc._externalEntryWrapperClass = typeDesc.getExternalEntryWrapperClass();
        newTypeDesc._blobstoreEnabled = typeDesc.isBlobstoreEnabled();
        newTypeDesc.queryExtensionsInfo = typeDesc.getQueryExtensions();
        newTypeDesc._broadcast = typeDesc.isBroadcast();
        newTypeDesc.classBinaryStorageAdapter = typeDesc.getClassBinaryStorageAdapter();

        newTypeDesc.initializeV9_0_0();

        return newTypeDesc;
    }

    private void validate() {
        if (_fifoGroupingName != null && !StringUtils.hasText(_fifoGroupingName))
            throw new IllegalArgumentException("When fifo grouping property is set, it must not be an empty path");
        // validate - if there are any fifoGrouping indexes, then there is a fifoGrouping property
        if (!_fifoGroupingIndexes.isEmpty() && _fifoGroupingName == null)
            throw new IllegalStateException("Cannot declare fifo grouping index without a fifo grouping property");
        for (PropertyInfo property : _fixedProperties) {
            String propertyName = property.getName();

            // validate SpaceId, SpaceRouting and SpcaeFifoGrouping (property and indexes) with OBJECT storage type
            if (propertyName.equals(_idPropertyName))
                assertSupportsMatching(property, "SpaceId");
            if (propertyName.equals(_routingPropertyName))
                assertSupportsMatching(property, "SpaceRouting");
            if (_fifoGroupingName != null && isSameProperty(_fifoGroupingName, propertyName))
                assertSupportsMatching(property, "SpaceFifoGroupingProperty");
            for (String fifoGroupingIndexPath : _fifoGroupingIndexes)
                if (isSameProperty(fifoGroupingIndexPath, propertyName))
                    assertSupportsMatching(property, "SpaceFifoGroupingIndex");
            if (classBinaryStorageAdapter == null) {
                // validate primitives with storage type
                if (ReflectionUtils.isSpacePrimitive(property.getType().getName()))
                    assertObjectStorageType(property, "Primitive property type " + property.getType().getName());
            }
            // validate indexes with storage type
            for (String indexName : _indexes.keySet()) {
                SpaceIndexType indexType = _indexes.get(indexName).getIndexType();
                if (indexType != null && indexType != SpaceIndexType.NONE && isSameProperty(indexName, propertyName)) {
                    assertSupportsMatching(property, "Space index with type = " + indexType);
                    if (indexType.isOrdered()) {
                        assertSupportsOrder(property, "Space index with type = " + indexType);
                    }
                }
            }
        }
    }

    private void assertSupportsMatching(PropertyInfo property, String errMsg) {
        if (classBinaryStorageAdapter == null) {
            assertObjectStorageType(property, errMsg);
        }
        if (!property.supportsEqualsMatching())
            throw new SpaceMetadataValidationException(_typeName, property, errMsg + " cannot be used with storage adapter which does not support matching: " + property.getStorageAdapterName());
    }

    private void assertSupportsOrder(PropertyInfo property, String errMsg) {
        if (!property.supportsOrderedMatching())
            throw new SpaceMetadataValidationException(_typeName, property, errMsg + " cannot be used with storage adapter which does not support order: " + property.getStorageAdapterName());
    }

    private void assertObjectStorageType(PropertyInfo property, String errMsg) {
        if (property.getStorageType() != StorageType.OBJECT)
            throw new SpaceMetadataValidationException(_typeName, property, errMsg + " cannot be used with storage type " + property.getStorageType());
    }

    private boolean isSameProperty(String indexName, String propertyName) {
        return (indexName.equals(propertyName) || indexName.startsWith(propertyName + ".") || indexName.startsWith(propertyName + SpaceCollectionIndex.COLLECTION_INDICATOR));
    }

    private void validateAndUpdateSequenceNumberInfo(String sequenceNumberPropertyName) {//if sequence number specified validate & update
        _sequenceNumberFixedPropertyPos = -1;
        if (sequenceNumberPropertyName != null) {
            if (!StringUtils.hasText(sequenceNumberPropertyName))
                throw new IllegalArgumentException("When SpaceSequenceNumber property is set, it must not be empty");

            for (int pos = 0; pos < _fixedProperties.length; pos++) {

                PropertyInfo property = _fixedProperties[pos];
                String propertyName = property.getName();
                // validate SpaceId, SpaceRouting and SpcaeFifoGrouping (property and indexes) with OBJECT storage type
                if (propertyName.equals(sequenceNumberPropertyName)) {
                    if (!property.getTypeName().equals(Long.class.getName()) && !property.getTypeName().equals(long.class.getName())
                            && !property.getTypeName().equals(Object.class.getName()))
                        throw new IllegalArgumentException("SpaceSequenceNumber property must be of type Long or long");

                    _sequenceNumberFixedPropertyPos = pos;
                    return;
                }
            }
            throw new SpaceMetadataValidationException(_typeName, sequenceNumberPropertyName, " invalid property name specified for SpaceSequenceNumber");
        }
    }

    private ITypeIntrospector<? extends Object> initObjectIntrospector() {
        if (_objectClass == null)
            return null;

        if (ExternalEntry.class.isAssignableFrom(_objectClass))
            return null;
        try {
            if (MetaDataEntry.class.isAssignableFrom(_objectClass))
                return new MetadataEntryIntrospector<MetaDataEntry>(this);
            if (Entry.class.isAssignableFrom(_objectClass))
                return new EntryIntrospector<Entry>(this);
        } catch (NoSuchMethodException e) {
            throw new SpaceMetadataException("Failed to create introspector for type '" + _objectClass.getName() + "'", e);
        }

        return new PojoIntrospector<Object>(this);
    }

    private static int indexOfProperty(PropertyInfo[] properties, String propertyName) {
        if (propertyName == null)
            return NO_SUCH_PROPERTY;

        for (int i = 0; i < properties.length; i++)
            if (properties[i].getName().equals(propertyName))
                return i;

        return NO_SUCH_PROPERTY;
    }

    @Override
    public ITypeDesc clone() {
        try {
            TypeDesc copy = (TypeDesc) super.clone();
            copy._indexes = new HashMap<String, SpaceIndex>(this._indexes);
            copy.buildCompoundIndexesList();
            return copy;
        } catch (CloneNotSupportedException e) {
            // this shouldn't happen, since we are Cloneable
            throw new InternalError();
        }
    }

    public boolean isInactive() {
        return false;
    }

    public String getTypeName() {
        return _typeName;
    }

    @Override
    public String getTypeUidPrefix() {
        return _typeUidPrefix;
    }

    @Override
    public String getTypeSimpleName() {
        return _typeSimpleName;
    }

    public Class<? extends Object> getObjectClass() {
        return _objectClass;
    }

    public Class<? extends SpaceDocument> getDocumentWrapperClass() {
        return _documentWrapperClass;
    }

    public String getCodeBase() {
        return _codeBase;
    }

    public boolean isExternalizable() {
        return _isExternalizable;
    }

    public String[] getSuperClassesNames() {
        return _superTypesNames;
    }

    @Override
    public String getSuperTypeName() {
        if (_superTypesNames == null || _superTypesNames.length < 2)
            return null;
        return _superTypesNames[1];
    }

    public String[] getRestrictSuperClassesNames() {
        return _restrictedSuperClasses;
    }

    public PropertyInfo[] getProperties() {
        return _fixedProperties;
    }

    public int getNumOfFixedProperties() {
        return _fixedProperties.length;
    }

    public PropertyInfo getFixedProperty(int propertyID) {
        return _fixedProperties[propertyID];
    }

    public int getFixedPropertyPosition(String propertyName) {
        if (propertyName == null)
            return NO_SUCH_PROPERTY;

        Integer position = _fixedPropertiesMap.get(propertyName);
        return position != null ? position.intValue() : NO_SUCH_PROPERTY;
    }

    public PropertyInfo getFixedProperty(String propertyName) {
        int propertyID = getFixedPropertyPosition(propertyName);
        return (propertyID != NO_SUCH_PROPERTY ? _fixedProperties[propertyID] : null);
    }

    public boolean supportsDynamicProperties() {
        return _supportsDynamicProperties;
    }

    public boolean supportsOptimisticLocking() {
        return _supportsOptimisticLocking;
    }

    public int getNumOfIndexedProperties() {
        return _numOfIndexedProperties;
    }

    public int getIndexedPropertyID(int propertyID) {
        return _indexedPropertiesIDs[propertyID];
    }

    public int getIdentifierPropertyId() {
        return _idPropertyPos;
    }

    public String getIdPropertyName() {
        return _idPropertyName;
    }

    @Override
    public SpaceIdType getSpaceIdType() {
        if (_idPropertyName == null || _idPropertyName.length() == 0)
            return SpaceIdType.NONE;

        return _autoGenerateId ? SpaceIdType.AUTOMATIC : SpaceIdType.MANUAL;
    }

    @Override
    public boolean isAutoGenerateId() {
        return _autoGenerateId;
    }

    public boolean isAutoGenerateRouting() {
        return _autoGenerateRouting;
    }

    public int getRoutingPropertyId() {
        return _routingPropertyPos;
    }

    public String getRoutingPropertyName() {
        return _routingPropertyName;
    }

    public String getDefaultPropertyName() {
        return _defaultPropertyName;
    }

    public boolean isFifoSupported() {
        return _fifoSupport != FifoSupport.OFF;
    }

    public boolean isFifoDefault() {
        return _fifoSupport == FifoSupport.ALL;
    }

    public FifoSupport getFifoSupport() {
        return _fifoSupport;
    }

    public boolean isSystemType() {
        return _systemType;
    }

    public boolean isReplicable() {
        return _replicable;
    }

    @Override
    public boolean isBlobstoreEnabled() {
        return _blobstoreEnabled;
    }

    @Override
    public boolean isBroadcast() {
        return _broadcast;
    }

    public EntryType getObjectType() {
        return _objectType;
    }

    public String[] getPropertiesNames() {
        String[] names = new String[_fixedProperties.length];
        for (int i = 0; i < names.length; i++)
            names[i] = _fixedProperties[i].getName();
        return names;
    }

    public String[] getPropertiesTypes() {
        String[] types = new String[_fixedProperties.length];
        for (int i = 0; i < types.length; i++)
            types[i] = _fixedProperties[i].getTypeName();
        return types;
    }

    public boolean[] getPropertiesIndexTypes() {
        boolean[] indexTypes = new boolean[_fixedProperties.length];
        for (int i = 0; i < indexTypes.length; i++)
            indexTypes[i] = getIndexType(_fixedProperties[i].getName()).isIndexed();
        return indexTypes;
    }

    public int getChecksum() {
        return _checksum;
    }

    @Override
    public boolean isConcreteType() {
        return _objectIntrospector != null;
    }

    @Override
    public boolean supports(EntryType entryType) {
        return entryType == null || entryType.isVirtual() || this.isConcreteType();
    }

    @Override
    public EntryTypeDesc getEntryTypeDesc(EntryType entryType) {
        return _entryTypeDescs[entryType == null ? 0 : entryType.getTypeCode()];
    }

    public ITypeIntrospector getIntrospector(EntryType entryType) {
        ITypeIntrospector<?> result;

        if (entryType == null)
            entryType = _objectType;

        if (entryType.isConcrete())
            result = _objectIntrospector;
        else if (entryType == EntryType.DOCUMENT_JAVA || entryType == EntryType.CPP || entryType == EntryType.OBJECT_DOTNET || entryType == EntryType.DOCUMENT_DOTNET)
            result = _documentIntrospector;
        else if (entryType == EntryType.EXTERNAL_ENTRY)
            result = _externalEntryIntrospector;
        else
            throw new IllegalArgumentException("Unsupported entry type - " + entryType);

        if (result == null)
            throw new IllegalArgumentException("Type descriptor for type [" + getTypeName() + "] does not contain an introspector for " + entryType);
        return result;
    }

    public Map<String, SpaceIndex> getIndexes() {
        return _indexes;
    }

    @Override
    public TypeQueryExtensions getQueryExtensions() {
        return queryExtensionsInfo;
    }

    public SpaceIndexType getIndexType(String indexName) {
        SpaceIndex index = _indexes.get(indexName);
        return index != null ? index.getIndexType() : SpaceIndexType.NONE;
    }

    public byte getDotnetDynamicPropertiesStorageType() {
        return _dotnetDynamicPropertiesStorageType;
    }

    public String getDotnetDocumentWrapperTypeName() {
        return _dotnetDocumentWrapperTypeName;
    }

    @Override
    public String getPrimitivePropertiesWithoutNullValues() {
        return _primitivePropertiesWithoutNullValues;
    }

    private static int calcChecksum(String[] superClasses, PropertyInfo[] properties) {
        int superClassesChecksum = calculateChecksum(superClasses);
        int propertiesChecksum = calculateChecksum(properties);
        return superClassesChecksum ^ propertiesChecksum;
    }

    private static String[] calcRestrictSuperClasses(String[] superClasses, String className) {
        if (superClasses == null || superClasses.length < 2)
            return superClasses;

        int startIndex = 0;
        if (superClasses[0].equals(className))
            startIndex++;

        int endIndex = superClasses.length - 1;
        if (superClasses[superClasses.length - 1].equals(Object.class.getName()))
            endIndex--;

        final int size = endIndex - startIndex + 1;
        String[] result = new String[size];
        System.arraycopy(superClasses, startIndex, result, 0, size);
        return result;
    }

    private static String calcDefaultPropertyName(String explicitDefaultPropertyName,
                                                  String idPropertyName, PropertyInfo[] properties, Map<String, SpaceIndex> indexes) {
        // If default property was set explicitly, use it:
        if (explicitDefaultPropertyName != null)
            return explicitDefaultPropertyName;

        // Otherwise, if identifier property is set, return it:
        if (idPropertyName != null)
            return idPropertyName;

        // Otherwise, return first index, if any:
        if (indexes != null)
            for (int i = 0; i < properties.length; i++) {
                SpaceIndex spaceIndex = indexes.get(properties[i].getName());
                if (spaceIndex != null && spaceIndex.getIndexType().isIndexed())
                    return properties[i].getName();
            }

        // Otherwise, return first property, if any:
        return properties.length != 0 ? properties[0].getName() : null;
    }

    private void calcIndexedPropertiesIDs() {
        int length = _fixedProperties.length;

        _indexedPropertiesIDs = new int[length];
        _numOfIndexedProperties = 0;
        for (int i = 0; i < length; i++)
            _indexedPropertiesIDs[i] = (getIndexType(getFixedProperty(i).getName()).isIndexed() ? _numOfIndexedProperties++ : NO_SUCH_PROPERTY);
    }

    @Override
    public boolean hasSequenceNumber() {
        return _sequenceNumberFixedPropertyPos >= 0;
    }

    @Override
    public int getSequenceNumberFixedPropertyID() {
        return _sequenceNumberFixedPropertyPos;
    }

    private static int calculateChecksum(PropertyInfo[] properties) {
        if (properties == null)
            return 0;

        int result = 0;
        for (PropertyInfo property : properties) {
            result = 31 * result + hashCode(property.getName());
            result = 47 * result + hashCode(property.getTypeName());
        }

        return result;
    }

    private static int calculateChecksum(String[] array) {
        if (array == null)
            return 0;

        int result = 0;
        int offset = 0;
        int length = array.length;

        for (int i = 0; i < length; i++)
            result = 31 * result + hashCode(array[offset++]);

        return result;
    }

    private static int hashCode(String s) {
        int result = 0;
        int offset = 0;
        char chars[] = s.toCharArray();
        int length = chars.length;

        for (int i = 0; i < length; i++)
            result = 31 * result + chars[offset++];

        return result;
    }

    @Override
    public String toString() {
        if (_description == null)
            _description = generateDescription();
        return _description;
    }

    private String generateDescription() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("typeName=").append(_typeName).append(", ");
        sb.append("checksum=").append(_checksum).append(", ");
        sb.append("codebase=").append(_codeBase).append(", ");
        sb.append("superTypesNames=").append(Arrays.toString(_superTypesNames)).append(", ");
        sb.append("supportsDynamicProperties=").append(_supportsDynamicProperties).append(", ");
        sb.append("supportsOptimisticLocking=").append(_supportsOptimisticLocking).append(", ");
        sb.append("systemType=").append(_systemType).append(", ");
        sb.append("replicatable=").append(_replicable).append(", ");
        sb.append("blobstoreEnabled=").append(_blobstoreEnabled).append(", ");
        sb.append("broadcast").append(_broadcast).append(", ");
        sb.append("storageType=").append(_storageType).append(", ");
        sb.append("fifoSupport=").append(_fifoSupport).append(", ");
        sb.append("idPropertyName=").append(_idPropertyName).append(", ");
        sb.append("idAutoGenerate=").append(_autoGenerateId).append(", ");
        sb.append("routingPropertyName=").append(_routingPropertyName).append(", ");
        sb.append("fifoGroupingPropertyName=").append(_fifoGroupingName).append(", ");
        String sequenceNumberPropertyName = _sequenceNumberFixedPropertyPos != -1 ? _fixedProperties[_sequenceNumberFixedPropertyPos].getName() : null;
        sb.append("sequenceNumberPropertyName=").append(sequenceNumberPropertyName).append(", ");
        sb.append("objectClass=").append((_objectClass == null ? "" : _objectClass.getName())).append(", ");
        sb.append("documentWrapperClass=").append(_documentWrapperClassName).append(", ");
        sb.append("fixedProperties=").append(Arrays.toString(_fixedProperties)).append(", ");
        sb.append("indexes=").append(Arrays.toString(_indexes.values().toArray())).append(", ");
        sb.append("fifoGroupingIndexes=").append(_fifoGroupingIndexes);
        sb.append("]");
        return sb.toString();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        readExternal(in, LRMIInvocationContext.getEndpointLogicalVersion(), false);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException, ClassNotFoundException {
        readExternal(in, PlatformLogicalVersion.getLogicalVersion(), true);
    }

    void readExternal(ObjectInput in, PlatformLogicalVersion version, boolean swap)
            throws IOException, ClassNotFoundException {
        _sequenceNumberFixedPropertyPos = -1;

        if (version.greaterOrEquals(PlatformLogicalVersion.v11_0_0))
            readExternalV11_0_0(in, version, swap);
        else if (version.greaterOrEquals(PlatformLogicalVersion.v10_1_0))
            readExternalV10_1(in, version, swap);
        else if (version.greaterOrEquals(PlatformLogicalVersion.v10_0_0))
            readExternalV10_0(in, version);
        else
            readExternalV9_0_2(in, version);
    }

    private void readExternalV10_1(ObjectInput in, PlatformLogicalVersion version, boolean swap) throws IOException, ClassNotFoundException {
        _typeName = IOUtils.readString(in);
        _codeBase = IOUtils.readString(in);
        _superTypesNames = IOUtils.readStringArray(in);
        int numOfProperties = in.readInt();
        if (numOfProperties >= 0) {
            _fixedProperties = new PropertyInfo[numOfProperties];
            for (int i = 0; i < numOfProperties; i++)
                _fixedProperties[i] = PropertyInfo.deserialize(in, version);
        }
        _idPropertyName = IOUtils.readString(in);
        _autoGenerateId = in.readBoolean();
        _defaultPropertyName = IOUtils.readString(in);
        _routingPropertyName = IOUtils.readString(in);
        // New in 9.0.0 : fifo grouping property
        _fifoGroupingName = IOUtils.readString(in);
        // New in 9.0.0 : fifo grouping indexes
        final int numOfFifoGroupingIndexes = in.readInt();
        if (numOfFifoGroupingIndexes >= 0) {
            _fifoGroupingIndexes = new HashSet<String>();
            for (int i = 0; i < numOfFifoGroupingIndexes; i++) {
                String fifoGroupingIndex = IOUtils.readString(in);
                _fifoGroupingIndexes.add(fifoGroupingIndex);
            }
        }

        // New in 10.1: sequence number property
        _sequenceNumberFixedPropertyPos = in.readInt();
        _fifoSupport = FifoHelper.fromCode(in.readByte());
        _systemType = in.readBoolean();
        _replicable = in.readBoolean();
        _blobstoreEnabled = in.readBoolean(); //new 10.0.0
        _supportsDynamicProperties = in.readBoolean();
        // Changed in 8.0.4: read dot net dynamic properties storage type as code instead of object
        _dotnetDynamicPropertiesStorageType = in.readByte();
        _supportsOptimisticLocking = in.readBoolean();
        _objectType = EntryType.fromByte(in.readByte());
        _storageType = StorageType.fromCode(in.readInt());
        // Changed in 10.1: Object introspector serialization
        _objectIntrospector = readIntrospector(in, version);
        _documentWrapperClassName = IOUtils.readString(in);
        //New 8.0.1
        _dotnetDocumentWrapperTypeName = IOUtils.readString(in);

        final int numOfIndexes = in.readInt();
        if (numOfIndexes >= 0) {
            _indexes = new HashMap<String, SpaceIndex>(numOfIndexes);
            for (int i = 0; i < numOfIndexes; i++) {
                ISpaceIndex index;
                if (swap)
                    index = IOUtils.readNullableSwapExternalizableObject(in);
                else
                    index = IOUtils.readObject(in);

                _indexes.put(index.getName(), index);
            }
        }

        initializeV9_0_0();
    }

    private void readExternalV11_0_0(ObjectInput in, PlatformLogicalVersion version, boolean swap) throws IOException, ClassNotFoundException {
        _typeName = IOUtils.readString(in);
        _codeBase = IOUtils.readString(in);
        _superTypesNames = IOUtils.readStringArray(in);
        int numOfProperties = in.readInt();
        if (numOfProperties >= 0) {
            _fixedProperties = new PropertyInfo[numOfProperties];
            for (int i = 0; i < numOfProperties; i++)
                _fixedProperties[i] = PropertyInfo.deserialize(in, version);
        }
        _idPropertyName = IOUtils.readString(in);
        _autoGenerateId = in.readBoolean();
        _defaultPropertyName = IOUtils.readString(in);
        _routingPropertyName = IOUtils.readString(in);
        // New in 9.0.0 : fifo grouping property
        _fifoGroupingName = IOUtils.readString(in);
        // New in 9.0.0 : fifo grouping indexes
        final int numOfFifoGroupingIndexes = in.readInt();
        if (numOfFifoGroupingIndexes >= 0) {
            _fifoGroupingIndexes = new HashSet<String>();
            for (int i = 0; i < numOfFifoGroupingIndexes; i++) {
                String fifoGroupingIndex = IOUtils.readString(in);
                _fifoGroupingIndexes.add(fifoGroupingIndex);
            }
        }

        // New in 10.1: sequence number property
        _sequenceNumberFixedPropertyPos = in.readInt();
        _fifoSupport = FifoHelper.fromCode(in.readByte());
        _systemType = in.readBoolean();
        _replicable = in.readBoolean();
        _blobstoreEnabled = in.readBoolean(); //new 10.0.0
        _supportsDynamicProperties = in.readBoolean();
        // Changed in 8.0.4: read dot net dynamic properties storage type as code instead of object
        _dotnetDynamicPropertiesStorageType = in.readByte();
        _supportsOptimisticLocking = in.readBoolean();
        _objectType = EntryType.fromByte(in.readByte());
        _storageType = StorageType.fromCode(in.readInt());
        // Changed in 10.1: Object introspector serialization
        _objectIntrospector = readIntrospector(in, version);
        _documentWrapperClassName = IOUtils.readString(in);
        //New 8.0.1
        _dotnetDocumentWrapperTypeName = IOUtils.readString(in);

        final int numOfIndexes = in.readInt();
        if (numOfIndexes >= 0) {
            _indexes = new HashMap<String, SpaceIndex>(numOfIndexes);
            for (int i = 0; i < numOfIndexes; i++) {
                ISpaceIndex index;
                if (swap)
                    index = IOUtils.readNullableSwapExternalizableObject(in);
                else
                    index = IOUtils.readObject(in);

                _indexes.put(index.getName(), index);
            }
        }

        readObjectsFromByteArray(in);


        // New in 15.8.0: Space class binary storage adapter
        if (version.greaterOrEquals(PlatformLogicalVersion.v15_8_0)) {
            _broadcast = in.readBoolean();
            String storageAdapterClassName = IOUtils.readString(in);
            if (storageAdapterClassName != null) {
                classBinaryStorageAdapter = ClassBinaryStorageAdapterRegistry.getInstance().getOrCreate(ClassLoaderHelper.loadClass(storageAdapterClassName));
            }
        } else {
            _broadcast = false;
        }

        initializeV9_0_0();
        initHybridProperties();
    }

    private void writeObjectsAsByteArray(ObjectOutput out) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream ba = new ObjectOutputStream(byteArrayOutputStream);
        ba.writeObject(queryExtensionsInfo);
        IOUtils.writeObject(out, byteArrayOutputStream.toByteArray());
    }

    private void readObjectsFromByteArray(ObjectInput in) throws IOException, ClassNotFoundException {
        byte[] byteArray = IOUtils.readObject(in);
        CustomClassLoaderObjectInputStream objectInputStream = new CustomClassLoaderObjectInputStream(
                new ByteArrayInputStream(byteArray), Thread.currentThread().getContextClassLoader());
        //noinspection unchecked
        queryExtensionsInfo = (TypeQueryExtensions) objectInputStream.readObject();
    }

    private void readExternalV10_0(ObjectInput in, PlatformLogicalVersion version) throws IOException, ClassNotFoundException {
        _typeName = IOUtils.readString(in);
        _codeBase = IOUtils.readString(in);
        _superTypesNames = IOUtils.readStringArray(in);
        int numOfProperties = in.readInt();
        if (numOfProperties >= 0) {
            _fixedProperties = new PropertyInfo[numOfProperties];
            for (int i = 0; i < numOfProperties; i++)
                _fixedProperties[i] = PropertyInfo.deserialize(in, version);
        }
        _idPropertyName = IOUtils.readString(in);
        _autoGenerateId = in.readBoolean();
        _defaultPropertyName = IOUtils.readString(in);
        _routingPropertyName = IOUtils.readString(in);
        // New in 9.0.0 : fifo grouping property
        _fifoGroupingName = IOUtils.readString(in);
        // New in 9.0.0 : fifo grouping indexes
        final int numOfFifoGroupingIndexes = in.readInt();
        if (numOfFifoGroupingIndexes >= 0) {
            _fifoGroupingIndexes = new HashSet<String>();
            for (int i = 0; i < numOfFifoGroupingIndexes; i++) {
                String fifoGroupingIndex = IOUtils.readString(in);
                _fifoGroupingIndexes.add(fifoGroupingIndex);
            }
        }

        _fifoSupport = FifoHelper.fromCode(in.readByte());
        _systemType = in.readBoolean();
        _replicable = in.readBoolean();
        _blobstoreEnabled = in.readBoolean(); //new 10.0.0
        _supportsDynamicProperties = in.readBoolean();
        // Changed in 8.0.4: read dot net dynamic properties storage type as code instead of object
        _dotnetDynamicPropertiesStorageType = in.readByte();
        _supportsOptimisticLocking = in.readBoolean();
        _objectType = EntryType.fromByte(in.readByte());
        _storageType = StorageType.fromCode(in.readInt());
        _objectIntrospector = IOUtils.readObject(in);
        _documentWrapperClassName = IOUtils.readString(in);
        //New 8.0.1
        _dotnetDocumentWrapperTypeName = IOUtils.readString(in);

        final int numOfIndexes = in.readInt();
        if (numOfIndexes >= 0) {
            _indexes = new HashMap<String, SpaceIndex>(numOfIndexes);
            for (int i = 0; i < numOfIndexes; i++) {
                ISpaceIndex index = IOUtils.readObject(in);
                _indexes.put(index.getName(), index);
            }
        }

        initializeV9_0_0();
    }

    private void readExternalV9_0_2(ObjectInput in, PlatformLogicalVersion version) throws IOException, ClassNotFoundException {
        _typeName = IOUtils.readString(in);
        _codeBase = IOUtils.readString(in);
        _superTypesNames = IOUtils.readStringArray(in);
        int numOfProperties = in.readInt();
        if (numOfProperties >= 0) {
            _fixedProperties = new PropertyInfo[numOfProperties];
            for (int i = 0; i < numOfProperties; i++)
                _fixedProperties[i] = PropertyInfo.deserialize(in, version);
        }
        _idPropertyName = IOUtils.readString(in);
        _autoGenerateId = in.readBoolean();
        _defaultPropertyName = IOUtils.readString(in);
        _routingPropertyName = IOUtils.readString(in);
        // New in 9.0.0 : fifo grouping property
        _fifoGroupingName = IOUtils.readString(in);
        // New in 9.0.0 : fifo grouping indexes
        final int numOfFifoGroupingIndexes = in.readInt();
        if (numOfFifoGroupingIndexes >= 0) {
            _fifoGroupingIndexes = new HashSet<String>();
            for (int i = 0; i < numOfFifoGroupingIndexes; i++) {
                String fifoGroupingIndex = IOUtils.readString(in);
                _fifoGroupingIndexes.add(fifoGroupingIndex);
            }
        }

        _fifoSupport = FifoHelper.fromCode(in.readByte());
        _systemType = in.readBoolean();
        _replicable = in.readBoolean();
        _supportsDynamicProperties = in.readBoolean();
        // Changed in 8.0.4: read dot net dynamic properties storage type as code instead of object
        _dotnetDynamicPropertiesStorageType = in.readByte();
        _supportsOptimisticLocking = in.readBoolean();
        _objectType = EntryType.fromByte(in.readByte());
        _storageType = StorageType.fromCode(in.readInt());
        _objectIntrospector = IOUtils.readObject(in);
        _documentWrapperClassName = IOUtils.readString(in);
        //New 8.0.1
        _dotnetDocumentWrapperTypeName = IOUtils.readString(in);

        final int numOfIndexes = in.readInt();
        if (numOfIndexes >= 0) {
            _indexes = new HashMap<String, SpaceIndex>(numOfIndexes);
            for (int i = 0; i < numOfIndexes; i++) {
                ISpaceIndex index = IOUtils.readObject(in);
                _indexes.put(index.getName(), index);
            }
        }

        initializeV9_0_0();
    }

    private void initializeV9_0_0() {
        _typeSimpleName = StringUtils.getSuffix(_typeName, ".");
        _typeUidPrefix = SpaceUidFactory.generateTypePrefix(_typeName);
        _idPropertyPos = indexOfProperty(_fixedProperties, _idPropertyName);
        // map properties names to positions:
        _fixedPropertiesMap = new HashMap<String, Integer>();
        for (int i = 0; i < _fixedProperties.length; i++)
            _fixedPropertiesMap.put(_fixedProperties[i].getName(), i);

        _defaultPropertyPos = indexOfProperty(_fixedProperties, _defaultPropertyName);
        _routingPropertyPos = indexOfProperty(_fixedProperties, _routingPropertyName);
        _restrictedSuperClasses = calcRestrictSuperClasses(_superTypesNames, _typeName);
        _checksum = calcChecksum(_superTypesNames, _fixedProperties);

        calcIndexedPropertiesIDs();

        if (_objectIntrospector != null) {
            _objectIntrospector.initialize(this);
            _isExternalizable = ENABLE_EXTERNALIZABLE &&
                    Externalizable.class.isAssignableFrom(_objectIntrospector.getType()) &&
                    !_objectIntrospector.hasConstructorProperties();
        }

        _externalEntryIntrospector = new ExternalEntryIntrospector(this, _externalEntryWrapperClass);

        // load the class locally - document wrapper class is a proxy level feature and not propagated to the space
        _documentWrapperClass = ClassLoaderHelper.loadClass(_documentWrapperClassName, true, SpaceDocument.class);
        _documentIntrospector = new VirtualEntryIntrospector(this, _documentWrapperClass);
        _autoGenerateRouting = _autoGenerateId && _idPropertyName.equals(_routingPropertyName);

        this._isAllPropertiesObjectStorageType = initializeAllPropertiesObjectStorageType();
        this._entryTypeDescs = initEntryTypeDescs();
        buildCompoundIndexesList();
        this._primitivePropertiesWithoutNullValues = findPrimitivePropertiesWithoutNullValues();
    }

    private String findPrimitivePropertiesWithoutNullValues() {
        if (_objectIntrospector == null)
            return null;

        String result = null;
        for (int i = 0; i < _fixedProperties.length; i++)
            if (_fixedProperties[i].isPrimitive() && _fixedProperties[i].getType() != boolean.class &&
                    !_objectIntrospector.propertyHasNullValue(i)) {
                if (result == null)
                    result = _fixedProperties[i].getName() + " (" + _fixedProperties[i].getTypeName() + ")";
                else
                    result = result + ", " + _fixedProperties[i].getName() + " (" + _fixedProperties[i].getTypeName()
                            + ")";
            }
        return result;
    }

    private boolean initializeAllPropertiesObjectStorageType() {
        // initialize _isAllPropertiesObjectStorageType
        for (PropertyInfo property : _fixedProperties) {
            if (property.getStorageAdapter() != null)
                return false;
        }
        return true;
    }

    private EntryTypeDesc[] initEntryTypeDescs() {
        EntryTypeDesc[] result = new EntryTypeDesc[EntryType.MAX + 1];
        result[EntryType.OBJECT_JAVA.getTypeCode()] = new EntryTypeDesc(EntryType.OBJECT_JAVA, this);
        result[EntryType.EXTERNAL_ENTRY.getTypeCode()] = new EntryTypeDesc(EntryType.EXTERNAL_ENTRY, this);
        result[EntryType.DOCUMENT_JAVA.getTypeCode()] = new EntryTypeDesc(EntryType.DOCUMENT_JAVA, this);
        result[EntryType.CPP.getTypeCode()] = new EntryTypeDesc(EntryType.CPP, this);
        result[EntryType.OBJECT_DOTNET.getTypeCode()] = new EntryTypeDesc(EntryType.OBJECT_DOTNET, this);
        result[EntryType.DOCUMENT_DOTNET.getTypeCode()] = new EntryTypeDesc(EntryType.DOCUMENT_DOTNET, this);
        // Init default last (since its a reference to a previously constructed EntryTypeDesc)
        result[0] = result[_objectType.getTypeCode()];

        return result;
    }

    @Override
    public boolean isAllPropertiesObjectStorageType() {
        return this._isAllPropertiesObjectStorageType;
    }

    private void addFifoGroupingIndexesIfNeeded(Map<String, SpaceIndex> indexes, String fifoGroupingName, Set<String> fifoGroupingIndexNames) {
        if (fifoGroupingName == null)
            return;
        if (indexes.get(fifoGroupingName) == null)
            indexes.put(fifoGroupingName, SpaceIndexFactory.createPropertyIndex(fifoGroupingName, SpaceIndexType.EQUAL));
        for (String fifoGroupingIndexName : fifoGroupingIndexNames)
            if (indexes.get(fifoGroupingIndexName) == null)
                indexes.put(fifoGroupingIndexName, SpaceIndexFactory.createPropertyIndex(fifoGroupingIndexName, SpaceIndexType.EQUAL));
    }

    @Override
    public StorageType getStorageType() {
        return _storageType;
    }

    @Override
    public String getFifoGroupingPropertyPath() {
        return _fifoGroupingName;
    }

    @Override
    public Set<String> getFifoGroupingIndexesPaths() {
        return _fifoGroupingIndexes;
    }

    private void buildCompoundIndexesList() {
        if (_indexes != null && !_indexes.isEmpty()) {
            for (Map.Entry<String, SpaceIndex> entry : _indexes.entrySet()) {
                if (((ISpaceIndex) entry.getValue()).isCompoundIndex()) {
                    if (_compoundIndexes == null)
                        _compoundIndexes = new ArrayList<SpaceIndex>();
                    _compoundIndexes.add(entry.getValue());

                }
            }
        }


    }

    public List<SpaceIndex> getCompoundIndexes() {
        return _compoundIndexes;
    }

    public boolean anyCompoundIndex() {
        return _compoundIndexes != null && _compoundIndexes.size() > 0;
    }


    @Override
    public ClassBinaryStorageAdapter getClassBinaryStorageAdapter() {
        return classBinaryStorageAdapter;
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        writeExternal(out, LRMIInvocationContext.getEndpointLogicalVersion(), false);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        writeExternal(out, PlatformLogicalVersion.getLogicalVersion(), true);
    }

    void writeExternal(ObjectOutput out, PlatformLogicalVersion version, boolean swap) throws IOException {
        if (version.greaterOrEquals(PlatformLogicalVersion.v11_0_0))
            writeExternalV11_0_0(out, version, swap);
        else if (version.greaterOrEquals(PlatformLogicalVersion.v10_1_0))
            writeExternalV10_1(out, version, swap);
        else if (version.greaterOrEquals(PlatformLogicalVersion.v10_0_0))
            writeExternalV10_0(out, version);
        else
            writeExternalV9_0_2(out, version);
    }

    private void writeExternalV11_0_0(ObjectOutput out, PlatformLogicalVersion version, boolean swap) throws IOException {
        IOUtils.writeString(out, _typeName);
        IOUtils.writeString(out, _codeBase);
        IOUtils.writeStringArray(out, _superTypesNames);
        int numOfProperties = _fixedProperties == null ? -1 : _fixedProperties.length;
        out.writeInt(numOfProperties);
        for (int i = 0; i < numOfProperties; i++) {
            _fixedProperties[i].serialize(out, version);
        }
        IOUtils.writeString(out, _idPropertyName);
        out.writeBoolean(_autoGenerateId);
        IOUtils.writeString(out, _defaultPropertyName);
        IOUtils.writeString(out, _routingPropertyName);
        // New in 9.0.0 : fifo grouping property
        IOUtils.writeString(out, _fifoGroupingName);
        // New in 9.0.0 : fifo grouping indexes
        final int numOfFifoGroupingIndexes = _fifoGroupingIndexes == null ? -1 : _fifoGroupingIndexes.size();
        out.writeInt(numOfFifoGroupingIndexes);
        if (numOfFifoGroupingIndexes > 0) {
            for (String fifoGroupingIndexName : _fifoGroupingIndexes)
                IOUtils.writeString(out, fifoGroupingIndexName);
        }
        // New in 10.1: sequence number property
        out.writeInt(_sequenceNumberFixedPropertyPos);
        out.writeByte(FifoHelper.toCode(_fifoSupport));
        out.writeBoolean(_systemType);
        out.writeBoolean(_replicable);
        out.writeBoolean(_blobstoreEnabled);
        out.writeBoolean(_supportsDynamicProperties);
        // Changed in 8.0.4: write dot net dynamic properties storage type as code instead of object
        out.writeByte(_dotnetDynamicPropertiesStorageType);
        out.writeBoolean(_supportsOptimisticLocking);
        out.writeByte(_objectType.getTypeCode());
        out.writeInt(_storageType.getCode());
        // Changed in 10.1: object introspector serialization
        writeIntrospector(out, version, _objectIntrospector);
        IOUtils.writeString(out, _documentWrapperClassName);
        // New in 8.0.1: write dotnet document wrapper type name.
        IOUtils.writeString(out, _dotnetDocumentWrapperTypeName);
        final int numOfIndexes = _indexes == null ? -1 : _indexes.size();
        out.writeInt(numOfIndexes);
        if (numOfIndexes > 0) {
            for (Map.Entry<String, SpaceIndex> entry : _indexes.entrySet()) {
                if (swap)
                    IOUtils.writeNullableSwapExternalizableObject(out,
                            (ISpaceIndex) entry.getValue());
                else
                    IOUtils.writeObject(out, entry.getValue());
            }
        }

        writeObjectsAsByteArray(out);
        // New in 15.8.0: Space class storage adapter
        if (version.greaterOrEquals(PlatformLogicalVersion.v15_8_0)) {
            out.writeBoolean(_broadcast);
            if(classBinaryStorageAdapter != null){
                IOUtils.writeString(out, classBinaryStorageAdapter.getClass().getName());
            }else {
                IOUtils.writeString(out, null);
            }
        }
    }

    private void writeExternalV10_1(ObjectOutput out, PlatformLogicalVersion version, boolean swap) throws IOException {
        IOUtils.writeString(out, _typeName);
        IOUtils.writeString(out, _codeBase);
        IOUtils.writeStringArray(out, _superTypesNames);
        int numOfProperties = _fixedProperties == null ? -1 : _fixedProperties.length;
        out.writeInt(numOfProperties);
        for (int i = 0; i < numOfProperties; i++) {
            _fixedProperties[i].serialize(out, version);
        }
        IOUtils.writeString(out, _idPropertyName);
        out.writeBoolean(_autoGenerateId);
        IOUtils.writeString(out, _defaultPropertyName);
        IOUtils.writeString(out, _routingPropertyName);
        // New in 9.0.0 : fifo grouping property
        IOUtils.writeString(out, _fifoGroupingName);
        // New in 9.0.0 : fifo grouping indexes
        final int numOfFifoGroupingIndexes = _fifoGroupingIndexes == null ? -1 : _fifoGroupingIndexes.size();
        out.writeInt(numOfFifoGroupingIndexes);
        if (numOfFifoGroupingIndexes > 0) {
            for (String fifoGroupingIndexName : _fifoGroupingIndexes)
                IOUtils.writeString(out, fifoGroupingIndexName);
        }
        // New in 10.1: sequence number property
        out.writeInt(_sequenceNumberFixedPropertyPos);
        out.writeByte(FifoHelper.toCode(_fifoSupport));
        out.writeBoolean(_systemType);
        out.writeBoolean(_replicable);
        out.writeBoolean(_blobstoreEnabled);
        out.writeBoolean(_supportsDynamicProperties);
        // Changed in 8.0.4: write dot net dynamic properties storage type as code instead of object
        out.writeByte(_dotnetDynamicPropertiesStorageType);
        out.writeBoolean(_supportsOptimisticLocking);
        out.writeByte(_objectType.getTypeCode());
        out.writeInt(_storageType.getCode());
        // Changed in 10.1: object introspector serialization
        writeIntrospector(out, version, _objectIntrospector);
        IOUtils.writeString(out, _documentWrapperClassName);
        // New in 8.0.1: write dotnet document wrapper type name.
        IOUtils.writeString(out, _dotnetDocumentWrapperTypeName);

        final int numOfIndexes = _indexes == null ? -1 : _indexes.size();
        out.writeInt(numOfIndexes);
        if (numOfIndexes > 0)
            for (Map.Entry<String, SpaceIndex> entry : _indexes.entrySet()) {
                if (swap)
                    IOUtils.writeNullableSwapExternalizableObject(out,
                            (ISpaceIndex) entry.getValue());
                else
                    IOUtils.writeObject(out, entry.getValue());
            }

    }

    private void writeExternalV10_0(ObjectOutput out, PlatformLogicalVersion version) throws IOException {
        IOUtils.writeString(out, _typeName);
        IOUtils.writeString(out, _codeBase);
        IOUtils.writeStringArray(out, _superTypesNames);
        int numOfProperties = _fixedProperties == null ? -1 : _fixedProperties.length;
        out.writeInt(numOfProperties);
        for (int i = 0; i < numOfProperties; i++)
            _fixedProperties[i].serialize(out, version);
        IOUtils.writeString(out, _idPropertyName);
        out.writeBoolean(_autoGenerateId);
        IOUtils.writeString(out, _defaultPropertyName);
        IOUtils.writeString(out, _routingPropertyName);
        // New in 9.0.0 : fifo grouping property
        IOUtils.writeString(out, _fifoGroupingName);
        // New in 9.0.0 : fifo grouping indexes
        final int numOfFifoGroupingIndexes = _fifoGroupingIndexes == null ? -1 : _fifoGroupingIndexes.size();
        out.writeInt(numOfFifoGroupingIndexes);
        if (numOfFifoGroupingIndexes > 0) {
            for (String fifoGroupingIndexName : _fifoGroupingIndexes)
                IOUtils.writeString(out, fifoGroupingIndexName);
        }
        out.writeByte(FifoHelper.toCode(_fifoSupport));
        out.writeBoolean(_systemType);
        out.writeBoolean(_replicable);
        out.writeBoolean(_blobstoreEnabled);
        out.writeBoolean(_supportsDynamicProperties);
        // Changed in 8.0.4: write dot net dynamic properties storage type as code instead of object
        out.writeByte(_dotnetDynamicPropertiesStorageType);
        out.writeBoolean(_supportsOptimisticLocking);
        out.writeByte(_objectType.getTypeCode());
        out.writeInt(_storageType.getCode());
        IOUtils.writeObject(out, _objectIntrospector);
        IOUtils.writeString(out, _documentWrapperClassName);
        // New in 8.0.1: write dotnet document wrapper type name.
        IOUtils.writeString(out, _dotnetDocumentWrapperTypeName);
        final int numOfIndexes = _indexes == null ? -1 : _indexes.size();
        out.writeInt(numOfIndexes);
        if (numOfIndexes > 0)
            for (Map.Entry<String, SpaceIndex> entry : _indexes.entrySet())
                IOUtils.writeObject(out, entry.getValue());
    }

    private void writeExternalV9_0_2(ObjectOutput out, PlatformLogicalVersion version) throws IOException {
        IOUtils.writeString(out, _typeName);
        IOUtils.writeString(out, _codeBase);
        IOUtils.writeStringArray(out, _superTypesNames);
        int numOfProperties = _fixedProperties == null ? -1 : _fixedProperties.length;
        out.writeInt(numOfProperties);
        for (int i = 0; i < numOfProperties; i++)
            _fixedProperties[i].serialize(out, version);
        IOUtils.writeString(out, _idPropertyName);
        out.writeBoolean(_autoGenerateId);
        IOUtils.writeString(out, _defaultPropertyName);
        IOUtils.writeString(out, _routingPropertyName);
        // New in 9.0.0 : fifo grouping property
        IOUtils.writeString(out, _fifoGroupingName);
        // New in 9.0.0 : fifo grouping indexes
        final int numOfFifoGroupingIndexes = _fifoGroupingIndexes == null ? -1 : _fifoGroupingIndexes.size();
        out.writeInt(numOfFifoGroupingIndexes);
        if (numOfFifoGroupingIndexes > 0) {
            for (String fifoGroupingIndexName : _fifoGroupingIndexes)
                IOUtils.writeString(out, fifoGroupingIndexName);
        }
        out.writeByte(FifoHelper.toCode(_fifoSupport));
        out.writeBoolean(_systemType);
        out.writeBoolean(_replicable);
        out.writeBoolean(_supportsDynamicProperties);
        // Changed in 8.0.4: write dot net dynamic properties storage type as code instead of object
        out.writeByte(_dotnetDynamicPropertiesStorageType);
        out.writeBoolean(_supportsOptimisticLocking);
        out.writeByte(_objectType.getTypeCode());
        out.writeInt(_storageType.getCode());
        IOUtils.writeObject(out, _objectIntrospector);
        IOUtils.writeString(out, _documentWrapperClassName);
        // New in 8.0.1: write dotnet document wrapper type name.
        IOUtils.writeString(out, _dotnetDocumentWrapperTypeName);
        final int numOfIndexes = _indexes == null ? -1 : _indexes.size();
        out.writeInt(numOfIndexes);
        if (numOfIndexes > 0)
            for (Map.Entry<String, SpaceIndex> entry : _indexes.entrySet()) {
                IOUtils.writeObject(out, entry.getValue());
            }
    }

    @Override
    public Serializable getVersionedSerializable() {
        return new TypeDescVersionedSerializable(this);
    }

    private static void writeIntrospector(ObjectOutput out, PlatformLogicalVersion version, ITypeIntrospector<?> objectIntrospector) throws IOException {
        if (objectIntrospector == null) {
            out.write(0);
        } else {
            out.write(objectIntrospector.getExternalizableCode());
            objectIntrospector.writeExternal(out, version);
        }
    }

    private static ITypeIntrospector<?> readIntrospector(ObjectInput in, PlatformLogicalVersion version) throws IOException, ClassNotFoundException {
        ITypeIntrospector result = initFromCode(in.readByte());
        if (result != null)
            result.readExternal(in, version);
        return result;
    }

    private static ITypeIntrospector<?> initFromCode(byte code) {
        switch (code) {
            case 0:
                return null;
            case PojoIntrospector.EXTERNALIZABLE_CODE:
                return new PojoIntrospector();
            case EntryIntrospector.EXTERNALIZABLE_CODE:
                return new EntryIntrospector();
            case MetadataEntryIntrospector.EXTERNALIZABLE_CODE:
                return new MetadataEntryIntrospector();
            case ExternalEntryIntrospector.EXTERNALIZABLE_CODE:
                return new ExternalEntryIntrospector();
            default:
                throw new IllegalStateException("Unsupported introspector code: " + code);
        }
    }

    public String getDocumentWrapperClassName() {
        return _documentWrapperClassName;
    }

    public Class<? extends ExternalEntry> getExternalEntryWrapperClass() {
        return _externalEntryWrapperClass;
    }

    @Override
    public boolean isSerializedProperty(int index) {
        return _fixedProperties[index].isBinarySpaceProperty(this);
    }

    @Override
    public int findHybridIndex(int index) {
        return _fixedProperties[index].getHybridIndex();
    }

    public PropertyInfo[] getSerializedProperties() {
        return _serializedProperties;
    }

    public PropertyInfo[] getNonSerializedProperties() {
        return _nonSerializedProperties;
    }

    public int[] getPositionsForScanning() {
        return positionsForScanning;
    }
}