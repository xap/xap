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

import com.gigaspaces.annotation.pojo.*;
import com.gigaspaces.annotation.pojo.SpaceClass.IncludeProperties;
import com.gigaspaces.annotation.pojo.SpaceProperty.IndexType;
import com.gigaspaces.client.storage_adapters.class_storage_adapters.ClassBinaryStorageAdapter;
import com.gigaspaces.document.DocumentProperties;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.io.XmlUtils;
import com.gigaspaces.internal.metadata.annotations.CustomSpaceIndex;
import com.gigaspaces.internal.metadata.annotations.CustomSpaceIndexes;
import com.gigaspaces.internal.metadata.annotations.SpaceSystemClass;
import com.gigaspaces.internal.metadata.pojo.PojoPropertyInfo;
import com.gigaspaces.internal.metadata.pojo.PojoTypeInfo;
import com.gigaspaces.internal.metadata.pojo.PojoTypeInfoRepository;
import com.gigaspaces.internal.query.valuegetter.ISpaceValueGetter;
import com.gigaspaces.internal.reflection.IConstructor;
import com.gigaspaces.internal.reflection.IParamsConstructor;
import com.gigaspaces.internal.reflection.IProperties;
import com.gigaspaces.internal.reflection.ReflectionUtil;
import com.gigaspaces.internal.utils.ObjectUtils;
import com.gigaspaces.internal.utils.ReflectionUtils;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.metadata.SpaceDocumentSupport;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.gigaspaces.metadata.SpaceMetadataValidationException;
import com.gigaspaces.metadata.StorageType;
import com.gigaspaces.metadata.index.CustomIndex;
import com.gigaspaces.metadata.index.ISpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexFactory;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.gigaspaces.metadata.index.SpacePropertyIndex;
import com.gigaspaces.server.ServerEntry;
import com.j_spaces.kernel.ClassLoaderHelper;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@com.gigaspaces.api.InternalApi
public class SpaceTypeInfo implements Externalizable {
    private static final long serialVersionUID = 1L;
    private static final byte OldVersionId = 2;

    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_METADATA_POJO);

    private static final String GS_XML_SUFFIX = ".gs.xml";

    private transient final Object _lock = new Object();
    private Class<?> _type;
    private SpaceTypeInfo _superTypeInfo;
    private IConstructor<?> _constructor;
    private IParamsConstructor<?> _paramsConstructor;
    private String[] _superClasses;

    private boolean _systemClass;
    private Boolean _persist;
    private Boolean _replicate;
    private FifoSupport _fifoSupport;
    private Boolean _inheritIndexes;
    private IncludeProperties _includeProperties;

    private Map<String, SpaceIndex> _indexes;
    private Map<String, SpacePropertyInfo> _properties;
    private SpacePropertyInfo[] _spaceProperties;
    private Class<? extends ClassBinaryStorageAdapter> _spaceClassStorageAdapter;
    private SpacePropertyInfo _idProperty;
    private Boolean _idAutoGenerate;
    private SpacePropertyInfo _routingProperty;
    private SpacePropertyInfo _versionProperty;
    private SpacePropertyInfo _persistProperty;
    private SpacePropertyInfo _leaseExpirationProperty;
    private SpacePropertyInfo _dynamicPropertiesProperty;

    private String _fifoGroupingName;
    private Set<String> _fifoGroupingIndexes;

    private transient IProperties _propertiesAccessor;

    private StorageType _storageType = StorageType.DEFAULT;

    private Boolean _blobstoreEnabled;

    private ConstructorInstantiationDescriptor _constructorDescriptor;

    private String _sequenceNumberPropertyName;

    private Boolean _broadcast;

    /**
     * Default constructor for externalizable.
     */
    public SpaceTypeInfo() {
    }

    public SpaceTypeInfo(Class<?> type, SpaceTypeInfo superTypeInfo, Map<String, Node> xmlMap) {
        initialize(type, superTypeInfo);

        InitContext initContext = new InitContext();

        // Initialize:
        boolean isInitialized = initByGsXml(type, xmlMap, initContext);
        if (!isInitialized)
            initByAnnotations(initContext);

        applyDefaults();

        if (hasConstructorProperties()) {
            generateConstructorBasedSpaceProperties(initContext);
        } else {
            generateSpaceProperties(initContext, null, false);

        }

        initIndexes(initContext);
        validate();
    }

    public String getName() {
        return _type.getName();
    }

    public Class<?> getType() {
        return _type;
    }

    public IParamsConstructor<?> getParamsConstructor() {
        if (_paramsConstructor != null)
            return _paramsConstructor;

        try {
            Constructor<?> constructor = _type.getDeclaredConstructor(_constructorDescriptor.getConstructorParameterTypes());
            _paramsConstructor = ReflectionUtil.createParamsCtor(constructor);
            return _paramsConstructor;
        } catch (SecurityException e) {
            throw new SpaceMetadataException("Error getting constructor for type [" + _type.getName() + "].", e);
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    public IConstructor<?> getDefaultConstructor() {
        if (_constructor != null)
            return _constructor;

        try {
            Constructor<?> constructor = _type.getDeclaredConstructor();
            _constructor = ReflectionUtil.createCtor(constructor);
            return _constructor;
        } catch (SecurityException e) {
            throw new SpaceMetadataException("Error getting default constructor for type [" + _type.getName() + "].", e);
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    public SpaceTypeInfo getSuperTypeInfo() {
        return _superTypeInfo;
    }

    public String[] getSuperClasses() {
        return _superClasses;
    }

    public boolean hasConstructorProperties() {
        // The first condition holds during initial type info initialization
        // The second condition holds for deserialized instances
        return _constructorDescriptor != null;
    }

    public boolean isSystemClass() {
        return _systemClass;
    }

    public boolean isPersist() {
        return _persist;
    }

    public boolean isReplicate() {
        return _replicate;
    }

    public FifoSupport getFifoSupport() {
        return _fifoSupport;
    }

    public StorageType getStorageType() {
        return _storageType;
    }

    public boolean isBlobstoreEnabled() {
        return _blobstoreEnabled;
    }

    public boolean isBroadcast() {
        return _broadcast;
    }

    public int getNumOfProperties() {
        return _properties.size();
    }

    public int getNumOfSpaceProperties() {
        return _spaceProperties.length;
    }

    public SpacePropertyInfo[] getSpaceProperties() {
        return _spaceProperties;
    }

    public SpacePropertyInfo getProperty(int ordinal) {
        return _spaceProperties[ordinal];
    }

    public SpacePropertyInfo getProperty(String name) {
        return _properties.get(name);
    }

    public String getSequenceNumberPropertyName() {
        return _sequenceNumberPropertyName;
    }

    public int indexOf(SpacePropertyInfo property) {
        if (property == null)
            return -1;

        for (int i = 0; i < _spaceProperties.length; i++)
            if (_spaceProperties[i] == property)
                return i;

        return -1;
    }

    public SpacePropertyInfo getIdProperty() {
        return _idProperty;
    }

    public boolean getIdAutoGenerate() {
        return _idAutoGenerate;
    }

    public SpacePropertyInfo getDynamicPropertiesProperty() {
        return _dynamicPropertiesProperty;
    }

    public String getFifoGroupingName() {
        return _fifoGroupingName;
    }

    public Set<String> getFifoGroupingIndexes() {
        return _fifoGroupingIndexes;
    }

    public SpacePropertyInfo getLeaseExpirationProperty() {
        return _leaseExpirationProperty;
    }

    public SpacePropertyInfo getPersistProperty() {
        return _persistProperty;
    }

    public SpacePropertyInfo getRoutingProperty() {
        return _routingProperty;
    }

    public SpacePropertyInfo getVersionProperty() {
        return _versionProperty;
    }

    public Object createInstance() {
        IConstructor<?> constructor = getDefaultConstructor();
        if (constructor == null)
            throw new SpaceMetadataValidationException(_type, "Type must have a constructor with no parameters.");

        try {
            return constructor.newInstance();
        } catch (InvocationTargetException e) {
            if (_logger.isErrorEnabled())
                _logger.error("Failed to create instance of type [" + _type.getName() + "]: " + e.getMessage(), e);
            throw new SpaceMetadataException("Failed to create instance of type [" + _type.getName() + "]: " + e.getMessage(), e);
        } catch (InstantiationException e) {
            if (_logger.isErrorEnabled())
                _logger.error("Failed to create instance of type [" + _type.getName() + "]: " + e.getMessage(), e);
            throw new SpaceMetadataException("Failed to create instance of type [" + _type.getName() + "]: " + e.getMessage(), e);
        } catch (IllegalAccessException e) {
            if (_logger.isErrorEnabled())
                _logger.error("Failed to create instance of type [" + _type.getName() + "]: " + e.getMessage(), e);
            throw new SpaceMetadataException("Failed to create instance of type [" + _type.getName() + "]: " + e.getMessage(), e);
        }
    }

    public Object createConstructorBasedInstance(
            Object[] spacePropertyValues,
            Map<String, Object> dynamicProperties,
            String uid,
            int version,
            long lease,
            boolean persistent) {
        IParamsConstructor<?> paramsConstructor = getParamsConstructor();
        if (paramsConstructor == null)
            throw new SpaceMetadataValidationException(_type, "Type must have a exactly 1 constructor with parameters.");

        try {
            Object[] constructorArguments = new Object[_constructorDescriptor.getNumberOfParameters()];

            if (spacePropertyValues != null) {
                for (int i = 0; i < spacePropertyValues.length; i++) {
                    int spacePropertyToConstructorIndex = _constructorDescriptor.getSpacePropertyToConstructorIndex(i);
                    // at the time of writing this code, this could only be
                    // in the case of id auto generate true, the actual value in this case will be null
                    if (spacePropertyToConstructorIndex < 0)
                        continue;

                    Object value = getProperty(i).convertFromNullIfNeeded(spacePropertyValues[i]);
                    constructorArguments[spacePropertyToConstructorIndex] = value;
                }
            }

            if (_constructorDescriptor.getDynamicPropertiesConstructorIndex() >= 0)
                constructorArguments[_constructorDescriptor.getDynamicPropertiesConstructorIndex()] = getDocumentProperties(dynamicProperties);
            if (_idAutoGenerate && _constructorDescriptor.getIdPropertyConstructorIndex() >= 0)
                constructorArguments[_constructorDescriptor.getIdPropertyConstructorIndex()] = uid;
            if (_constructorDescriptor.getVersionConstructorIndex() >= 0)
                constructorArguments[_constructorDescriptor.getVersionConstructorIndex()] = version;
            if (_constructorDescriptor.getLeaseConstructorIndex() >= 0)
                constructorArguments[_constructorDescriptor.getLeaseConstructorIndex()] = lease;
            if (_constructorDescriptor.getPersistConstructorIndex() >= 0)
                constructorArguments[_constructorDescriptor.getPersistConstructorIndex()] = persistent;

            // Add default values for primitive types
            for (int i = 0; i < _constructorDescriptor.getNumberOfParameters(); i++) {
                if (_constructorDescriptor.isIndexExcluded(i))
                    constructorArguments[i] = ObjectUtils.getDefaultValue(_constructorDescriptor.getConstructorParameterTypes()[i]);
            }

            Object result;
            try {
                result = _paramsConstructor.newInstance(constructorArguments);
            } catch (NullPointerException e){
                throw new SpaceMetadataException("Error creating a new instance with constructor for type [" + _type.getName() + "].", e);
            }
            if (_constructorDescriptor.getConstructorParameterNames().length < _spaceProperties.length) {
                for (int i = 0; i < spacePropertyValues.length; i++) {
                    int spacePropertyToConstructorIndex = _constructorDescriptor.getSpacePropertyToConstructorIndex(i);
                    if (spacePropertyToConstructorIndex < 0) {
                        _spaceProperties[i].setValue(result, spacePropertyValues[i]);
                    }
                }

            }

            // Handling of metadata properties which can bet set using a setter method as well.
            if (_idAutoGenerate && _idProperty != null && _constructorDescriptor.getIdPropertyConstructorIndex() < 0)
                _idProperty.setValue(result, uid);

            if (_versionProperty != null && _constructorDescriptor.getVersionConstructorIndex() < 0)
                _versionProperty.setValue(result, version);

            if (_leaseExpirationProperty != null && _constructorDescriptor.getLeaseConstructorIndex() < 0)
                _leaseExpirationProperty.setValue(result, lease);

            if (_persistProperty != null && _constructorDescriptor.getPersistConstructorIndex() < 0)
                _persistProperty.setValue(result, persistent);

            if (_dynamicPropertiesProperty != null && _constructorDescriptor.getDynamicPropertiesConstructorIndex() < 0)
                _dynamicPropertiesProperty.setValue(result, getDocumentProperties(dynamicProperties));

            return result;
        } catch (InvocationTargetException e) {
            if (_logger.isErrorEnabled())
                _logger.error("Failed to create instance of type [" + _type.getName() + "]: " + e.getMessage(), e);
            throw new SpaceMetadataException("Failed to create instance of type [" + _type.getName() + "]: " + e.getMessage(), e);
        } catch (InstantiationException e) {
            if (_logger.isErrorEnabled())
                _logger.error("Failed to create instance of type [" + _type.getName() + "]: " + e.getMessage(), e);
            throw new SpaceMetadataException("Failed to create instance of type [" + _type.getName() + "]: " + e.getMessage(), e);
        } catch (IllegalAccessException e) {
            if (_logger.isErrorEnabled())
                _logger.error("Failed to create instance of type [" + _type.getName() + "]: " + e.getMessage(), e);
            throw new SpaceMetadataException("Failed to create instance of type [" + _type.getName() + "]: " + e.getMessage(), e);
        }
    }

    private DocumentProperties getDocumentProperties(Map<String, Object> dynamicProperties) {
        if (dynamicProperties == null)
            return null;
        if (dynamicProperties instanceof DocumentProperties)
            return (DocumentProperties) dynamicProperties;
        DocumentProperties result = new DocumentProperties();
        result.putAll(dynamicProperties);
        return result;
    }

    private IProperties getSpacePropertiesAccessor() {
        if (_propertiesAccessor == null) {
            synchronized (_lock) {
                if (_propertiesAccessor == null)
                    _propertiesAccessor = ReflectionUtil.createProperties(this);
            }
        }

        return _propertiesAccessor;
    }

    public Object[] getSpacePropertiesValues(Object object, boolean useNullValues) {
        Object[] values;
        try {
            values = getSpacePropertiesAccessor().getValues(object);
        } catch (IllegalArgumentException e) {
            throw new SpaceMetadataException("Failed to get values from object of type '" + object.getClass().getName() + "'.", e);
        } catch (IllegalAccessException e) {
            throw new SpaceMetadataException("Failed to get values from object of type '" + object.getClass().getName() + "'.", e);
        } catch (InvocationTargetException e) {
            throw new SpaceMetadataException("Failed to get values from object of type '" + object.getClass().getName() + "'.", e);
        }

        if (useNullValues) {
            for (int i = 0; i < values.length; i++)
                values[i] = _spaceProperties[i].convertToNullIfNeeded(values[i]);
        }

        return values;
    }

    public void setSpacePropertiesValues(Object object, Object[] values) {
        try {
            getSpacePropertiesAccessor().setValues(object, values);
        } catch (IllegalArgumentException e) {
            throw new SpaceMetadataException("Failed to set values in object of type '" + object.getClass().getName() + "'.", e);
        } catch (IllegalAccessException e) {
            throw new SpaceMetadataException("Failed to set values in object of type '" + object.getClass().getName() + "'.", e);
        } catch (InvocationTargetException e) {
            throw new SpaceMetadataException("Failed to set values in object of type '" + object.getClass().getName() + "'.", e);
        }
    }

    ////////////////////////////////////////
    //      Initialization methods     /////
    ////////////////////////////////////////
    private void initialize(Class<?> type, SpaceTypeInfo superTypeInfo) {
        this._type = type;
        this._superTypeInfo = superTypeInfo;
        this._properties = generateProperties(type);
        this._superClasses = generateSuperClasses(type, superTypeInfo);
        this._indexes = new HashMap<String, SpaceIndex>();
        this._fifoGroupingIndexes = new HashSet<String>();
    }

    private static Map<String, SpacePropertyInfo> generateProperties(Class<?> type) {
        Map<String, SpacePropertyInfo> properties = new HashMap<String, SpacePropertyInfo>();

        final PojoTypeInfo pojoTypeInfo = PojoTypeInfoRepository.getPojoTypeInfo(type);
        for (Entry<String, PojoPropertyInfo> entry : pojoTypeInfo.getProperties().entrySet())
            if (entry.getValue().getType() != null)
                properties.put(entry.getKey(), new SpacePropertyInfo(entry.getValue()));

        return properties;
    }

    private static String[] generateSuperClasses(Class<?> type, SpaceTypeInfo superTypeInfo) {
        if (superTypeInfo == null)
            return new String[]{Object.class.getName(), Object.class.getName()};
        if (superTypeInfo.getSuperTypeInfo() == null)
            return new String[]{type.getName(), superTypeInfo.getName()};

        String[] superSuperClasses = superTypeInfo.getSuperClasses();
        String[] superClasses = new String[superSuperClasses.length + 1];
        superClasses[0] = type.getName();
        for (int i = 1; i < superClasses.length; i++)
            superClasses[i] = superSuperClasses[i - 1];

        return superClasses;
    }

    private void applyDefaults() {
        if (_persist == null)
            _persist = PojoDefaults.PERSIST;
        if (_replicate == null)
            _replicate = PojoDefaults.REPLICATE;
        if (_blobstoreEnabled == null)
            _blobstoreEnabled = PojoDefaults.BLOBSTORE_ENABLED;
        if (_fifoSupport == null || _fifoSupport == FifoSupport.NOT_SET || _fifoSupport == FifoSupport.DEFAULT)
            _fifoSupport = _superTypeInfo != null ? _superTypeInfo.getFifoSupport() : PojoDefaults.FIFO_SUPPORT;
        if (_inheritIndexes == null)
            _inheritIndexes = PojoDefaults.INHERIT_INDEXES;
        if (_includeProperties == null)
            _includeProperties = PojoDefaults.INCLUDE_PROPERTIES;

        if (_superTypeInfo != null) {
            _idProperty = updatePropertyBySuper(_idProperty, _superTypeInfo._idProperty);
            _leaseExpirationProperty = updatePropertyBySuper(_leaseExpirationProperty, _superTypeInfo._leaseExpirationProperty);
            _persistProperty = updatePropertyBySuper(_persistProperty, _superTypeInfo._persistProperty);
            _routingProperty = updatePropertyBySuper(_routingProperty, _superTypeInfo._routingProperty);
            _versionProperty = updatePropertyBySuper(_versionProperty, _superTypeInfo._versionProperty);
            _dynamicPropertiesProperty = updatePropertyBySuper(_dynamicPropertiesProperty, _superTypeInfo._dynamicPropertiesProperty);

            if (_fifoGroupingName == null)
                _fifoGroupingName = _superTypeInfo.getFifoGroupingName();
            else if (_superTypeInfo._fifoGroupingName != null)
                throw new SpaceMetadataValidationException(_type, "Cannot declare a fifo grouping [" + _fifoGroupingName + "] if one has already been defined in the super class [" + _superTypeInfo._fifoGroupingName + "].");

            if (_sequenceNumberPropertyName == null)
                _sequenceNumberPropertyName = _superTypeInfo.getSequenceNumberPropertyName();
            else if (_superTypeInfo._sequenceNumberPropertyName != null)
                throw new SpaceMetadataValidationException(_type, "Cannot declare a sequence-number [" + _sequenceNumberPropertyName + "] if one has already been defined in the super class [" + _superTypeInfo._sequenceNumberPropertyName + "].");


            for (String superFifoGroupingIndex : _superTypeInfo._fifoGroupingIndexes)
                _fifoGroupingIndexes.add(superFifoGroupingIndex);

            StorageType superStorageType = _superTypeInfo.getStorageType();
            if (_storageType == StorageType.DEFAULT) {
                _storageType = superStorageType;
            } else if (superStorageType != StorageType.DEFAULT && superStorageType != _storageType)
                throw new SpaceMetadataValidationException(_type, "Cannot declare a storage type [" + _storageType + "] if one has already been defined in the super class [" + superStorageType + "].");

            if (_idAutoGenerate == null)
                _idAutoGenerate = _superTypeInfo._idAutoGenerate;
        }

        if (_idAutoGenerate == null)
            _idAutoGenerate = false;

        if(_broadcast == null)
            _broadcast = PojoDefaults.BROADCAST;
    }

    private SpacePropertyInfo updatePropertyBySuper(SpacePropertyInfo currProperty, SpacePropertyInfo superProperty) {
        return (currProperty != null || superProperty == null
                ? currProperty
                : _properties.get(superProperty.getName()));
    }

    //////////////////////////////////
    //		gs.xml init             //
    //////////////////////////////////

    private boolean initByGsXml(Class<?> type, Map<String, Node> xmlMap, InitContext initContext) {
        Node classNode = findClassNode(type, xmlMap);
        if (classNode == null)
            return false;

        _persist = XmlUtils.getAttributeBoolean(classNode, "persist");
        _replicate = XmlUtils.getAttributeBoolean(classNode, "replicate");
        _blobstoreEnabled = XmlUtils.getAttributeBoolean(classNode, "blobstore-enabled");
        _broadcast = XmlUtils.getAttributeBoolean(classNode, "broadcast");
        _fifoSupport = XmlUtils.getAttributeEnum(classNode, "fifo-support", FifoSupport.class);
        _inheritIndexes = XmlUtils.getAttributeBoolean(classNode, "inherit-indexes");
        _includeProperties = XmlUtils.getAttributeEnum(classNode, "include-properties", IncludeProperties.class);
        _storageType = XmlUtils.getAttributeEnum(classNode, "storage-type", StorageType.class, StorageType.DEFAULT, true);

        Boolean fifo = XmlUtils.getAttributeBoolean(classNode, "fifo");
        if (fifo != null) {
            if (_fifoSupport != null)
                throw new SpaceMetadataValidationException(type, "Defining 'fifo' on a class is no longer supported - use 'fifoSupport' instead.");
            _fifoSupport = fifo.booleanValue() ? FifoSupport.ALL : FifoSupport.OFF;
        }

        List<Node> compounds = null;
        NodeList nodeList = classNode.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node propertyNode = nodeList.item(i);
            String nodeName = propertyNode.getNodeName();
            if (nodeName.equals("property")) {
                SpacePropertyInfo property = getProperty(propertyNode);
                SpaceIndexType indexType = XmlUtils.getAttributeEnum(propertyNode, "index", SpaceIndexType.class);
                //unique?
                boolean unique = XmlUtils.getAttributeBoolean(propertyNode, "unique", false);

                if (indexType != null && indexType != SpaceIndexType.NONE)
                    initContext.addIndex(property.getName(), indexType, unique);

                property.setNullValue(XmlUtils.getAttribute(propertyNode, "null-value"));

                property.setStorageType(XmlUtils.getAttributeEnum(propertyNode, "storage-type", StorageType.class, StorageType.DEFAULT, true));

                property.setStorageAdapterClass(parseClass(XmlUtils.getAttribute(propertyNode, "property-storage-adapter")));

                property.setDocumentSupport(XmlUtils.getAttributeEnum(propertyNode, "document-support", SpaceDocumentSupport.class, SpaceDocumentSupport.DEFAULT));

                // parse property indexes
                NodeList propertyNodeList = propertyNode.getChildNodes();
                for (int j = 0; j < propertyNodeList.getLength(); j++) {
                    Node propertyChildNode = propertyNodeList.item(j);
                    String propertyChildNodeName = propertyChildNode.getNodeName();
                    if (propertyChildNodeName.equals("index"))
                        addPropertyIndex(property.getName(), propertyChildNode, initContext);
                    else if (propertyChildNodeName.equals("fifo-grouping-index"))
                        updateFifoGroupingIndexes(property, propertyChildNode, initContext);
                }

                initContext.explicitlyIncluded.add(property);
            } else if (nodeName.equals("id")) {
                _idProperty = updateProperty(_idProperty, propertyNode);
                _idAutoGenerate = XmlUtils.getAttributeBoolean(propertyNode, "auto-generate");
            } else if (nodeName.equals("routing"))
                _routingProperty = updateProperty(_routingProperty, propertyNode);
            else if (nodeName.equals("version"))
                _versionProperty = updateProperty(_versionProperty, propertyNode);
            else if (nodeName.equals("persist"))
                _persistProperty = updateProperty(_persistProperty, propertyNode);
            else if (nodeName.equals("lease-expiration"))
                _leaseExpirationProperty = updateProperty(_leaseExpirationProperty, propertyNode);
            else if (nodeName.equals("dynamic-properties"))
                _dynamicPropertiesProperty = updateProperty(_dynamicPropertiesProperty, propertyNode);
            else if (nodeName.equals("fifo-grouping-property"))
                updateFifoGroupingName(propertyNode, initContext);
            else if (nodeName.equals("sequence-number"))
                updateSequenceNumberName(propertyNode, initContext);
            else if (nodeName.equals("exclude"))
                initContext.explicitlyExcluded.add(getProperty(propertyNode));
            else if (nodeName.equals("reference")) {
                String superClass = XmlUtils.getAttribute(propertyNode, "class-ref");
                String actualSuperClass = _superClasses[1];
                if (!actualSuperClass.equals(superClass)) {
                    if (_logger.isWarnEnabled())
                        _logger.warn("Metadata for type [" + _type.getName() +
                                "] defines class-ref [" + superClass +
                                "] but actual super class is [" + actualSuperClass + "]. The actual super class will be used. " +
                                "To avoid this message please remove the class-ref definition (it is not required).");
                } else if (_logger.isInfoEnabled())
                    _logger.info("Specifying class reference in gs.xml is no longer required (see type " + _type.getName() + ").");
            } else if (nodeName.equals("compound-index")) {// format = 		<compound-index paths="p1 p2"></compound-index>
                if (compounds == null)
                    compounds = new ArrayList<Node>();
                compounds.add(propertyNode);
            } else if (_logger.isWarnEnabled())
                _logger.warn("Unrecognized xml node: " + nodeName);
        }

        //do we have compound indices to cater for ?
        if (compounds != null && !compounds.isEmpty()) {
            for (Node c : compounds) {
                String paths = XmlUtils.getAttribute(c, "paths", null);
                if (paths == null || paths.equals("")) {
                    _logger.error("missing paths in xml definition of compound index class " + _type.getName());
                    throw new IllegalArgumentException("missing paths in xml definition of compound index class " + _type.getName());
                }
                //parse the paths
                String delim = paths.indexOf(",") != -1 ? "," : " ";
                StringTokenizer st = new StringTokenizer(paths, delim);
                int pnum = st.countTokens();
                if (pnum < 2) {
                    _logger.error("invalid paths in xml definition of compound index class " + _type.getName() + " paths=" + paths);
                    throw new IllegalArgumentException("invalid paths in xml definition of compound index class " + _type.getName() + " paths=" + paths);
                }
                String[] ps = new String[pnum];
                for (int i = 0; i < pnum; i++)
                    ps[i] = st.nextToken().trim();
                //index type
                IndexType indexType = XmlUtils.getAttributeEnum(c, "type", IndexType.class, IndexType.BASIC, true /*throwOnError*/);
                //unique?
                boolean unique = XmlUtils.getAttributeBoolean(c, "unique", false);
                try {
                    addCompoundIndex(ps, indexType.toSpaceIndexType(), unique);
                } catch (Exception ex) {
                    _logger.error("creating a compound index from GSXML failed class " + _type.getName() + " exception=" + ex);
                    throw new RuntimeException("creating a compound index from GSXML failed class " + _type.getName() + " exception=" + ex);
                }
            }
        }

        return true;
    }

    private static <T> Class<T> parseClass(String className) {
        try {
            return className == null ? null : (Class<T>) Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new SpaceMetadataException("Failed to load class: " + className, e);
        }
    }

    private void addPropertyIndex(String name, com.gigaspaces.annotation.pojo.SpaceIndex annotation, InitContext initContext) {
        if (annotation == null)
            return;

        addPropertyIndex(name, annotation.path(), annotation.type(), "@" + annotation.annotationType().getSimpleName(), annotation.unique(), initContext);
    }

    private void addPropertyIndex(String name, Node xmlNode, InitContext initContext) {
        if (xmlNode == null)
            return;

        String path = XmlUtils.getAttribute(xmlNode, "path");
        SpaceIndexType indexType = XmlUtils.getAttributeEnum(xmlNode, "type", SpaceIndexType.class, SpaceIndexType.EQUAL);
        //unique?
        boolean unique = XmlUtils.getAttributeBoolean(xmlNode, "unique", false);
        addPropertyIndex(name, path, indexType, "<index>", unique, initContext);
    }

    private void addPropertyIndex(String name, String path, SpaceIndexType indexType, String configName, boolean unique, InitContext initContext) {

        if (path == null || path.length() == 0)
            initContext.addIndex(name, indexType, unique);
        else {
            // Add property name to index path
            if (path.startsWith(SpaceCollectionIndex.COLLECTION_INDICATOR)) {
                // path = "[*]..."
                path = name + path;
            } else {
                // path = "property..." or "property[*]..."
                path = name + "." + path;
            }
            SpaceIndex index = SpaceIndexFactory.createPathIndex(path, indexType, unique);
            addIndex(index, configName);
        }
    }


    private Node findClassNode(Class<?> type, Map<String, Node> xmlMap) {
        // Try to get xml info from xml cache:
        String typeName = type.getName();
        if (xmlMap != null) {
            Node classNode = xmlMap.get(typeName);
            if (classNode != null)
                return classNode;
        }

        // Otherwise, try to get from current folder + fully qualified type name:
        String fileName = "/" + typeName.replace('.', '/') + GS_XML_SUFFIX;
        Node classNode = SpaceTypeInfoRepository.loadFile(fileName, typeName);
        if (classNode != null)
            return classNode;

		/*
        // Otherwise, try to get from current folder + short type name:
		fileName = "/" + type.getSimpleName().replace('.', '/') + GS_XML_SUFFIX;
		classNode = typeInfoRepository.loadFile(fileName, typeName);
		classNode = typeInfoRepository.getClassXmlNode(typeName);
		if (classNode != null)
			return classNode;
		*/

        // Otherwise, not available:
        return null;
    }

    private SpacePropertyInfo getProperty(Node xmlNode) {
        String name = XmlUtils.getAttribute(xmlNode, "name");
        if (name == null || name.length() == 0)
            throw new SpaceMetadataValidationException(_type, "Property name was not set for xml node " + xmlNode.getNodeName());
        SpacePropertyInfo property = _properties.get(name);
        if (property == null)
            throw new SpaceMetadataValidationException(_type, "Type does not contain property [" + name + "] specified in xml node [" + xmlNode.getNodeName() + "].");
        return property;
    }

    private SpacePropertyInfo updateProperty(SpacePropertyInfo currProperty, Node propertyNode) {
        SpacePropertyInfo newProperty = getProperty(propertyNode);
        if (currProperty != null)
            throw new SpaceMetadataValidationException(_type, "xml node [" + propertyNode.getNodeName() +
                    "] is declared on both [" + currProperty.getName() + "] and [" + newProperty.getName() + "].");
        return newProperty;
    }

    private void updateFifoGroupingName(Node propertyNode, InitContext initContext) {
        SpacePropertyInfo newProperty = getProperty(propertyNode);

        String path = XmlUtils.getAttribute(propertyNode, "path");
        validateNoCollectionPath(path);

        String fifoGroupingName = constructFifoGroupingName(newProperty.getName(), path);
        if (_fifoGroupingName != null)
            throw new SpaceMetadataValidationException(_type, "xml node [" + propertyNode.getNodeName() +
                    "] is declared on both [" + _fifoGroupingName + "] and [" + fifoGroupingName + "].");

        _fifoGroupingName = fifoGroupingName;

        initContext.explicitlyIncluded.add(newProperty);
    }

    private void updateSequenceNumberName(Node propertyNode, InitContext initContext) {
        SpacePropertyInfo newProperty = getProperty(propertyNode);

        String sequenceNumberPropertyName = XmlUtils.getAttribute(propertyNode, "name");

        if (_sequenceNumberPropertyName != null)
            throw new SpaceMetadataValidationException(_type, "xml node [" + propertyNode.getNodeName() +
                    "] is declared on both [" + _sequenceNumberPropertyName + "] and [" + sequenceNumberPropertyName + "].");

        _sequenceNumberPropertyName = sequenceNumberPropertyName;

        initContext.explicitlyIncluded.add(newProperty);
    }

    private void updateFifoGroupingIndexes(SpacePropertyInfo property, Node propertyNode, InitContext initContext) {
        String path = XmlUtils.getAttribute(propertyNode, "path");
        validateNoCollectionPath(path);

        _fifoGroupingIndexes.add(constructFifoGroupingName(property.getName(), path));
        initContext.explicitlyIncluded.add(property);
    }

    private String constructFifoGroupingName(String fifoGroupingPropertyName, String fifoGroupingPropertyPath) {
        String fifoGroupingName = fifoGroupingPropertyName;
        if (fifoGroupingPropertyPath != null && fifoGroupingPropertyPath.length() != 0)
            fifoGroupingName += "." + fifoGroupingPropertyPath;
        return fifoGroupingName;
    }

    //////////////////////////////////
    //		Annotations init        //
    //////////////////////////////////
    @SuppressWarnings("deprecation")
    private void initByAnnotations(InitContext initContext) {
        SpaceClass classAnnotation = _type.getAnnotation(SpaceClass.class);
        if (classAnnotation != null) {
            _persist = classAnnotation.persist();
            _replicate = classAnnotation.replicate();
            _fifoSupport = classAnnotation.fifoSupport();
            _inheritIndexes = classAnnotation.inheritIndexes();
            _includeProperties = classAnnotation.includeProperties();
            _storageType = classAnnotation.storageType();
            _blobstoreEnabled = classAnnotation.blobstoreEnabled();
            _broadcast = classAnnotation.broadcast();
        }
        SpaceSystemClass systemClassAnnotation = _type.getAnnotation(SpaceSystemClass.class);
        if (systemClassAnnotation != null) {
            _systemClass = true;
        }
        CustomSpaceIndexes customIndexesAnnotation = _type.getAnnotation(CustomSpaceIndexes.class);
        if (customIndexesAnnotation != null) {
            CustomSpaceIndex[] customIndexAnnotations = customIndexesAnnotation.value();
            if (customIndexAnnotations != null)
                for (int i = 0; i < customIndexAnnotations.length; i++)
                    addCustomIndex(customIndexAnnotations[i]);
        }

        // check for top level custom index definition
        addCustomIndex(_type.getAnnotation(CustomSpaceIndex.class));

        SpaceClassBinaryStorage spaceClassBinaryStorage = _type.getAnnotation(SpaceClassBinaryStorage.class);
        if (spaceClassBinaryStorage != null){
            this._spaceClassStorageAdapter = spaceClassBinaryStorage.layout().getAdapterClass();
            // TODO: validate ambiguity.
            this._storageType = StorageType.BINARY;
        }

        for (Entry<String, SpacePropertyInfo> entry : _properties.entrySet()) {
            SpacePropertyInfo property = entry.getValue();
            Method getter = property.getGetterMethod();
            if (getter == null)
                continue;

            if (getter.getDeclaringClass() != _type)
                continue;

            SpaceProperty propertyAnnotation = getter.getAnnotation(SpaceProperty.class);
            if (propertyAnnotation != null) {
                initContext.explicitlyIncluded.add(property);
                property.setNullValue(propertyAnnotation.nullValue());
                property.setDocumentSupport(propertyAnnotation.documentSupport());
                IndexType indexType = propertyAnnotation.index();
                if (indexType != null && indexType != IndexType.NOT_SET)
                    initContext.addIndex(property.getName(), indexType.toSpaceIndexType());
            } else {
                property.setDocumentSupport(SpaceDocumentSupport.DEFAULT);
            }

            SpacePropertyStorage spacePropertyStorage = getter.getAnnotation(SpacePropertyStorage.class);
            if (spacePropertyStorage != null){
                property.setStorageType(spacePropertyStorage.value());
            }

            SpaceStorageType storageTypeAnnotation = getter.getAnnotation(SpaceStorageType.class);
            if (storageTypeAnnotation != null) {
                if (spacePropertyStorage != null)
                    throw new SpaceMetadataException("SpaceStorageType cannot be used with SpacePropertyStorage");
                property.setStorageType(storageTypeAnnotation.storageType());
                initContext.explicitlyIncluded.add(property);
            }
            // else it will set to OBJECT storage type only if the property is a space property.

            SpacePropertyStorageAdapter storageAdapterAnnotation = getter.getAnnotation(SpacePropertyStorageAdapter.class);
            if (storageAdapterAnnotation != null) {
                property.setStorageAdapterClass(storageAdapterAnnotation.value());
                initContext.explicitlyIncluded.add(property);
            }

            SpaceId idAnnotation = getter.getAnnotation(SpaceId.class);
            if (idAnnotation != null) {
                _idProperty = updateProperty(_idProperty, property, idAnnotation);
                _idAutoGenerate = idAnnotation.autoGenerate();
            }

            _routingProperty = updateProperty(_routingProperty, property, SpaceRouting.class);
            _versionProperty = updateProperty(_versionProperty, property, SpaceVersion.class);
            _persistProperty = updateProperty(_persistProperty, property, SpacePersist.class);
            _leaseExpirationProperty = updateProperty(_leaseExpirationProperty, property, SpaceLeaseExpiration.class);
            _dynamicPropertiesProperty = updateProperty(_dynamicPropertiesProperty, property, SpaceDynamicProperties.class);

            if (getter.getAnnotation(SpaceExclude.class) != null)
                initContext.explicitlyExcluded.add(property);

            SpaceFifoGroupingProperty fifoGroupingPropertyAnnotation = getter.getAnnotation(SpaceFifoGroupingProperty.class);
            if (fifoGroupingPropertyAnnotation != null) {
                updateFifoGroupingName(property, fifoGroupingPropertyAnnotation);
                initContext.explicitlyIncluded.add(property);
            }

            SpaceFifoGroupingIndex fifoGroupingIndexAnnotation = getter.getAnnotation(SpaceFifoGroupingIndex.class);
            if (fifoGroupingIndexAnnotation != null) {
                updateFifoGroupingIndexes(property, fifoGroupingIndexAnnotation);
                initContext.explicitlyIncluded.add(property);
            }

            // parse PathPropertyIndexes - can be a single annotation or an array (for ease of use)
            addPropertyIndex(property.getName(), getter.getAnnotation(com.gigaspaces.annotation.pojo.SpaceIndex.class), initContext);

            com.gigaspaces.annotation.pojo.SpaceIndexes indexesAnnotation = getter.getAnnotation(com.gigaspaces.annotation.pojo.SpaceIndexes.class);
            if (indexesAnnotation != null) {
                com.gigaspaces.annotation.pojo.SpaceIndex[] indexAnnotations = indexesAnnotation.value();
                if (indexAnnotations != null)
                    for (int i = 0; i < indexAnnotations.length; i++)
                        addPropertyIndex(property.getName(), indexAnnotations[i], initContext);
            }
            //is it a sequence number propery?
            SpaceSequenceNumber sequenceNumberAnnotation = getter.getAnnotation(SpaceSequenceNumber.class);
            if (sequenceNumberAnnotation != null) {
                updateSequenceNumberName(property, sequenceNumberAnnotation);
                initContext.explicitlyIncluded.add(property);
            }
        }

        //add the compound indexes, done after properties are set so a check can be made
        CompoundSpaceIndex csi = _type.getAnnotation(CompoundSpaceIndex.class);
        if (csi != null) {//a single compound
            addCompoundIndex(csi);
        }
        CompoundSpaceIndexes csis = _type.getAnnotation(CompoundSpaceIndexes.class);
        if (csis != null) {
            CompoundSpaceIndex[] cs = csis.value();
            for (CompoundSpaceIndex c : cs) {
                if (c == null)
                    throw new IllegalArgumentException("CompoundSpaceIndexes member cannot be null");
                addCompoundIndex(c);
            }
        }

        // Check if a space class contructor is defined
        Constructor<?> spaceClassConstructor = null;
        for (Constructor<?> constructor : _type.getConstructors()) {
            if (constructor.getAnnotation(SpaceClassConstructor.class) != null) {
                if (_includeProperties == IncludeProperties.EXPLICIT)
                    throw new SpaceMetadataValidationException(_type, "Cannot set explicit include properties together with space class constructor");
                if (spaceClassConstructor != null)
                    throw new SpaceMetadataValidationException(_type, "Cannot define more then one space class constructor annotation");
                spaceClassConstructor = constructor;
            }
        }
        if (spaceClassConstructor != null) {
            _constructorDescriptor = new ConstructorInstantiationDescriptor();
            _constructorDescriptor.setConstructor(spaceClassConstructor);
        }
    }

    private void addIndex(SpaceIndex index, String configName) {
        if (_indexes.containsKey(index.getName()))
            throw new SpaceMetadataValidationException(_type, configName + ": duplicate index definition. Index with the name ["
                    + index.getName() + "] is already defined.");

        _indexes.put(index.getName(), index);
    }

    private void addCustomIndex(CustomSpaceIndex indexAnnotation) {
        if (indexAnnotation == null)
            return;

        final String annotationName = "@" + indexAnnotation.annotationType().getSimpleName();
        final Class<?> indexValueGetterClass = indexAnnotation.indexValueGetter();

        if (!ISpaceValueGetter.class.isAssignableFrom(indexValueGetterClass))
            throw new SpaceMetadataValidationException(_type, annotationName
                    + "(indexValueGetter) value must be a java class that implements ["
                    + ISpaceValueGetter.class.getName() + "<IServerEntry>].");

        ISpaceValueGetter<ServerEntry> indexGetter;
        try {
            indexGetter = (ISpaceValueGetter<ServerEntry>) indexValueGetterClass.newInstance();
        } catch (Exception e) {
            throw new SpaceMetadataValidationException(_type,
                    "Failed to instantiate " + annotationName +
                            "(indexValueGetter) class [" + indexValueGetterClass.getName() + "].", e);
        }
        CustomIndex index = new CustomIndex(indexAnnotation.name(),
                indexGetter, indexAnnotation.unique(), indexAnnotation.type());

        addIndex(index, annotationName);
    }

    private void addCompoundIndex(CompoundSpaceIndex csi) {
        addCompoundIndex(csi.paths(), SpaceIndexType.EQUAL /*csi.type()*/, csi.unique());

    }

    private void addCompoundIndex(String paths[], SpaceIndexType type, boolean unique) {
        SpaceIndex index = SpaceIndexFactory.createCompoundIndex(paths, type, null /*indexName*/, unique);
        addIndex(index, index.getName());

    }

    private SpacePropertyInfo updateProperty(SpacePropertyInfo currProperty, SpacePropertyInfo newProperty,
                                             Class<? extends Annotation> annotation) {
        return updateProperty(currProperty, newProperty, newProperty.getGetterMethod().getAnnotation(annotation));
    }

    private void updateSequenceNumberName(SpacePropertyInfo property, SpaceSequenceNumber sequenceNumberAnnotation) {
        if (_sequenceNumberPropertyName != null)
            throw new SpaceMetadataValidationException(_type, property, "@" + sequenceNumberAnnotation.annotationType().getSimpleName() +
                    " is already defined on [" + _sequenceNumberPropertyName + "].");
        _sequenceNumberPropertyName = property.getName();
    }

    private void updateFifoGroupingName(SpacePropertyInfo property, SpaceFifoGroupingProperty fifoGroupingAnnotation) {
        if (_fifoGroupingName != null)
            throw new SpaceMetadataValidationException(_type, property, "@" + fifoGroupingAnnotation.annotationType().getSimpleName() +
                    " is already defined on [" + _fifoGroupingName + "].");

        String path = fifoGroupingAnnotation.path();
        validateNoCollectionPath(path);

        _fifoGroupingName = constructFifoGroupingName(property.getName(), path);
    }

    private void updateFifoGroupingIndexes(SpacePropertyInfo property, SpaceFifoGroupingIndex fifoGroupingIndexAnnotation) {
        String path = fifoGroupingIndexAnnotation.path();
        validateNoCollectionPath(path);

        _fifoGroupingIndexes.add(constructFifoGroupingName(property.getName(), path));
    }

    private void validateNoCollectionPath(String path) {
        if (path != null && path.length() != 0 && path.indexOf(SpaceCollectionIndex.COLLECTION_INDICATOR) != -1)
            throw new SpaceMetadataValidationException(_type, "[" + path + "] collection index cannot be fifo groups index");
    }

    private SpacePropertyInfo updateProperty(SpacePropertyInfo currProperty, SpacePropertyInfo newProperty, Annotation annotation) {
        // If annotation is not defined return current:
        if (annotation == null)
            return currProperty;
        // Validate value is not already set:
        if (currProperty != null)
            throw new SpaceMetadataValidationException(_type, newProperty, "@" + annotation.annotationType().getSimpleName() +
                    " is already defined on property [" + currProperty.getName() + "].");
        // Return new value:
        return newProperty;
    }

    //////////////////////////////////
    //     Properties generation    //
    //////////////////////////////////

    private void generateSpaceProperties(InitContext initContext, List<SpacePropertyInfo> spacePropertiesList, boolean afterGenerateConstructorProperties) {
        List<SpacePropertyInfo> list;
        if (spacePropertiesList == null) {
            list = new ArrayList<SpacePropertyInfo>(_properties.size());
        } else {
            list = spacePropertiesList;
        }

        for (Entry<String, SpacePropertyInfo> entry : _properties.entrySet()) {
            SpacePropertyInfo property = entry.getValue();
            processSpaceProperty(initContext, list, property, afterGenerateConstructorProperties);
        }

        Collections.sort(list);

        // HACK:
        // workaround for backwards compatibility with old ExternalEntry envelope -
        // properties were introduced in explicit order: {"key", "value", "cacheID"}.
        // since the sorted list is {cacheId, key, value} we explicitly remove the first
        // property and re-append it at the end.
        if (_type == com.j_spaces.map.Envelope.class)
            list.add(list.remove(0));

        _spaceProperties = list.toArray(new SpacePropertyInfo[list.size()]);
    }

    private void generateConstructorBasedSpaceProperties(InitContext initContext) {
        Constructor<?> constructor = _constructorDescriptor.getConstructor();
        String[] parameterNames = ConstructorPropertiesHelper.extractParameterNames(constructor);
        Class<?>[] parameterTypes = constructor.getParameterTypes();

        List<SpacePropertyInfo> list = new ArrayList<SpacePropertyInfo>(parameterNames.length);
        boolean idPropertyInParameterNames = false;
        for (int i = 0; i < parameterNames.length; i++) {
            String parameterName = parameterNames[i];
            Class<?> parameterType = parameterTypes[i];
            SpacePropertyInfo propertyInfo = _properties.get(parameterName);
            if (propertyInfo == null || propertyInfo.getType() != parameterType) {
                if (ConstructorPropertiesHelper.possiblyGenericConstructorParameterName(parameterName)) {
                    throw new SpaceMetadataValidationException(_type, "Invalid constructor parameter: " + parameterName +
                            ". It is possible that the class file does not contain " +
                            " the necessary debug information required to extract the " +
                            "constructor parameter names.");
                } else {
                    throw new SpaceMetadataValidationException(_type, "Invalid constructor parameter: " + parameterName +
                            ". Are you missing a getter for this property? (If you want" +
                            " to exclude this property, add getter for it with @SpaceExclude)");
                }
            }
            processSpaceProperty(initContext, list, propertyInfo, false);
            if (propertyInfo == _idProperty)
                idPropertyInParameterNames = true;
        }

        // Allow constructors without an id property in case the id is auto generated
        if (_idAutoGenerate && !idPropertyInParameterNames)
            processSpaceProperty(initContext, list, _idProperty, false);

        generateSpaceProperties(initContext, list, true);

        // build full descriptor
        _constructorDescriptor = ConstructorPropertiesHelper.buildConstructorInstantiationDescriptor(constructor, parameterNames, parameterTypes, this);

        extractFieldsForProperties();
    }

    // For this feature to work with change operation on local cache, we must be able to change the values of
    // the pojos we work on after their instantiation. Thus, for each property, we enfore a matching field
    // of the same name and type.
    private void extractFieldsForProperties() {
        List<SpacePropertyInfo> extractedProperties = new LinkedList<SpacePropertyInfo>();
        extractedProperties.addAll(Arrays.asList(_spaceProperties));
        if (_leaseExpirationProperty != null)
            extractedProperties.add(_leaseExpirationProperty);
        if (_persistProperty != null)
            extractedProperties.add(_persistProperty);
        if (_dynamicPropertiesProperty != null)
            extractedProperties.add(_dynamicPropertiesProperty);

        for (SpacePropertyInfo property : extractedProperties) {
            // Changing an id propertly is illegal, so there is no point of
            // enforcing a matching field to this property.
            if (property == _idProperty)
                continue;

            if (property.getSetterMethod() == null) {
                try {
                    Field field = ReflectionUtils.getDeclaredField(_type, property.getName());
                    if (field == null)
                        throw new SpaceMetadataValidationException(_type, property, "Failed finding matching field for setter-less property.");
                    if (field.getType() != property.getType())
                        throw new SpaceMetadataValidationException(_type, property, "Matching field for setter-less property is of different type then getter.");
                    property.setField(field);
                } catch (SecurityException e) {
                    throw new SpaceMetadataValidationException(_type, property, "Failed extracting matching field for setter-less property.", e);
                }
            }
        }
    }

    private void processSpaceProperty(InitContext initContext,
                                      List<SpacePropertyInfo> spaceProperties, SpacePropertyInfo property, boolean afterGenerateConstructorProperties) {
        // Get information from super type:
        int superLevel = -1;
        if (_superTypeInfo != null) {
            SpacePropertyInfo superProperty = _superTypeInfo.getProperty(property.getName());
            if (superProperty != null) {
                superLevel = superProperty.getLevel();
                // Copy null value from super:
                if (!property.hasNullValue() && superProperty.hasNullValue())
                    property.setNullValue(superProperty.getNullValue().toString());
                // Copy storage type from super:
                if (property.getStorageType() == null || property.getStorageType() == StorageType.DEFAULT)
                    property.setStorageType(superProperty.getStorageType());
                else if (superProperty.getStorageType() != null && superProperty.getStorageType() != property.getStorageType())
                    throw new SpaceMetadataValidationException(_type, property, "Cannot declare storage type different from the storage type declared in the super class.");
                // Copy storage adapter from super:
                if (property.getStorageAdapterClass() == null)
                    property.setStorageAdapterClass(superProperty.getStorageAdapterClass());
                else if (superProperty.getStorageAdapterClass() != null && superProperty.getStorageAdapterClass() != property.getStorageAdapterClass())
                    throw new SpaceMetadataValidationException(_type, property, "Cannot declare storage adapter class different from the super class definition.");

                if (property.getDocumentSupport() == null || property.getDocumentSupport() == SpaceDocumentSupport.DEFAULT)
                    property.setDocumentSupport(superProperty.getDocumentSupport());
            }
        }
        if (isSpaceProperty(property, superLevel, initContext)) {
            property.setLevel(superLevel + 1);
            applySpacePropertyAnnotationDefaults(property);
            if (!afterGenerateConstructorProperties || !isSetByConstructor(property)) {
                spaceProperties.add(property);
            }
        } else if (superLevel >= 0)
            throw new SpaceMetadataValidationException(_type, property, "Cannot exclude a property which is a space property in a super class.");
    }

    private void applySpacePropertyAnnotationDefaults(SpacePropertyInfo property) {
        if (property.getStorageType() == null)
            property.setStorageType(StorageType.DEFAULT);
        if (property.getDocumentSupport() == null || property.getDocumentSupport() == SpaceDocumentSupport.DEFAULT)
            property.setDocumentSupport(SpaceDocumentSupportHelper.getDefaultDocumentSupport(property.getType()));
    }


    private boolean isSpaceProperty(SpacePropertyInfo property, int superLevel, InitContext initContext) {
        boolean isExplicitlyExcluded = initContext.explicitlyExcluded.contains(property);
        boolean isExplicitlyIncluded = initContext.explicitlyIncluded.contains(property);
        String propertyName = property.getName();

        // Check if property is explicitly excluded:
        if (isExplicitlyExcluded) {
            if (property == _routingProperty)
                throw new SpaceMetadataValidationException(_type, property, "Cannot exclude a space routing property.");
            if (property == _idProperty)
                throw new SpaceMetadataValidationException(_type, property, "Cannot exclude a space id property.");
            if (initContext.indexedProperties.containsKey(propertyName)) {
                throw new SpaceMetadataValidationException(_type, property, "Cannot exclude an indexed property.");
            }
            if (isExplicitlyIncluded)
                throw new SpaceMetadataValidationException(_type, property, "Cannot exclude an explicitly included space property.");
            return false;
        }

        if (property == _versionProperty) {
            if (isExplicitlyIncluded)
                throw new SpaceMetadataValidationException(_type, property, "A version property cannot be a space property.");
            return false;
        }

        if (property == _leaseExpirationProperty) {
            if (isExplicitlyIncluded)
                throw new SpaceMetadataValidationException(_type, property, "A lease expiration property cannot be a space property.");
            return false;
        }
        if (property == _dynamicPropertiesProperty) {
            if (isExplicitlyIncluded)
                throw new SpaceMetadataValidationException(_type, property, "A dynamic properties property cannot be a space property.");
            return false;
        }
        if (property == _persistProperty) {
            if (isExplicitlyIncluded)
                throw new SpaceMetadataValidationException(_type, property, "A persist property cannot be a space property.");
            return false;
        }

        if (isExplicitlyIncluded)
            return true;
        if (property == _idProperty)
            return true;
        if (property == _routingProperty)
            return true;
        if (initContext.indexedProperties.containsKey(property.getName()))
            return true;

        // End of explicit property inclusion/exclusion.

        // Ignore implicit properties without getter or setter:
        Method getter = property.getGetterMethod();
        Method setter = property.getSetterMethod();
        if (getter == null || (setter == null && !isSetByConstructor(property)))
            return false;

        switch (_includeProperties) {
            case IMPLICIT:
                // If either getter or setter are defined in this class, include:
                if (getter.getDeclaringClass().equals(_type) ||
                        (setter != null && setter.getDeclaringClass().equals(_type)))
                    return true;
                // Otherwise, go by the parent:
                return superLevel >= 0;
            case EXPLICIT:
                // If explicit we go by the super - we cannot exclude what's included in the super.
                return superLevel >= 0;
            default:
                throw new IllegalArgumentException("Illegal IncludeProperties: " + _includeProperties);
        }
    }

    private boolean isSetByConstructor(SpacePropertyInfo property) {
        if (_constructorDescriptor != null) {
            Constructor<?> constructor = _constructorDescriptor.getConstructor();
            List<String> constructorParamNames = new ArrayList<String>(Arrays.asList(ConstructorPropertiesHelper.extractParameterNames(constructor)));
            return constructorParamNames.contains(property.getName());
        } else {
            return false;
        }
    }

    private void initIndexes(InitContext initContext) {
        for (Entry<String, SpaceIndex> entry : initContext.indexedProperties.entrySet()) {
            final String name = entry.getKey();
            if (_indexes.containsKey(name))
                throw new SpaceMetadataValidationException(_type, "Duplicate index definition for '" + name + "'.");
            SpacePropertyInfo property = getProperty(name);
            SpaceIndexType indexType = entry.getValue().getIndexType();
            int propertyPosition = indexOf(property);
            boolean isUnique = (property == _idProperty && !_idAutoGenerate);
            if (!isUnique && entry.getValue().getIndexType().isIndexed())
                isUnique = ((ISpaceIndex) (entry.getValue())).isUnique();
            SpaceIndex index = new SpacePropertyIndex(property.getName(), indexType, isUnique, propertyPosition);
            _indexes.put(index.getName(), index);
        }

        // Copy index type from super:
        // TODO: Consider cloning index.
        if (_inheritIndexes && _superTypeInfo != null) {
            for (Entry<String, SpaceIndex> entry : _superTypeInfo.getIndexes().entrySet()) {
                if (!_indexes.containsKey(entry.getKey())) {
                    SpaceIndex index = entry.getValue();
                    String indexName = entry.getKey();
                    _indexes.put(indexName, index);
                }
            }
        }

        // Set default index for id property, if not set:
        if (_idProperty != null && !_idAutoGenerate && !_indexes.containsKey(_idProperty.getName())) {
            SpaceIndex index = new SpacePropertyIndex(_idProperty.getName(), SpaceIndexType.EQUAL, true, indexOf(_idProperty));
            _indexes.put(index.getName(), index);
        }

        // Set default index for routing property, if not set:
        if (_routingProperty != null && !_indexes.containsKey(_routingProperty.getName())) {
            SpaceIndex index = new SpacePropertyIndex(_routingProperty.getName(), SpaceIndexType.EQUAL, false, indexOf(_routingProperty));
            _indexes.put(index.getName(), index);
        }
    }

    //////////////////////////////////
    //		Validation methods      //
    //////////////////////////////////

    private static enum ConstructorPropertyValidation {
        REQUIERS_SETTER,
        REQUIERS_CONSTRUCTOR_PARAM,
        REQUIERS_AT_LEAST_ONE;
    }

    private void validate() {
        // Validate properties types:
        validatePropertyType(_persistProperty, "persist", boolean.class, Boolean.class);
        validatePropertyType(_leaseExpirationProperty, "lease expiration", long.class, Long.class);
        validatePropertyType(_versionProperty, "version", int.class, Integer.class);
        validatePropertyType(_dynamicPropertiesProperty, "dynamic properties", Map.class, Map.class);
        if (_idProperty != null && _idAutoGenerate && _idProperty.getType() != String.class)
            throw new SpaceMetadataValidationException(_type, _idProperty, "id property with autogenerate=true must be of type String.");
        validateSequenceNumberProperty();

        // Validate combinations:
        validatePropertyCombination(_versionProperty, _leaseExpirationProperty, "version", "lease expiration");
        validatePropertyCombination(_versionProperty, _persistProperty, "version", "persist");
        validatePropertyCombination(_versionProperty, _idProperty, "version", "id");
        validatePropertyCombination(_versionProperty, _routingProperty, "version", "routing");

        validatePropertyCombination(_leaseExpirationProperty, _persistProperty, "lease expiration", "persist");
        validatePropertyCombination(_leaseExpirationProperty, _idProperty, "lease expiration", "id");
        validatePropertyCombination(_leaseExpirationProperty, _routingProperty, "lease expiration", "routing");

        validatePropertyCombination(_persistProperty, _idProperty, "persist", "id");
        validatePropertyCombination(_persistProperty, _routingProperty, "persist", "routing");

        if(isBroadcast())
            validateBroadcastTable();

        validateGetterSetter(_idProperty, "Id", _idAutoGenerate ? ConstructorPropertyValidation.REQUIERS_SETTER :
                ConstructorPropertyValidation.REQUIERS_CONSTRUCTOR_PARAM);
        if (_routingProperty != _idProperty)
            validateGetterSetter(_routingProperty, "Routing", ConstructorPropertyValidation.REQUIERS_CONSTRUCTOR_PARAM);
        validateGetterSetter(_versionProperty, "Version", ConstructorPropertyValidation.REQUIERS_SETTER);
        validateGetterSetter(_persistProperty, "Persist", ConstructorPropertyValidation.REQUIERS_AT_LEAST_ONE);
        validateGetterSetter(_leaseExpirationProperty, "Lease expiration", ConstructorPropertyValidation.REQUIERS_AT_LEAST_ONE);

        for (int i = 0; i < _spaceProperties.length; i++) {
            // Already validated
            if (_spaceProperties[i] == _idProperty || _spaceProperties[i] == _routingProperty)
                continue;

            validateGetterSetter(_spaceProperties[i], "Space", ConstructorPropertyValidation.REQUIERS_CONSTRUCTOR_PARAM);
        }       
        
		/*
        if (_logger.isWarnEnabled() && _superTypeInfo != null)
			validateWarnings();
		*/
    }

    private void validatePropertyType(SpacePropertyInfo property, String attribute, Class<?> validType1, Class<?> validType2) {
        if (property != null && !validType1.isAssignableFrom(property.getType()) && !validType2.isAssignableFrom(property.getType()))
            throw new SpaceMetadataValidationException(_type, property, attribute + " property must be of type " + validType1.getSimpleName() + ".");
    }

    private void validateSequenceNumberProperty() {
        if (_sequenceNumberPropertyName == null)
            return;
        SpacePropertyInfo property = _properties.get(_sequenceNumberPropertyName);
        if (property == null)
            throw new SpaceMetadataValidationException(_type, "Type does not contain property [" + _sequenceNumberPropertyName + "] specified as sequence-number " + "].");
        if (!property.getTypeName().equals(Long.class.getName()) && !property.getTypeName().equals(long.class.getName()) && !property.getTypeName().equals(Object.class.getName()))
            throw new IllegalArgumentException("SpaceSequenceNumber property must be of type Long ,long or Object");

    }

    private void validatePropertyCombination(SpacePropertyInfo property1, SpacePropertyInfo property2,
                                             String property1Desc, String property2Desc) {
        if (property1 != null && property1 == property2)
            throw new SpaceMetadataValidationException(_type, property1, property1Desc + " and " + property2Desc + " cannot be used for the same property.");
    }

    private void validateStorageAdapterCombination() {
        if (_spaceClassStorageAdapter != null){
            for (Entry<String, SpacePropertyInfo> entry : _properties.entrySet()) {
                if(entry.getValue().getStorageAdapterClass()!= null){
                    throw new SpaceMetadataValidationException(_type, entry.getValue(), "class binary storage adapter and property storage adapter cannot be used together.");
                }
                if(entry.getValue().getStorageType() != null && !entry.getValue().getStorageType().equals(StorageType.DEFAULT) && !entry.getValue().getStorageType().equals(StorageType.OBJECT)){
                    throw new SpaceMetadataValidationException(_type, entry.getValue(), "class binary storage adapter and property non DEFAULT storage type cannot be used together.");
                }
            }
        }
    }

    private void validateBroadcastTable() {
        if(_superTypeInfo._superTypeInfo != null && !_superTypeInfo._superTypeInfo.isBroadcast())
            throw new SpaceMetadataValidationException(_type, "Broadcast table cannot extend non broadcast table.");
        if(_routingProperty != null)
            throw new SpaceMetadataValidationException(_type, "Routing property and broadcast table cannot be used together.");
        if(_idAutoGenerate)
            throw new SpaceMetadataValidationException(_type, "Auto generated id and broadcast table cannot be used together.");
    }

    private void validateGetterSetter(SpacePropertyInfo property, String propertyDesc, ConstructorPropertyValidation validation) {
        if (property != null) {
            if (property.getGetterMethod() == null)
                throw new SpaceMetadataValidationException(_type, property, propertyDesc + " property must have a getter.");

            final boolean hasSetterMethod = property.getSetterMethod() != null;
            if (hasConstructorProperties()) {
                if (validation == ConstructorPropertyValidation.REQUIERS_SETTER && !hasSetterMethod)
                    throw new SpaceMetadataValidationException(_type, property, propertyDesc + " property must have a setter.");
                boolean hasConstructorParameter = _constructorDescriptor.indexOfProperty(property.getName()) >= 0;

                if (validation == ConstructorPropertyValidation.REQUIERS_CONSTRUCTOR_PARAM && hasConstructorParameter && hasSetterMethod)
                    throw new SpaceMetadataValidationException(_type, property, propertyDesc + " property is ambiguous, property shouldn't be included in the constructor AND have a setter method.");

                if (validation == ConstructorPropertyValidation.REQUIERS_CONSTRUCTOR_PARAM && (!hasConstructorParameter && !hasSetterMethod))
                    throw new SpaceMetadataValidationException(_type, property, propertyDesc + " property must be included in the constructor or have a setter method.");
                // make sure at least one way to set the values exists
                if (validation == ConstructorPropertyValidation.REQUIERS_AT_LEAST_ONE &&
                        !hasSetterMethod && !hasConstructorParameter)
                    throw new SpaceMetadataValidationException(_type, property, propertyDesc + " property must have a setter or be included in the constructor.");
            } else {
                if (!hasSetterMethod)
                    throw new SpaceMetadataValidationException(_type, property, propertyDesc + " property must have a setter.");
            }
        }
    }

    private void validateWarnings() {
        // No space properties:
        if (_spaceProperties.length == 0) {
            _logger.warn("Type [" + _type.getName() + "] does not have any properties which can be saved in a space.");
            // Skip remaining warnings - no point bothering the user about them for now.
            return;
        }

        // No explicit routing/id:
        if (_routingProperty == null && (_idProperty == null || _idAutoGenerate))
            _logger.warn("Type [" + _type.getName() + "] does not define a routing property or an id property with autoGenerate=false - The routing property will be selected implicitly.\n"
                    + "Consider setting a routing property explicitly to avoid unexpected behaviour or errors.");

        if (_indexes.isEmpty())
            _logger.warn("Type [" + _type.getName() + "] does not have any indexes - this may impact queries performance.\n"
                    + "Consider indexing properties which participate in common queries to improve performance.");

		/* TODO: Validate indexes types.
		for (Entry<String, ISpaceIndex> entry : _indexes.entrySet())
		{
			ISpaceIndex index = entry.getValue();
			if (index.getType().isIndexed())
			{
		    	// Does the index has equals/hashCode
		    		SpacePropertyInfo property = _properties.get(index);
		    		validatePropertyTypeMethods(property, EQUAL_METHOD);
		    		validatePropertyTypeMethods(property, HASHCODE_METHOD);
			}
		}
		*/
    }

    public String getFullDescription() {
        StringBuilder sb = new StringBuilder();

        sb.append("SpaceTypeInfo for type [" + _type.getName() + "]:\n");
        sb.append("Super classes: " + concat(_superClasses) + ".\n");
        sb.append("Persist = [" + _persist + "]\n");
        sb.append("Replicate = [" + _replicate + "]\n");
        sb.append("blobstoreEnabled = [" + _blobstoreEnabled + "]\n");
        sb.append("FifoSupport = [" + _fifoSupport + "]\n");
        sb.append("Fifo grouping = [" + _fifoGroupingName + "]\n");
        sb.append("Fifo grouping indexes = [" + _fifoGroupingIndexes + "]\n");

        sb.append("InheritIndexes = [" + _inheritIndexes + "]\n");
        sb.append("IncludeProperties = [" + _includeProperties + "]\n");
        sb.append("Number of properties = [" + _properties.size() + "]\n");
        sb.append("Number of space properties = [" + _spaceProperties.length + "]\n");
        for (int i = 0; i < _spaceProperties.length; i++) {
            SpacePropertyInfo prop = _spaceProperties[i];
            sb.append("Property #" + i + " - Name: [" + prop.getName() +
                    "], NullValue: [" + prop.getNullValue() + "]\n");
        }
        sb.append("Id Property: [" + getPropertyName(_idProperty) + "], AutoGenerate: [" + _idAutoGenerate + "]\n");
        sb.append("Routing Property: [" + getPropertyName(_routingProperty) + "]\n");
        sb.append("Version Property: [" + getPropertyName(_versionProperty) + "]\n");
        sb.append("Persist Property: [" + getPropertyName(_persistProperty) + "]\n");
        sb.append("Lease Expiration Property: [" + getPropertyName(_leaseExpirationProperty) + "]\n");
        sb.append("Dynamic Properties Property: [" + getPropertyName(_dynamicPropertiesProperty) + "]\n");
        sb.append("End of info for type [" + _type.getName() + "].");

        return sb.toString();
    }

    private static String concat(String[] array) {
        if (array == null)
            return "[null]";
        if (array.length == 0)
            return "[]";

        String result = "[" + array[0];
        for (int i = 1; i < array.length; i++)
            result += "," + array[i];

        result = result + "]";

        return result;
    }

    private static String getPropertyName(SpacePropertyInfo property) {
        return property == null ? null : property.getName();
    }

    public Map<String, SpaceIndex> getIndexes() {
        return _indexes;
    }

    public Class<? extends ClassBinaryStorageAdapter> getSpaceClassStorageAdapter() {
        return _spaceClassStorageAdapter;
    }

    /////////////////////////////////////
    //		Serialization methods      //
    /////////////////////////////////////
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        readExternal(in, LRMIInvocationContext.getEndpointLogicalVersion());
    }

    public void readExternal(ObjectInput in, PlatformLogicalVersion version) throws IOException, ClassNotFoundException {
        if (version.greaterOrEquals(PlatformLogicalVersion.v10_1_0))
            readExternalV10_1(in, version);
        else if (version.greaterOrEquals(PlatformLogicalVersion.v10_0_0))
            readExternalV10_0(in, version);
        else if (version.greaterOrEquals(PlatformLogicalVersion.v9_6_0))
            readExternalV9_6(in, version);
        else
            readExternalV8_0(in);
    }

    private void readExternalV10_1(ObjectInput in, PlatformLogicalVersion version)
            throws IOException, ClassNotFoundException {
        readExternalV10_0(in, version);
        _sequenceNumberPropertyName = IOUtils.readString(in);
    }

    private void readExternalV10_0(ObjectInput in, PlatformLogicalVersion version)
            throws IOException, ClassNotFoundException {
        readExternalV9_6(in, version);
        _blobstoreEnabled = in.readBoolean();
    }

    private void readExternalV9_6(ObjectInput in, PlatformLogicalVersion version)
            throws IOException, ClassNotFoundException {
        // Read type name:
        String typeName = IOUtils.readString(in);
        // Load type:
        Class<?> type = ClassLoaderHelper.loadClass(typeName);

        // Read super type info, if any:
        SpaceTypeInfo superTypeInfo = null;
        boolean hasSuperTypeInfo = in.readBoolean();
        if (hasSuperTypeInfo) {
            superTypeInfo = new SpaceTypeInfo();
            superTypeInfo.readExternal(in, version);
        }

        // Initialize:
        initialize(type, superTypeInfo);

        // Read type attributes:
        _systemClass = in.readBoolean();
        _persist = in.readBoolean();
        _replicate = in.readBoolean();
        _fifoSupport = FifoHelper.fromCode(in.readByte());

        // Read constructor descriptor (if exists)
        _constructorDescriptor = IOUtils.readObject(in);

        // Read type special properties:
        _idProperty = readProperty(in);
        _idAutoGenerate = in.readBoolean();
        _routingProperty = readProperty(in);
        _versionProperty = readProperty(in);
        _persistProperty = readProperty(in);
        _leaseExpirationProperty = readProperty(in);
        _dynamicPropertiesProperty = readProperty(in);

        int numOfSpaceProperties = in.readInt();
        _spaceProperties = new SpacePropertyInfo[numOfSpaceProperties];
        for (int i = 0; i < numOfSpaceProperties; i++) {
            SpacePropertyInfo property = readProperty(in);
            String nullValue = IOUtils.readString(in);
            property.setNullValue(nullValue);
            //TODO ST - implement serialization for storage type
            _spaceProperties[i] = property;
        }

        final int length = in.readInt();
        if (length >= 0) {
            _indexes = new HashMap<String, SpaceIndex>(length);
            for (int i = 0; i < length; i++) {
                SpaceIndex index = IOUtils.readObject(in);
                _indexes.put(index.getName(), index);
            }
        }

        // set fields for setter-less properties
        if (hasConstructorProperties())
            extractFieldsForProperties();
        _blobstoreEnabled = PojoDefaults.BLOBSTORE_ENABLED;
    }

    private void readExternalV8_0(ObjectInput in)
            throws IOException, ClassNotFoundException {
        // Read type name:
        String typeName = IOUtils.readString(in);
        // Load type:
        Class<?> type = ClassLoaderHelper.loadClass(typeName);

        // Read super type info, if any:
        SpaceTypeInfo superTypeInfo = null;
        boolean hasSuperTypeInfo = in.readBoolean();
        if (hasSuperTypeInfo) {
            superTypeInfo = new SpaceTypeInfo();
            superTypeInfo.readExternal(in);
        }

        // Initialize:
        initialize(type, superTypeInfo);

        // Read type attributes:
        _systemClass = in.readBoolean();
        _persist = in.readBoolean();
        _replicate = in.readBoolean();
        _fifoSupport = FifoHelper.fromCode(in.readByte());

        // Read type special properties:
        _idProperty = readProperty(in);
        _idAutoGenerate = in.readBoolean();
        _routingProperty = readProperty(in);
        _versionProperty = readProperty(in);
        _persistProperty = readProperty(in);
        _leaseExpirationProperty = readProperty(in);
        _dynamicPropertiesProperty = readProperty(in);

        int numOfSpaceProperties = in.readInt();
        _spaceProperties = new SpacePropertyInfo[numOfSpaceProperties];
        for (int i = 0; i < numOfSpaceProperties; i++) {
            SpacePropertyInfo property = readProperty(in);
            String nullValue = IOUtils.readString(in);
            property.setNullValue(nullValue);
            //TODO ST - implement serialization for storage type
            _spaceProperties[i] = property;
        }

        final int length = in.readInt();
        if (length >= 0) {
            _indexes = new HashMap<String, SpaceIndex>(length);
            for (int i = 0; i < length; i++) {
                SpaceIndex index = IOUtils.readObject(in);
                _indexes.put(index.getName(), index);
            }
        }
        _blobstoreEnabled = PojoDefaults.BLOBSTORE_ENABLED;
    }

    private SpacePropertyInfo readProperty(ObjectInput in)
            throws IOException, ClassNotFoundException {
        final String propertyName = IOUtils.readString(in);
        if (propertyName == null)
            return null;

        SpacePropertyInfo property = _properties.get(propertyName);
        if (property == null) {
            throw new SpaceMetadataException("Error deserializing space type info for type [" + _type.getName() +
                    "] - the property [" + propertyName + "] does not exist in the class loaded in the server.");
        }
        return property;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        writeExternal(out, LRMIInvocationContext.getEndpointLogicalVersion());
    }

    public void writeExternal(ObjectOutput out, PlatformLogicalVersion version) throws IOException {
        if (version.greaterOrEquals(PlatformLogicalVersion.v10_1_0))
            writeExternalV10_1(out, version);
        else if (version.greaterOrEquals(PlatformLogicalVersion.v10_0_0))
            writeExternalV10_0(out, version);
        else if (version.greaterOrEquals(PlatformLogicalVersion.v9_6_0))
            writeExternalV9_6(out, version);
        else
            writeExternalV8_0(out);
    }

    private void writeExternalV10_1(ObjectOutput out, PlatformLogicalVersion version)
            throws IOException {
        writeExternalV10_0(out, version);  //same prefix
        IOUtils.writeString(out, _sequenceNumberPropertyName);
    }

    private void writeExternalV10_0(ObjectOutput out, PlatformLogicalVersion version)
            throws IOException {
        writeExternalV9_6(out, version);  //same prefix
        out.writeBoolean(_blobstoreEnabled);
    }

    private void writeExternalV9_6(ObjectOutput out, PlatformLogicalVersion version)
            throws IOException {
        // Write type name:
        IOUtils.writeString(out, _type.getName());

        // Write super type info, if any:
        if (_superTypeInfo == null)
            out.writeBoolean(false);
        else {
            out.writeBoolean(true);
            _superTypeInfo.writeExternal(out, version);
        }

        // Write type attributes:
        out.writeBoolean(_systemClass);
        out.writeBoolean(_persist);
        out.writeBoolean(_replicate);
        out.writeByte(FifoHelper.toCode(_fifoSupport));

        // Write constructor descriptor if exists.
        // For serialized instances, a non null descriptor
        // in an indicator for constructor properties
        IOUtils.writeObject(out, _constructorDescriptor);

        // Write type special properties:
        IOUtils.writeString(out, getPropertyName(_idProperty));
        out.writeBoolean(_idAutoGenerate);
        IOUtils.writeString(out, getPropertyName(_routingProperty));
        IOUtils.writeString(out, getPropertyName(_versionProperty));
        IOUtils.writeString(out, getPropertyName(_persistProperty));
        IOUtils.writeString(out, getPropertyName(_leaseExpirationProperty));
        IOUtils.writeString(out, getPropertyName(_dynamicPropertiesProperty));

        // Write space properties:
        out.writeInt(_spaceProperties.length);
        for (int i = 0; i < _spaceProperties.length; i++) {
            SpacePropertyInfo property = _spaceProperties[i];
            IOUtils.writeString(out, property.getName());
            String nullValue = property.hasNullValue() ? property.getNullValue().toString() : null;
            IOUtils.writeString(out, nullValue);
            //TODO ST - implement serialization of storage type
        }

        final int length = _indexes == null ? -1 : _indexes.size();
        out.writeInt(length);
        if (length > 0)
            for (Entry<String, SpaceIndex> entry : _indexes.entrySet())
                IOUtils.writeObject(out, entry.getValue());
    }

    private void writeExternalV8_0(ObjectOutput out)
            throws IOException {
        // Write type name:
        IOUtils.writeString(out, _type.getName());

        // Write super type info, if any:
        if (_superTypeInfo == null)
            out.writeBoolean(false);
        else {
            out.writeBoolean(true);
            _superTypeInfo.writeExternal(out);
        }

        // Write type attributes:
        out.writeBoolean(_systemClass);
        out.writeBoolean(_persist);
        out.writeBoolean(_replicate);
        out.writeByte(FifoHelper.toCode(_fifoSupport));

        // Write type special properties:
        IOUtils.writeString(out, getPropertyName(_idProperty));
        out.writeBoolean(_idAutoGenerate);
        IOUtils.writeString(out, getPropertyName(_routingProperty));
        IOUtils.writeString(out, getPropertyName(_versionProperty));
        IOUtils.writeString(out, getPropertyName(_persistProperty));
        IOUtils.writeString(out, getPropertyName(_leaseExpirationProperty));
        IOUtils.writeString(out, getPropertyName(_dynamicPropertiesProperty));

        // Write space properties:
        out.writeInt(_spaceProperties.length);
        for (int i = 0; i < _spaceProperties.length; i++) {
            SpacePropertyInfo property = _spaceProperties[i];
            IOUtils.writeString(out, property.getName());
            String nullValue = property.hasNullValue() ? property.getNullValue().toString() : null;
            IOUtils.writeString(out, nullValue);
            //TODO ST - implement serialization of storage type
        }

        final int length = _indexes == null ? -1 : _indexes.size();
        out.writeInt(length);
        if (length > 0)
            for (Entry<String, SpaceIndex> entry : _indexes.entrySet())
                IOUtils.writeObject(out, entry.getValue());
    }

    private class InitContext {
        public final Map<String, SpaceIndex> indexedProperties = new HashMap<String, SpaceIndex>();
        public final Set<SpacePropertyInfo> explicitlyIncluded = new HashSet<SpacePropertyInfo>();
        public final Set<SpacePropertyInfo> explicitlyExcluded = new HashSet<SpacePropertyInfo>();

        public void addIndex(String name, SpaceIndexType indexType) {
            addIndex(name, indexType, false);
        }

        public void addIndex(String name, SpaceIndexType indexType, boolean unique) {
            if (indexedProperties.containsKey(name))
                throw new SpaceMetadataValidationException(_type, "Duplicate index definition. Index with the name ["
                        + name + "] is already defined.");
            indexedProperties.put(name, SpaceIndexFactory.createPropertyIndex(name, indexType, unique));
        }
    }
}
