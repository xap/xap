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

package com.gigaspaces.serialization.pbs;

import com.gigaspaces.annotation.pojo.FifoSupport;
import com.gigaspaces.document.DocumentProperties;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.metadata.ObjectType;
import com.gigaspaces.internal.client.spaceproxy.metadata.TypeDescFactory;
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.FifoHelper;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.metadata.SpaceIdType;
import com.gigaspaces.internal.metadata.SpaceIndexTypeHelper;
import com.gigaspaces.internal.metadata.SpacePropertyInfo;
import com.gigaspaces.internal.metadata.SpaceTypeInfo;
import com.gigaspaces.internal.metadata.SpaceTypeInfoRepository;
import com.gigaspaces.internal.metadata.converter.ConversionException;
import com.gigaspaces.internal.transport.AbstractProjectionTemplate;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.transport.PbsEntryPacket;
import com.gigaspaces.internal.transport.PbsProjectionTemplate;
import com.gigaspaces.internal.transport.PbsTemplatePacket;
import com.gigaspaces.internal.transport.TemplatePacketFactory;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.metadata.SpaceDocumentSupport;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.gigaspaces.metadata.StorageType;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexFactory;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.gigaspaces.metadata.index.SpacePropertyIndex;
import com.gigaspaces.serialization.BinaryObject;
import com.gigaspaces.serialization.pbs.collections.PbsCustomTypeList;
import com.gigaspaces.serialization.pbs.collections.PbsCustomTypeMap;
import com.j_spaces.core.IGSEntry;
import com.j_spaces.core.client.ExternalEntry;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.core.exception.internal.PBSInternalSpaceException;
import com.j_spaces.jdbc.builder.SQLQueryTemplatePacket;
import com.j_spaces.kernel.ClassLoaderHelper;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.RandomAccess;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides PBS services for writing/reading Entries to/from the space.
 *
 * @author Niv
 * @since 6.1
 */
@com.gigaspaces.api.InternalApi
public class PbsEntryFormatter {
    private static final Logger _logger = Logger.getLogger(Constants.LOGGER_COMMON);

    private static final short StreamVersion = 1;

    public static final int BITMAP_TRANSIENT = 1 << 0;
    private static final int BITMAP_UID_AUTOGENERATE = 1 << 1;
    public static final int BITMAP_FIFO = 1 << 2;
    public static final int BITMAP_REPLICATE = 1 << 3;
    public static final int BITMAP_METADATA = 1 << 4;
    private static final int BITMAP_SUPPORTS_OPTIMISTIC_LOCKING = 1 << 5;
    private static final int BITMAP_SUPPORTS_DYNAMIC_PROPERTIES = 1 << 6;

    public static void writeExternalEntry(PbsOutputStream output, IGSEntry entry) {
        writeEntry(output, entry.getFieldsValues(),
                entry.getClassName(), entry.getUID(), entry.getVersion(),
                entry.isTransient(), entry.isFifo(), entry.isReplicatable());
    }

    public static void writeEntry(PbsOutputStream output, Object[] values, String className, String uid, int version, boolean isTransient, boolean isFifo, boolean isReplicatable) {
        // Write PBS Version:
        output.writeShort(StreamVersion);
        // Write ClassName:
        output.writeString(className);
        // Write UID:
        if (uid != null)
            output.writeString(uid);
        else
            output.writeString("");
        // Write Version:
        output.writeInt(version);

        // Special Flags i.e. isFifo etc
        int specialFlags = BITMAP_UID_AUTOGENERATE; // set to BITMAP_UidAutoGenerate to cause a valid UID is sent
        if (isTransient)
            specialFlags |= BITMAP_TRANSIENT;
        if (isFifo)
            specialFlags |= BITMAP_FIFO;
        if (isReplicatable)
            specialFlags |= BITMAP_REPLICATE;
        output.writeInt(specialFlags);

        // Write routing field value:
        // Note: This field is only relevant when .Net/CPP sends requests to the client for routing.
        output.writeByte(PbsTypeInfo.NULL);

        if (values != null) {
            // Write number of values:
            output.writeInt(values.length);
            // Write values:
            for (int i = 0; i < values.length; i++)
                writeFieldValue(output, values[i]);
        } else {
            output.writeInt(0);
        }
    }

    public static ITypeDesc readTypeDescriptor(PbsInputStream input) {
        String typeName = input.readString();
        EntryType entryType = readEntryType(input);
        String[] superTypesNames = input.readStringArray();
        PropertyInfo[] propertiesInfo = readTypeProperties(input);
        boolean supportsDynamicProperties = input.readBoolean();
        byte dynamicPropertiesStorageType = input.readByte();
        String documentWrapperType = input.readString();
        String idPropertyName = input.readString();
        boolean idAutoGenerate = input.readBoolean();
        String routingPropertyName = input.readString();
        Map<String, SpaceIndex> indexes = readIndexes(input, idPropertyName, idAutoGenerate);
        FifoSupport fifoMode = FifoHelper.fromCode(input.readByte());
        boolean replicable = input.readBoolean();
        boolean supportsOptimisticLocking = input.readBoolean();
        String fifoGroupingPropertyPath = input.readString();
        Set<String> fifoGroupingIndexPaths = readFifoGroupingIndexPaths(input);
        //Generate type descriptor
        ITypeDesc typeDesc = TypeDescFactory.createPbsExplicitTypeDesc(entryType,
                typeName,
                superTypesNames,
                propertiesInfo,
                indexes,
                idPropertyName,
                idAutoGenerate,
                routingPropertyName,
                fifoGroupingPropertyPath,
                fifoGroupingIndexPaths,
                fifoMode,
                replicable,
                supportsOptimisticLocking,
                supportsDynamicProperties,
                dynamicPropertiesStorageType,
                documentWrapperType,
//TBD  pbs support for offHeapConfig per class               
                true /*blobstoreEnabled*/);
        return typeDesc;
    }

    private static Set<String> readFifoGroupingIndexPaths(PbsInputStream input) {
        int numOfGigoGroupingIndexes = input.readInt();
        if (numOfGigoGroupingIndexes < 0)
            return null;
        Set<String> fifoGroupingIndexPaths = new HashSet<String>();
        for (int i = 0; i < numOfGigoGroupingIndexes; i++) {
            fifoGroupingIndexPaths.add(input.readString());
        }
        return fifoGroupingIndexPaths;
    }

    private static PropertyInfo[] readTypeProperties(PbsInputStream input) {
        int length = input.readInt();
        PropertyInfo[] properties = new PropertyInfo[length];
        for (int i = 0; i < properties.length; i++) {
            String name = input.readString();
            String typeName = input.readString();
            byte dotNetStorageType = input.readByte();
            properties[i] = PropertyInfo.builder(name)
                    .type(typeName)
                    .storageType(StorageType.OBJECT)
                    .dotNetStorageType(dotNetStorageType)
                    .build();
        }
        return properties;
    }

    private static final byte INDEX_PROPERTY = 0;
    private static final byte INDEX_PATH = 1;
    private static final byte COMPOUND_INDEX = 2;

    private static Map<String, SpaceIndex> readIndexes(PbsInputStream input, String idPropertyName, boolean idAutoGenerate) {
        int length = input.readInt();
        if (length == -1)
            return null;

        Map<String, SpaceIndex> indexes = new HashMap<String, SpaceIndex>();
        for (int i = 0; i < length; i++) {
            String indexPath = input.readString();
            SpaceIndexType indexType = SpaceIndexTypeHelper.fromCode(input.readByte());
            boolean unique = input.readBoolean();
            byte indexPathType = input.readByte();
            SpaceIndex index;
            switch (indexPathType) {
                case INDEX_PROPERTY:
                    int propertyPos = input.readInt();
                    index = new SpacePropertyIndex(indexPath, indexType, unique, propertyPos);
                    break;
                case INDEX_PATH:
                    index = SpaceIndexFactory.createPathIndex(indexPath, indexType);
                    break;
                case COMPOUND_INDEX:
                    String[] paths = input.readStringArray();
                    index = SpaceIndexFactory.createCompoundIndex(paths);
                    break;
                default:
                    throw new PBSInternalSpaceException("Unexpected index path type byte [" + indexPathType + "]");
            }
            indexes.put(indexPath, index);
        }
        return indexes;
    }

    public static ITypeDesc readTypeDescIfExists(PbsInputStream input, String typeName, EntryType entryType) {
        final int flags = input.readInt();
        final boolean hasTypeDesc = ((flags & PbsEntryFormatter.BITMAP_METADATA) != 0);
        return hasTypeDesc ? readTypeDesc(input, typeName, entryType, flags) : null;
    }

    public static ITypeDesc readTypeDesc(PbsInputStream input, String typeName, EntryType entryType, int flags) {
        boolean isFifo = ((flags & BITMAP_FIFO) != 0);
        boolean isReplicable = ((flags & BITMAP_REPLICATE) != 0);
        boolean isAutoGen = ((flags & BITMAP_UID_AUTOGENERATE) != 0);
        boolean supportsOptimisticLocking = ((flags & BITMAP_SUPPORTS_OPTIMISTIC_LOCKING) != 0);
        boolean supportsDynamicProperties = ((flags & BITMAP_SUPPORTS_DYNAMIC_PROPERTIES) != 0);
        FifoSupport fifoMode = FifoHelper.fromOld(isFifo);

        // Read super class names:
        String[] superClassesNames = input.readStringArray();
        // Read fifo mode:
        // Read fields metadata:
        int fields = input.readInt();
        String[] fieldsNames = new String[fields];
        String[] fieldsTypes = new String[fields];
        SpaceIndexType[] fieldsIndexes = new SpaceIndexType[fields];
        for (int i = 0; i < fields; i++) {
            fieldsNames[i] = input.readString();
            fieldsTypes[i] = input.readString();
            fieldsIndexes[i] = SpaceIndexTypeHelper.fromCode(input.readByte());
        }
        // Read uid field name:
        String uidFieldName = input.readString();
        // Read routing field name:
        String routingFieldName = input.readString();

        return TypeDescFactory.createPbsTypeDesc(entryType, typeName, null /* codeBase */, superClassesNames,
                fieldsNames, fieldsTypes, fieldsIndexes, uidFieldName, isAutoGen, routingFieldName, fifoMode,
                isReplicable, supportsOptimisticLocking, supportsDynamicProperties);
    }

    /**
     * Writes a pbs entry packet to the stream
     */
    public static void writePbsEntryPacket(PbsOutputStream output, PbsEntryPacket pbsEntry) {
        byte[] array = pbsEntry.getStreamBytes();
        output.write(array, 0, array.length);
        writeDynamicProperties(output, pbsEntry.getDynamicProperties());
    }

    public static void writeDynamicProperties(PbsOutputStream output,
                                              Map<String, Object> dynamicProperties) {
        if (dynamicProperties == null || dynamicProperties.isEmpty()) {
            output.writeInt(-1);
            return;
        }
        output.writeInt(dynamicProperties.size());
        for (Entry<String, Object> dynamicProperty : dynamicProperties.entrySet()) {
            output.writeRepetitiveString(dynamicProperty.getKey());
            writeFieldValue(output, dynamicProperty.getValue());
        }
    }

    public static void writePbsEntryPacketLazy(PbsOutputStream output,
                                               PbsEntryPacket packet) {
        if (packet == null) {
            output.writeIntFixed(-1);
            return;
        }
        int startPosition = output.size();
        output.writeIntFixed(0);
        byte[] array = packet.getStreamBytes();
        output.write(array, 0, array.length);
        writeDynamicProperties(output, packet.getDynamicProperties());
        int endPosition = output.size();
        int length = endPosition - startPosition - 4;
        output.setSize(startPosition);
        output.writeIntFixed(length);
        output.setSize(endPosition);
    }

    /**
     * Writes a pbs entry packet to the stream
     */
    public static void writeNullablePbsEntryPacket(PbsOutputStream output, PbsEntryPacket pbsEntry) {
        if (pbsEntry != null) {
            output.writeBoolean(true);
            PbsEntryFormatter.writePbsEntryPacket(output, pbsEntry);
        } else
            output.writeBoolean(false);
    }


    /**
     * Write an object array of pbs entries to the stream
     */
    public static void writePbsEntryPacketObjectArray(PbsOutputStream output, Object[] objects) {
        if (objects != null) {
            output.writeInt(objects.length);
            for (Object entry : objects)
                writePbsEntryPacket(output, (PbsEntryPacket) entry);
        } else
            output.writeInt(-1);
    }

    /**
     * Write an object array of nullable pbs entries to the stream
     */
    public static void writeNullablePbsEntryPacketObjectArray(PbsOutputStream output, Object[] objects) {
        if (objects != null) {
            output.writeInt(objects.length);
            for (Object entry : objects)
                writeNullablePbsEntryPacket(output, (PbsEntryPacket) entry);
        } else
            output.writeInt(-1);
    }

    public static void writeFieldValue(PbsOutputStream output, Object value) {
        // Get value info:
        PbsTypeInfo typeInfo = PbsTypeInfo.getTypeInfo(value);

        // Write value type code:
        output.writeByte(typeInfo.typeCode);

        switch (typeInfo.typeCode) {
            case PbsTypeInfo.NULL:
                break;
            // Scalars:
            case PbsTypeInfo.BYTE:
            case PbsTypeInfo.BYTE_WRAPPER:
                output.writeByte((Byte) value);
                break;
            case PbsTypeInfo.SHORT:
            case PbsTypeInfo.SHORT_WRAPPER:
                output.writeInt(((Short) value).intValue());
                break;
            case PbsTypeInfo.INTEGER:
            case PbsTypeInfo.INTEGER_WRAPPER:
                output.writeInt((Integer) value);
                break;
            case PbsTypeInfo.LONG:
            case PbsTypeInfo.LONG_WRAPPER:
                output.writeLong((Long) value);
                break;
            case PbsTypeInfo.FLOAT:
            case PbsTypeInfo.FLOAT_WRAPPER:
                output.writeFloat((Float) value);
                break;
            case PbsTypeInfo.DOUBLE:
            case PbsTypeInfo.DOUBLE_WRAPPER:
                output.writeDouble((Double) value);
                break;
            case PbsTypeInfo.BOOLEAN:
            case PbsTypeInfo.BOOLEAN_WRAPPER:
                output.writeBoolean((Boolean) value);
                break;
            case PbsTypeInfo.CHAR:
            case PbsTypeInfo.CHAR_WRAPPER:
                output.writeChar(((Character) value));
                break;
            case PbsTypeInfo.STRING:
                output.writeString((String) value);
                break;
            case PbsTypeInfo.DATE:
                output.writeDateTime((Date) value);
                break;
            case PbsTypeInfo.DECIMAL:
                output.writeDecimal((BigDecimal) value);
                break;
            case PbsTypeInfo.UUID:
                output.writeUUID((UUID) value);
                break;
            // Hard-coded arrays:
            case PbsTypeInfo.BYTE_ARRAY:
                output.writeByteArray((byte[]) value);
                break;
            case PbsTypeInfo.SHORT_ARRAY:
                output.writeShortArray((short[]) value);
                break;
            case PbsTypeInfo.INTEGER_ARRAY:
                output.writeIntArray((int[]) value);
                break;
            case PbsTypeInfo.LONG_ARRAY:
                output.writeLongArray((long[]) value);
                break;
            case PbsTypeInfo.FLOAT_ARRAY:
                output.writeFloatArray((float[]) value);
                break;
            case PbsTypeInfo.DOUBLE_ARRAY:
                output.writeDoubleArray((double[]) value);
                break;
            case PbsTypeInfo.BOOLEAN_ARRAY:
                output.writeBooleanArray((boolean[]) value);
                break;
            case PbsTypeInfo.CHAR_ARRAY:
                output.writeCharArray((char[]) value);
                break;
            case PbsTypeInfo.STRING_ARRAY:
                output.writeStringArray((String[]) value);
                break;
            // Arrays:
            case PbsTypeInfo.ARRAY:
                writeArray(output, value, typeInfo);
                break;
            // Lists:
            case PbsTypeInfo.LIST_CUSTOM_TYPE:
            case PbsTypeInfo.LIST_ARRAY:
            case PbsTypeInfo.LIST_ARRAY_GENERIC:
                writeList(output, typeInfo, (List<?>) value);
                break;
            case PbsTypeInfo.MAP_CUSTOM_TYPE:
            case PbsTypeInfo.MAP_HASH:
            case PbsTypeInfo.MAP_HASH_GENERIC:
            case PbsTypeInfo.MAP_LINKED:
            case PbsTypeInfo.MAP_LINKED_GENERIC:
            case PbsTypeInfo.MAP_TREE:
            case PbsTypeInfo.MAP_TREE_GENERIC:
            case PbsTypeInfo.MAP_PROPERTIES:
                writeMap(output, typeInfo, (Map<?, ?>) value);
                break;
            // Objects:
            case PbsTypeInfo.OBJECT:
                writePbsObject(output, value);
                break;
            case PbsTypeInfo.DOCUMENT:
                writeDocumentObject(output, (SpaceDocument) value);
                break;
            // Binary data:
            case PbsTypeInfo.BINARY:
                output.writeBinaryObject((BinaryObject) value);
                break;
            case PbsTypeInfo.DOCUMENT_PROPERTIES:
                writeDocumentProperties(output, (DocumentProperties) value);
                break;
            default:
                throw new PBSInternalSpaceException("PBS failed on write due to unsupported type code: " + typeInfo.typeCode);
        }
    }

    private static void writeDocumentProperties(PbsOutputStream output,
                                                DocumentProperties value) {
        output.writeInt(value.size());
        for (Entry<String, Object> documentProperty : value.entrySet()) {
            output.writeRepetitiveString(documentProperty.getKey());
            writeFieldValue(output, documentProperty.getValue());
        }
    }

    public static Object readFieldValue(PbsInputStream input) {
        return readFieldValue(input, null);
    }

    public static Object readFieldValue(PbsInputStream input, ISpaceProxy contextSpaceProxy) {
        byte code = input.readByte();

        switch (code) {
            case PbsTypeInfo.NULL:
                return null;
            // Scalars:
            case PbsTypeInfo.BYTE:
            case PbsTypeInfo.BYTE_WRAPPER:
                return input.readByte();
            case PbsTypeInfo.SHORT:
            case PbsTypeInfo.SHORT_WRAPPER:
                return input.readShort();
            case PbsTypeInfo.INTEGER:
            case PbsTypeInfo.INTEGER_WRAPPER:
                return input.readInt();
            case PbsTypeInfo.LONG:
            case PbsTypeInfo.LONG_WRAPPER:
                return input.readLong();
            case PbsTypeInfo.FLOAT:
            case PbsTypeInfo.FLOAT_WRAPPER:
                return input.readFloat();
            case PbsTypeInfo.DOUBLE:
            case PbsTypeInfo.DOUBLE_WRAPPER:
                return input.readDouble();
            case PbsTypeInfo.BOOLEAN:
            case PbsTypeInfo.BOOLEAN_WRAPPER:
                return input.readBoolean();
            case PbsTypeInfo.CHAR:
            case PbsTypeInfo.CHAR_WRAPPER:
                return input.readChar();
            case PbsTypeInfo.STRING:
                return input.readString();
            case PbsTypeInfo.DATE:
                return input.readDateTime();
            case PbsTypeInfo.DECIMAL:
                return input.readDecimal();
            case PbsTypeInfo.UUID:
                return input.readUUID();
            // Hard-coded arrays:
            case PbsTypeInfo.BYTE_ARRAY:
                return input.readByteArray();
            case PbsTypeInfo.SHORT_ARRAY:
                return input.readShortArray();
            case PbsTypeInfo.INTEGER_ARRAY:
                return input.readIntArray();
            case PbsTypeInfo.LONG_ARRAY:
                return input.readLongArray();
            case PbsTypeInfo.FLOAT_ARRAY:
                return input.readFloatArray();
            case PbsTypeInfo.DOUBLE_ARRAY:
                return input.readDoubleArray();
            case PbsTypeInfo.BOOLEAN_ARRAY:
                return input.readBoolArray();
            case PbsTypeInfo.CHAR_ARRAY:
                return input.readCharArray();
            case PbsTypeInfo.STRING_ARRAY:
                return input.readStringArray();
            // Arrays:
            case PbsTypeInfo.ARRAY:
                return readArray(input, contextSpaceProxy);
            // Lists:
            case PbsTypeInfo.LIST_CUSTOM_TYPE:
            case PbsTypeInfo.LIST_ARRAY:
            case PbsTypeInfo.LIST_ARRAY_GENERIC:
                return readList(input, code, contextSpaceProxy);
            // Maps:
            case PbsTypeInfo.MAP_CUSTOM_TYPE:
            case PbsTypeInfo.MAP_HASH:
            case PbsTypeInfo.MAP_HASH_GENERIC:
            case PbsTypeInfo.MAP_LINKED:
            case PbsTypeInfo.MAP_LINKED_GENERIC:
            case PbsTypeInfo.MAP_TREE:
            case PbsTypeInfo.MAP_TREE_GENERIC:
            case PbsTypeInfo.MAP_PROPERTIES:
                return readMap(input, code, contextSpaceProxy);
            // Object:
            case PbsTypeInfo.OBJECT:
                return readPbsObject(input, contextSpaceProxy);
            case PbsTypeInfo.DOCUMENT:
                return readDocumentObject(input, contextSpaceProxy);
            // Binary data:
            case PbsTypeInfo.BINARY:
                return input.readBinaryObject();
            case PbsTypeInfo.DOCUMENT_PROPERTIES:
                return readDocumentProperties(input, contextSpaceProxy);
            default:
                throw new PBSInternalSpaceException("PBS failed on read due to unsupported type code: " + code);
        }
    }

    private static DocumentProperties readDocumentProperties(PbsInputStream input, ISpaceProxy contextSpaceProxy) {
        int length = input.readInt();
        DocumentProperties properties = new DocumentProperties(length);
        for (int i = 0; i < length; i++) {
            String propertyName = input.readRepetitiveString();
            Object value = readFieldValue(input, contextSpaceProxy);
            properties.put(propertyName, value);
        }
        return properties;
    }

    @SuppressWarnings("deprecation")
    private static void writePbsObject(PbsOutputStream output, Object value) {
        // Get type info:
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getGlobalRepository().getByType(value.getClass());
        // Extract properties values:
        int length = typeInfo.getNumOfSpaceProperties();
        Object[] values = new Object[length];
        for (int i = 0; i < length; i++) {
            SpacePropertyInfo propertyInfo = typeInfo.getProperty(i);
            Object propertyValue = propertyInfo.getValue(value);
            values[i] = propertyInfo.convertToNullIfNeeded(propertyValue);
        }

        // Create ExternalEntry:
        ExternalEntry entry = new ExternalEntry(typeInfo.getName(), values);
        // Now call back in to caller as we are at a new level.
        writeExternalEntry(output, entry);
    }

    @SuppressWarnings("deprecation")
    private static Object readPbsObject(PbsInputStream input, ISpaceProxy contextSpaceProxy) {
        final PbsEntryPacket component = new PbsEntryPacket(input, null, null);
        final String typeName = component.getTypeName();
        final Object[] values = component.getFieldValues();

        SpaceTypeInfo typeInfo;
        // Get type info:
        try {
            typeInfo = SpaceTypeInfoRepository.getGlobalRepository().getByName(typeName);
        } catch (SpaceMetadataException e) {
            if (contextSpaceProxy == null)
                throw e;
            try {
                //Attempt to load the class from the remote space if available
                contextSpaceProxy.loadRemoteClass(typeName);
            } catch (ClassNotFoundException cnfe) {
                //Rethrow original exception
                throw e;
            }
            // Retry attempt to get type info after we have successfully loaded the class
            typeInfo = SpaceTypeInfoRepository.getGlobalRepository().getByName(typeName);
        }
        // Create instance:
        Object obj = typeInfo.createInstance();
        // Copy properties values from ExternalEntry to Pojo:
        int length = typeInfo.getNumOfSpaceProperties();
        for (int i = 0; i < length; i++) {
            SpacePropertyInfo propertyInfo = typeInfo.getProperty(i);
            propertyInfo.setValue(obj, propertyInfo.convertFromNullIfNeeded(values[i]));
        }
        // Return result:
        return obj;
    }

    private static void writeDocumentObject(PbsOutputStream output,
                                            SpaceDocument value) {
        // Write class name:
        output.writeString(value.getTypeName());
        Map<String, Object> properties = value.getProperties();
        // Write properties:
        output.writeInt(properties.size());
        for (Entry<String, Object> iterable_element : properties.entrySet()) {
            output.writeString(iterable_element.getKey());
            writeFieldValue(output, iterable_element.getValue());
        }
    }

    private static SpaceDocument readDocumentObject(PbsInputStream input, ISpaceProxy contextSpaceProxy) {
        // Read ClassName:
        String className = input.readString();
        SpaceDocument document = new SpaceDocument(className);
        // Read properties:
        int length = input.readInt();
        for (int i = 0; i < length; i++) {
            String propertyName = input.readString();
            Object value = readFieldValue(input, contextSpaceProxy);
            document.setProperty(propertyName, value);
        }
        return document;
    }

    private static void writeArray(PbsOutputStream output, Object value, PbsTypeInfo arrayTypeInfo) {
        // Write array's component type code:
        // (hard coded arrays have different code and do not use this info)
        if (arrayTypeInfo.typeCode == PbsTypeInfo.ARRAY)
            output.writeByte(arrayTypeInfo.componentType.typeCode);

        switch (arrayTypeInfo.componentType.typeCode) {
            // Primitive wrappers arrays:
            case PbsTypeInfo.BYTE_WRAPPER:
                output.writeByteWrapperArray((Byte[]) value);
                break;
            case PbsTypeInfo.SHORT_WRAPPER:
                output.writeShortWrapperArray((Short[]) value);
                break;
            case PbsTypeInfo.INTEGER_WRAPPER:
                output.writeIntegerWrapperArray((Integer[]) value);
                break;
            case PbsTypeInfo.LONG_WRAPPER:
                output.writeLongWrapperArray((Long[]) value);
                break;
            case PbsTypeInfo.FLOAT_WRAPPER:
                output.writeFloatWrapperArray((Float[]) value);
                break;
            case PbsTypeInfo.DOUBLE_WRAPPER:
                output.writeDoubleWrapperArray((Double[]) value);
                break;
            case PbsTypeInfo.BOOLEAN_WRAPPER:
                output.writeBooleanWrapperArray((Boolean[]) value);
                break;
            case PbsTypeInfo.CHAR_WRAPPER:
                output.writeCharWrapperArray((Character[]) value);
                break;
            // System arrays:
            case PbsTypeInfo.DATE:
                output.writeDateTimeArray((Date[]) value);
                break;
            case PbsTypeInfo.DECIMAL:
                output.writeDecimalArray((BigDecimal[]) value);
                break;
            case PbsTypeInfo.UUID:
                output.writeUUIDArray((UUID[]) value);
                break;
            case PbsTypeInfo.DOCUMENT_PROPERTIES:
                writeDocumentPropertiesArray(output, (DocumentProperties[]) value);
                break;
            case PbsTypeInfo.OBJECT:
                writePbsObjectArray(output, (Object[]) value);
                break;
            case PbsTypeInfo.DOCUMENT:
                writeDocumentObjectComponentArray(output, (SpaceDocument[]) value);
                break;
            default:
                throw new PBSInternalSpaceException("PBS failed on array write due to unsupported type code: " + arrayTypeInfo.componentType.typeCode);
        }
    }

    private static Object readArray(PbsInputStream input, ISpaceProxy contextSpaceProxy) {
        // Read component type code:
        byte componentTypeCode = input.readByte();

        // Read array's components:
        switch (componentTypeCode) {
            // Read primitive types:
            //case PbsTypeInfo.BYTE:				return PbsFormatter.readByteArray(input);
            //case PbsTypeInfo.SHORT:				return PbsFormatter.readShortArray(input);
            //case PbsTypeInfo.INTEGER:			return PbsFormatter.readIntArray(input);
            //case PbsTypeInfo.LONG:				return PbsFormatter.readLongArray(input);
            //case PbsTypeInfo.FLOAT:				return PbsFormatter.readFloatArray(input);
            //case PbsTypeInfo.DOUBLE:			return PbsFormatter.readDoubleArray(input);
            //case PbsTypeInfo.BOOLEAN:			return PbsFormatter.readBoolArray(input);
            //case PbsTypeInfo.CHAR: 				return PbsFormatter.readCharArray(input);
            // Read primitive wrapper arrays:
            case PbsTypeInfo.BYTE_WRAPPER:
                return input.readByteWrapperArray();
            case PbsTypeInfo.SHORT_WRAPPER:
                return input.readShortWrapperArray();
            case PbsTypeInfo.INTEGER_WRAPPER:
                return input.readIntegerWrapperArray();
            case PbsTypeInfo.LONG_WRAPPER:
                return input.readLongWrapperArray();
            case PbsTypeInfo.FLOAT_WRAPPER:
                return input.readFloatWrapperArray();
            case PbsTypeInfo.DOUBLE_WRAPPER:
                return input.readDoubleWrapperArray();
            case PbsTypeInfo.BOOLEAN_WRAPPER:
                return input.readBooleanWrapperArray();
            case PbsTypeInfo.CHAR_WRAPPER:
                return input.readCharWrapperArray();
            // Read system types:
            //case PbsTypeInfo.STRING:			return PbsFormatter.readStringArray(input);
            case PbsTypeInfo.DATE:
                return input.readDateTimeArray();
            case PbsTypeInfo.DECIMAL:
                return input.readDecimalArray();
            case PbsTypeInfo.UUID:
                return input.readUUIDArray();
            case PbsTypeInfo.DOCUMENT_PROPERTIES:
                return readDocumentPropertiesArray(input, contextSpaceProxy);
            case PbsTypeInfo.OBJECT:
                return readPbsObjectArray(input, contextSpaceProxy);
            case PbsTypeInfo.DOCUMENT:
                return readDocumentObjectComponentArray(input, contextSpaceProxy);
            default:
                throw new PBSInternalSpaceException("PBS failed on array read due to unsupported type code: " + componentTypeCode);
        }
    }

    private static void writeDocumentObjectComponentArray(PbsOutputStream output,
                                                          SpaceDocument[] array) {
        // Array's type
        String arrName = array.length == 0 ? Object.class.getName() : array[0].getTypeName();
        output.writeString(arrName);
        // Array length and items
        output.writeInt(array.length);
        for (int i = 0; i < array.length; i++)
            writeFieldValue(output, array[i]);
    }

    private static SpaceDocument[] readDocumentObjectComponentArray(PbsInputStream input, ISpaceProxy contextSpaceProxy) {
        //We have to read this because the corresponding writeDocumentObjectComponentArray
        //writes this so the .NET knows which array type to instantiate
        String typeName = input.readString();
        int length = input.readInt();
        SpaceDocument[] retValue = new SpaceDocument[length];

        for (int i = 0; i < length; i++)
            retValue[i] = (SpaceDocument) readFieldValue(input, contextSpaceProxy);
        return retValue;
    }

    private static void writeDocumentPropertiesArray(PbsOutputStream output,
                                                     DocumentProperties[] value) {
        output.writeInt(value.length);
        for (int i = 0; i < value.length; i++)
            writeDocumentProperties(output, value[i]);
    }

    private static DocumentProperties[] readDocumentPropertiesArray(PbsInputStream input, ISpaceProxy contextSpaceProxy) {
        int length = input.readInt();
        DocumentProperties[] result = new DocumentProperties[length];
        for (int i = 0; i < length; i++)
            result[i] = readDocumentProperties(input, contextSpaceProxy);

        return result;
    }

    private static void writePbsObjectArray(PbsOutputStream output, Object[] array) {
        // Array's type
        String arrName = array.getClass().getComponentType().getName();
        output.writeString(arrName);
        // Array length and items
        output.writeInt(array.length);
        for (int i = 0; i < array.length; i++)
            writeFieldValue(output, array[i]); // Recurse call
    }

    private static Object readPbsObjectArray(PbsInputStream input, ISpaceProxy contextSpaceProxy) {
        Class<?> objClass;
        String className = input.readString();
        try {
            objClass = ClassLoaderHelper.loadClass(className);
        } catch (ClassNotFoundException e) {
            try {
                if (contextSpaceProxy != null)
                    objClass = contextSpaceProxy.loadRemoteClass(className);
                else
                    throw e;
            } catch (ClassNotFoundException ei) {
                _logger.log(Level.SEVERE, "PBS error: Object array class not found [" + className + "]");
                throw new ConversionException("PBS error: Object array class not found [" + className + "].", e);
            }
        }
        int length = input.readInt();
        Object[] retValue = (Object[]) Array.newInstance(objClass, length);

        for (int i = 0; i < length; i++)
            retValue[i] = readFieldValue(input, contextSpaceProxy);
        return retValue;
    }

    private static void writeList(PbsOutputStream output, PbsTypeInfo listTypeInfo, List<?> list) {
        // If list is custom, serialize the type name:
        if (listTypeInfo.typeCode == PbsTypeInfo.LIST_CUSTOM_TYPE)
            output.writeString(((PbsCustomTypeList<?>) list).getTypeName());
            // Otherwise, if list is generic write generic component type name:
        else if (isGeneric(listTypeInfo.typeCode))
            output.writeString(((ISingleGenericType) list).getGenericType());

        // Write list length:
        int length = list.size();
        output.writeInt(length);

        // Write list elements:
        if (list instanceof RandomAccess) {
            for (int i = 0; i < length; i++) {
                Object item = list.get(i);
                writeFieldValue(output, item);
            }
        } else {
            Iterator<?> iterator = list.iterator();
            while (iterator.hasNext()) {
                Object item = iterator.next();
                writeFieldValue(output, item);
            }
        }
    }

    private static Object readList(PbsInputStream input, byte listTypeCode, ISpaceProxy contextSpaceProxy) {
        String customTypeName = null;
        String genericTypeName = null;
        // If list is custom read customTypeName:
        if (listTypeCode == PbsTypeInfo.LIST_CUSTOM_TYPE)
            customTypeName = input.readString();
            // Otherwise if list is generic Read generic component type name:
        else if (isGeneric(listTypeCode))
            genericTypeName = input.readString();

        // Read length:
        int length = input.readInt();

        // Instantiate list:
        List<Object> list = null;
        switch (listTypeCode) {
            case PbsTypeInfo.LIST_CUSTOM_TYPE:
                list = new PbsCustomTypeList<Object>(length, customTypeName);
                break;
            case PbsTypeInfo.LIST_ARRAY:
                list = new ArrayList<Object>(length);
                break;
            case PbsTypeInfo.LIST_ARRAY_GENERIC:
                list = new DotnetGenericArrayList(length, genericTypeName);
                break;
            default:
                throw new PBSInternalSpaceException("PBS failed on list read due to unsupported list type code: " + listTypeCode);
        }

        // Read list items:
        for (int i = 0; i < length; i++) {
            Object item = readFieldValue(input, contextSpaceProxy);
            list.add(item);
        }

        return list;
    }

    private static void writeMap(PbsOutputStream output, PbsTypeInfo mapTypeInfo, Map map) {
        // If map is custom, serialize the type name:
        if (mapTypeInfo.typeCode == PbsTypeInfo.MAP_CUSTOM_TYPE)
            output.writeString(((PbsCustomTypeMap<?, ?>) map).getTypeName());
            // Otherwise, if map is generic write generic key and component types names:
        else if (isGeneric(mapTypeInfo.typeCode)) {
            IDoubleGenericType genType = (IDoubleGenericType) map;
            output.writeString(genType.getFirstGenericType());
            output.writeString(genType.getSecondGenericType());
        }

        // Write map length:
        int length = map.size();
        output.writeInt(length);

        // Write map key-value pairs:
        Iterator<Entry<?, ?>> mapIterator = map.entrySet().iterator();
        while (mapIterator.hasNext()) {
            Entry<?, ?> entry = mapIterator.next();
            writeFieldValue(output, entry.getKey());
            writeFieldValue(output, entry.getValue());
        }
    }

    private static Object readMap(PbsInputStream input, byte mapTypeCode, ISpaceProxy contextSpaceProxy) {
        final float defaultLoadFactor = 0.75f;

        String customTypeName = null;
        String keyGenericType = null;
        String valueGenericType = null;

        if (mapTypeCode == PbsTypeInfo.MAP_CUSTOM_TYPE)
            customTypeName = input.readString();
        else if (isGeneric(mapTypeCode)) {
            keyGenericType = input.readString();
            valueGenericType = input.readString();
        }

        // Read length:
        int length = input.readInt();
        int capacity = (int) Math.ceil(length / defaultLoadFactor);

        // Instantiate map:
        Map<Object, Object> map = null;
        switch (mapTypeCode) {
            case PbsTypeInfo.MAP_CUSTOM_TYPE:
                map = new PbsCustomTypeMap<Object, Object>(capacity, defaultLoadFactor, customTypeName);
                break;
            case PbsTypeInfo.MAP_HASH:
                map = new HashMap<Object, Object>(capacity, defaultLoadFactor);
                break;
            case PbsTypeInfo.MAP_LINKED:
                map = new LinkedHashMap<Object, Object>(capacity, defaultLoadFactor);
                break;
            case PbsTypeInfo.MAP_PROPERTIES:
                map = new Properties();
                break;
            case PbsTypeInfo.MAP_TREE:
                map = new TreeMap<Object, Object>();
                break;
            case PbsTypeInfo.MAP_HASH_GENERIC:
                map = new DotnetGenericHashMap(capacity, defaultLoadFactor, keyGenericType, valueGenericType);
                break;
            case PbsTypeInfo.MAP_LINKED_GENERIC:
                map = new DotnetGenericLinkedHashMap(capacity, defaultLoadFactor, keyGenericType, valueGenericType);
                break;
            case PbsTypeInfo.MAP_TREE_GENERIC:
                map = new DotnetGenericTreeMap(keyGenericType, valueGenericType);
                break;
            default:
                throw new PBSInternalSpaceException("PBS failed on map read due to unsupported map type code: " + mapTypeCode);
        }

        // Read map key-value pairs:
        for (int i = 0; i < length; i++) {
            Object key = readFieldValue(input, contextSpaceProxy);
            Object value = readFieldValue(input, contextSpaceProxy);
            map.put(key, value);
        }

        return map;
    }

    /**
     * Serialize a primitive (PbsFormatter primitive types) to the given stream
     *
     * @param output Stream to serialize into
     * @param array  Array of objects to serialize
     */
    public static void writePrimitiveArray(PbsOutputStream output, Object[] array) {
        if (array == null)
            output.writeInt(-1);
        else {
            output.writeInt(array.length);
            for (int i = 0; i < array.length; ++i) {
                PbsEntryFormatter.writeFieldValue(output, array[i]);
            }
        }
    }

    public static Object[] readPrimitiveArray(PbsInputStream input, ISpaceProxy contextSpaceProxy) {
        int length = input.readInt();
        if (length == -1)
            return null;

        Object[] array = new Object[length];
        for (int i = 0; i < length; i++)
            array[i] = PbsEntryFormatter.readFieldValue(input, contextSpaceProxy);

        return array;
    }

    private static Object createInstance(String className) {
        try {
            // Load class:
            Class<?> clazz = ClassLoaderHelper.loadClass(className);
            // Create instance:
            Object instance = clazz.newInstance();
            // Return result:
            return instance;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            // TODO: Log exception.
            throw new PBSInternalSpaceException("PBS failed to load class " + className, e);
        } catch (InstantiationException e) {
            e.printStackTrace();
            // TODO: Log exception.
            throw new PBSInternalSpaceException("PBS failed to instantiate class " + className, e);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            // TODO: Log exception.
            throw new PBSInternalSpaceException("PBS failed to instantiate class " + className, e);
        }
    }

    public static PbsEntryPacket readPbsEntryPacket(PbsInputStream input) {
        return readPbsEntryPacket(input, false);
    }

    public static PbsTemplatePacket readPbsTemplatePacket(PbsInputStream input) {
        return (PbsTemplatePacket) readPbsEntryPacket(input, true);
    }

    public static PbsEntryPacket[] readPbsEntryPacketArray(PbsInputStream input) {
        int length = input.readInt();
        if (length < 0)
            return null;
        PbsEntryPacket[] entries = new PbsEntryPacket[length];
        for (int i = 0; i < length; ++i)
            entries[i] = readPbsEntryPacket(input, false);
        return entries;
    }


    private static PbsEntryPacket readPbsEntryPacket(PbsInputStream input, boolean isTemplate) {
        byte[] streamBytes = input.readFixedByteArray();
        DocumentProperties dynamicProperties = readDynamicProperties(input);

        PbsEntryPacket result;
        if (streamBytes == null)
            result = PbsTemplatePacket.getNullTemplate(QueryResultTypeInternal.OBJECT_DOTNET);
        else if (isTemplate) {
            QueryResultTypeInternal queryResultType = readQueryResultType(input);
            result = new PbsTemplatePacket(streamBytes, dynamicProperties, queryResultType);
        } else {
            EntryType entryType = readEntryType(input);
            result = new PbsEntryPacket(streamBytes, dynamicProperties, entryType);
        }

        return result;
    }

    public static DocumentProperties readDynamicProperties(PbsInputStream input) {
        boolean hasDynamicProperties = input.readBoolean();
        if (!hasDynamicProperties)
            return null;

        int length = input.readIntFixed();
        if (length == 0)
            return null;
        DocumentProperties properties = new DocumentProperties();
        for (int i = 0; i < length; i++) {
            String key = input.readRepetitiveString();
            Object value = readFieldValue(input);
            properties.put(key, value);
        }
        return properties;
    }

    private static final byte NULL_TEMPLATE = 0;
    // private static final byte SQLQUERY_TEMPLATE_OLD = 1;
    // private static final byte PREPARED_TEMPLATE_WITH_METADATA = 2;
    private static final byte PREPARED_TEMPLATE_WITHOUT_METADATA = 3;
    private static final byte OBJECT_TEMPLATE = 4;
    private static final byte SQLQUERY_TEMPLATE_PARAMETERS = 5;
    private static final byte IDQUERY_TEMPLATE = 6;

    public static ITemplatePacket readPbsTemplate(PbsInputStream input, ISpaceProxy spaceProxy) {
        return readPbsTemplate(input, spaceProxy, null);
    }

    public static ITemplatePacket readPbsTemplate(PbsInputStream input, ISpaceProxy spaceProxy, ITypeDesc typeDescriptor) {
        byte templateTypeCode = input.readByte();
        switch (templateTypeCode) {
            case NULL_TEMPLATE:
                return PbsTemplatePacket.getNullTemplate(QueryResultTypeInternal.OBJECT_DOTNET);
//          case SQLQUERY_TEMPLATE_OLD:
//              return readSqlQueryWithTemplate(input, spaceProxy);
//        	case PREPARED_TEMPLATE_WITH_METADATA:
            case PREPARED_TEMPLATE_WITHOUT_METADATA:
                final long templateHandleId = input.readLong();
                return PbsPreparedTemplateCache.getCache().get(templateHandleId);
            case OBJECT_TEMPLATE:
                return readPbsTemplatePacket(input);
            case SQLQUERY_TEMPLATE_PARAMETERS:
                return readSqlQuery(input, spaceProxy, typeDescriptor);
            case IDQUERY_TEMPLATE:
                return readIdQueryTemplate(input, spaceProxy);
            default:
                throw new PBSInternalSpaceException("readPbsTemplate received unknown code " + templateTypeCode);
        }
    }


    private static ITemplatePacket readIdQueryTemplate(PbsInputStream input, ISpaceProxy spaceProxy) {
        String typeName = input.readRepetitiveString();
        Object id = PbsEntryFormatter.readFieldValue(input, spaceProxy);
        Object routing = PbsEntryFormatter.readFieldValue(input, spaceProxy);
        int version = input.readInt();
        QueryResultTypeInternal queryResultType = PbsEntryFormatter.readQueryResultType(input);
        AbstractProjectionTemplate projectionTemplate = readProjectionTemplate(input);

        ITypeDesc typeDesc = spaceProxy.getDirectProxy().getTypeManager().getTypeDescByName(typeName);
        return TemplatePacketFactory.createIdOrUidPacket(typeDesc, queryResultType, routing, id, version, projectionTemplate);
    }

    public static ITemplatePacket readSqlQuery(PbsInputStream input, ISpaceProxy spaceProxy, ITypeDesc typeDescriptor) {
        // Read query arguments:
        String typeName = input.readRepetitiveString();
        QueryResultTypeInternal queryResultType = readQueryResultType(input);
        ITypeDesc implicitTypeDesc = PbsEntryFormatter.readTypeDescIfExists(input, typeName, queryResultType.getEntryType());
        String expression = input.readString();
        Object[] parameters = readPrimitiveArray(input, spaceProxy);
        Object routing = PbsEntryFormatter.readFieldValue(input, spaceProxy);

        PbsProjectionTemplate projectionTemplate = readProjectionTemplate(input);

        // Create query:
        SQLQuery<Object> sqlQuery = new SQLQuery<Object>(typeName, expression, parameters);
        sqlQuery.setRouting(routing);
        if (typeDescriptor == null)
            typeDescriptor = implicitTypeDesc;
        if (typeDescriptor != null)
            spaceProxy.getDirectProxy().getTypeManager().registerTypeDesc(typeDescriptor);

        return getTemplatePacketFromSqlQuery(sqlQuery, spaceProxy, projectionTemplate, queryResultType);
    }

    public static PbsProjectionTemplate readProjectionTemplate(
            PbsInputStream input) {
        PbsProjectionTemplate projectionTemplate = null;
        if (input.readBoolean()) {
            byte[] streamBytes = input.readFixedByteArray();
            String[] dynamicProperties = input.readRepetitiveStringArray();
            String[] fixedPaths = input.readRepetitiveStringArray();
            String[] dynamicPaths = input.readRepetitiveStringArray();
            projectionTemplate = new PbsProjectionTemplate(streamBytes, dynamicProperties, fixedPaths, dynamicPaths);
        }
        return projectionTemplate;
    }

    public static ITemplatePacket getCppTemplatePacketFromSqlQuery(SQLQuery<?> sqlQuery, ISpaceProxy spaceProxy) {
        return getTemplatePacketFromSqlQuery(sqlQuery, spaceProxy, null, QueryResultTypeInternal.CPP);
    }

    public static ITemplatePacket getTemplatePacketFromSqlQuery(SQLQuery<?> sqlQuery, ISpaceProxy spaceProxy, PbsProjectionTemplate projectionTemplate, QueryResultTypeInternal resultType) {
        ITemplatePacket templatePacket = spaceProxy.getDirectProxy().getTypeManager().getTemplatePacketFromObject(sqlQuery, ObjectType.SQL);
        if (templatePacket instanceof SQLQueryTemplatePacket) {
            SQLQueryTemplatePacket sqlQueryTemplatePacket = (SQLQueryTemplatePacket) templatePacket;
            sqlQueryTemplatePacket.setQueryResultType(resultType);
            sqlQueryTemplatePacket.setProjectionTemplate(projectionTemplate);
        } else {
            throw new IllegalStateException("Attempt to create SQLQuery and the internal conversion is not of type " + SQLQueryTemplatePacket.class.getName());
        }
        return templatePacket;
    }

    public static void writeTypeDesc(PbsOutputStream output, ITypeDesc typeDescriptor) {
        if (typeDescriptor == null) {
            output.writeBoolean(false);
            return;
        }
        output.writeBoolean(true);
        output.writeString(typeDescriptor.getTypeName());
        output.writeBoolean(typeDescriptor.isReplicable());
        output.writeByte(FifoHelper.toCode(typeDescriptor.getFifoSupport()));
        int numOfFixedProperties = typeDescriptor.getNumOfFixedProperties();
        output.writeInt(numOfFixedProperties);
        for (int i = 0; i < numOfFixedProperties; i++) {
            PropertyInfo property = typeDescriptor.getFixedProperty(i);
            output.writeString(property.getName());
            output.writeString(property.getTypeName());
            output.writeByte(property.getDotnetStorageType());
        }
        Map<String, SpaceIndex> indexes = typeDescriptor.getIndexes();
        if (indexes == null)
            output.writeInt(-1);
        else {
            output.writeInt(indexes.size());
            for (Entry<String, SpaceIndex> index : indexes.entrySet()) {
                output.writeString(index.getKey());
                output.writeByte(SpaceIndexTypeHelper.toCode(index.getValue().getIndexType()));
                output.writeBoolean(index.getValue().isUnique());
            }
        }
        output.writeString(typeDescriptor.getIdPropertyName());
        output.writeBoolean(typeDescriptor.getSpaceIdType() == SpaceIdType.AUTOMATIC);
        output.writeString(typeDescriptor.getRoutingPropertyName());
        output.writeBoolean(typeDescriptor.supportsOptimisticLocking());
        output.writeBoolean(typeDescriptor.supportsDynamicProperties());
        output.writeByte(typeDescriptor.getDotnetDynamicPropertiesStorageType());
        output.writeString(typeDescriptor.getDotnetDocumentWrapperTypeName());
        output.writeStringArray(typeDescriptor.getSuperClassesNames());
        output.writeString(typeDescriptor.getFifoGroupingPropertyPath());
        writeFifoGroupingIndexesPaths(output, typeDescriptor);
    }

    private static void writeFifoGroupingIndexesPaths(PbsOutputStream output, ITypeDesc typeDescriptor) {
        Set<String> fifoGroupingIndexesPaths = typeDescriptor.getFifoGroupingIndexesPaths();
        if (fifoGroupingIndexesPaths == null)
            output.writeInt(-1);
        else {
            output.writeInt(fifoGroupingIndexesPaths.size());
            for (String path : fifoGroupingIndexesPaths) {
                output.writeString(path);
            }
        }
    }

    private static boolean isGeneric(int typeCode) {
        return typeCode == PbsTypeInfo.LIST_ARRAY_GENERIC ||
                typeCode == PbsTypeInfo.MAP_HASH_GENERIC ||
                typeCode == PbsTypeInfo.MAP_LINKED_GENERIC ||
                typeCode == PbsTypeInfo.MAP_TREE_GENERIC;
    }

    public static QueryResultTypeInternal readQueryResultType(PbsInputStream input) {
        return QueryResultTypeInternal.fromCode(input.readByte());
    }

    public static EntryType readEntryType(PbsInputStream input) {
        return readQueryResultType(input).getEntryType();
    }

}
