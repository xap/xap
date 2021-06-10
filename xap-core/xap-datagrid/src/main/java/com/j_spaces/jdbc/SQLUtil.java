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


package com.j_spaces.jdbc;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.ITypeIntrospector;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.utils.ObjectConverter;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.gigaspaces.metadata.SpacePropertyDescriptor;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.UnknownTypeException;
import com.j_spaces.core.admin.IRemoteJSpaceAdmin;
import com.j_spaces.jdbc.driver.Blob;
import com.j_spaces.jdbc.driver.Clob;
import com.j_spaces.jdbc.query.QueryColumnData;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * SQLUtil defines utility methods for the SQL query classes
 *
 * @author anna
 * @version 1.0
 * @since 5.1
 */
@com.gigaspaces.api.InternalApi
public class SQLUtil {
    /* Default buffer size when reading from Streams */
    private static final int DEFAULT_BUFFER_SIZE = 2048;

    /**
     * Verifies that the correct type of object is supplied for given column. If types don't match -
     * object is converted to the column type.
     */
    public static Object cast(ITypeDesc typeDesc, String propertyName, Object obj, boolean isPreparedValue)
            throws SQLException {
        if (obj == null)
            return null;

        final Class<?> type = getPropertyType(typeDesc, propertyName);
        if (type == void.class)
            return obj;

        if (isPreparedValue) {
            if (type.getName().equals(Clob.class.getName()))
                return new Clob((String) obj);
            if (type.getName().equals(Blob.class.getName()))
                return new Blob((byte[]) obj);
            return applyStorageAdapterIfNeeded(typeDesc, propertyName, obj);
        }

        Object result =  ObjectConverter.convert(obj, type);
        return applyStorageAdapterIfNeeded(typeDesc, propertyName, result);
    }

    private static Object applyStorageAdapterIfNeeded(ITypeDesc typeDesc, String propertyName, Object obj) throws SQLException {
        if (typeDesc.isAllPropertiesObjectStorageType())
            return obj;
        PropertyInfo property = (PropertyInfo) typeDesc.getFixedProperty(propertyName);
        if (property == null || property.getStorageAdapter() == null)
            return obj;
        // Preserve backwards behaviour for properties with storage type:
        if (property.getStorageType().getStorageAdapterClass() != null)
            return obj;
        try {
            return property.getStorageAdapter().toSpace(obj);
        } catch (IOException e) {
            throw new SQLException("Failed to apply property storage adapter on value", e);
        }
    }

    public static Class<?> getPropertyType(ITypeDesc typeDesc, String propertyName)
            throws SQLException {
        if (propertyName.contains("[*]"))
            return void.class;

        SpacePropertyDescriptor property = typeDesc.getFixedProperty(propertyName);
        if (property != null)
            return property.getType();

        // temporary check for POJO - must be removed once other object types support nested properties
        final ITypeIntrospector introspector = typeDesc.getIntrospector(typeDesc.getObjectType());

        //check for nested properties
        if (introspector.supportsNestedOperations())
            return introspector.getPathType(propertyName);

        //TODO move this to the introspector
        if (typeDesc.supportsDynamicProperties())
            return void.class;

        throw new SQLException("Property '" + propertyName
                + "' doesn't exist in type '" + typeDesc.getTypeName()
                + "'. Valid properties are "
                + Arrays.toString(typeDesc.getPropertiesNames()) + ".");
    }

    public static Object getFieldValue(IEntryPacket e, ITypeDesc typeDesc, String propertyName) {
        SpacePropertyDescriptor property = typeDesc.getFixedProperty(propertyName);
        if (property != null)
            return e.getPropertyValue(propertyName);

        // temporary check - must be removed once other object types support nested properties
        final ITypeIntrospector introspector = typeDesc.getIntrospector(typeDesc.getObjectType());
        if (!introspector.supportsNestedOperations())
            return null;

        return introspector.getPathValue(e, propertyName);
    }

    public static Object getFieldValue(IEntryPacket entry,
                                       QueryColumnData columnData) {

        if (columnData.isNestedQueryColumn())
            return SQLUtil.getFieldValue(entry, entry.getTypeDescriptor(), columnData.getColumnPath());

        if (columnData.getColumnIndexInTable() == -1) {
            if (columnData.getColumnTableData() != null && columnData.getColumnTableData().getTypeDesc().supportsDynamicProperties())
                return entry.getPropertyValue(columnData.getColumnName());

            throw new IllegalArgumentException("No table was found for column '" + columnData.getColumnName() + "'");
        }

        if (entry.getTypeDescriptor() != null && entry.getTypeDescriptor().getIdentifierPropertyId() == columnData.getColumnIndexInTable())
            return entry.getID();

        return entry.getFieldValue(columnData.getColumnIndexInTable());
    }


    /**
     * Get this space admin
     */
    public static IRemoteJSpaceAdmin getAdmin(IJSpace space)
            throws RemoteException {
        return (IRemoteJSpaceAdmin) ((ISpaceProxy) space).getPrivilegedAdmin();
    }

    /**
     * Checks if given table (class name) exists in space
     */
    public static ITypeDesc checkTableExistence(String tableName, IJSpace space)
            throws SQLException {
        try {
            return space.getDirectProxy().getTypeManager().getTypeDescByName(tableName);
        } catch (SpaceMetadataException ex) {
            if (ex.getCause() instanceof UnknownTypeException)
                throw new SQLException("Table [" + tableName + "] does not exist", "GSP", -105);
            else
                throw ex;
        }
    }

    /**
     * Converts an input stream to a byte array
     *
     * @param inputStream       The input stream
     * @param inputStreamLength Number of bytes to read from input stream
     * @return The converted input stream as a byte array
     */
    public static byte[] convertInputStreamToByteArray(InputStream inputStream, int inputStreamLength) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int blockSize = DEFAULT_BUFFER_SIZE;
        byte[] buffer = new byte[blockSize];

        int left = inputStreamLength;
        while (left > 0) {
            int read = inputStream.read(buffer, 0, left > blockSize ? blockSize : left);

            if (read == -1)
                break;

            out.write(buffer, 0, read);

            left -= read;
        }
        return out.toByteArray();
    }

    /**
     * Converts an ascii input stream to a string
     *
     * @param inputStream       The input stream to convert to string
     * @param inputStreamLength The number of bytes to read from input stream
     * @return The input stream as a string
     */
    public static String convertAsciiInputStreamToString(InputStream inputStream, int inputStreamLength) throws IOException {

        InputStreamReader in = new InputStreamReader(inputStream);
        StringWriter writer = new StringWriter();
        int blockSize = DEFAULT_BUFFER_SIZE;
        char[] buffer = new char[blockSize];

        int left = inputStreamLength;
        while (left > 0) {
            int read = in.read(buffer, 0, left > blockSize ? blockSize : left);

            if (read == -1)
                break;

            writer.write(buffer, 0, read);

            left -= read;
        }
        writer.close();

        return writer.toString();
    }

    /**
     * Convert a reader to a string
     *
     * @param reader       The reader
     * @param readerLength The number of characters to read from the reader
     * @return The converted reader as a string
     */
    public static String convertReaderToString(Reader reader, int readerLength) throws IOException {
        StringWriter writer = new StringWriter();
        int blockSize = DEFAULT_BUFFER_SIZE;
        char[] buffer = new char[blockSize];

        int left = readerLength;
        while (left > 0) {
            int read = reader.read(buffer, 0, left > blockSize ? blockSize : left);

            if (read == -1)
                break;

            writer.write(buffer, 0, read);

            left -= read;
        }
        writer.close();

        return writer.toString();
    }


}
