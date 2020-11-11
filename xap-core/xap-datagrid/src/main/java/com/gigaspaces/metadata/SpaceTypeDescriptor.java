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
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.query.extension.metadata.TypeQueryExtensions;

import java.util.Map;
import java.util.Set;

/**
 * Encapsulates information about a space type.
 *
 * @author Niv Ingberg
 * @see com.gigaspaces.metadata.SpaceTypeDescriptorBuilder
 * @see com.gigaspaces.metadata.SpacePropertyDescriptor
 * @see com.gigaspaces.metadata.index.SpaceIndex
 * @since 8.0
 */
public interface SpaceTypeDescriptor {
    /**
     * Gets the type name.
     */
    String getTypeName();

    /**
     * Gets the type simple name.
     *
     * @since 9.0.1
     */
    String getTypeSimpleName();

    /**
     * Gets the java class which correlates to the type, if available.
     */
    Class<? extends Object> getObjectClass();

    /**
     * Gets the document wrapper class. If a document wrapper class was not set, {@link
     * SpaceDocument} class is returned.
     */
    Class<? extends SpaceDocument> getDocumentWrapperClass();

    /**
     * Gets the type's FIFO support.
     *
     * @see com.gigaspaces.annotation.pojo.FifoSupport
     */
    FifoSupport getFifoSupport();

    /**
     * Returns true if this type is replicable, false otherwise.
     */
    boolean isReplicable();

    /**
     * Gets the number of fixed properties of the type.
     */
    int getNumOfFixedProperties();

    /**
     * Gets a fixed property by its position.
     *
     * @param position Position of requested fixed property.
     * @return Space property at specified position.
     */
    SpacePropertyDescriptor getFixedProperty(int position);

    /**
     * Gets a fixed property by its name.
     *
     * @param name Name of requested  fixes property.
     * @return Space property with specified name.
     */
    SpacePropertyDescriptor getFixedProperty(String name);

    /**
     * Gets the position of a fixed property by the specified property name. If there's no fixed
     * property with that name, -1 is returned.
     *
     * @param propertyName Name of property to locate, case-insensitive
     * @return Position of property.
     */
    int getFixedPropertyPosition(String propertyName);

    default int getFixedPropertyPositionIgnoreCase(String name) {
        int result = getFixedPropertyPosition(name);
        if (result != -1)
            return result;

        int length = getNumOfFixedProperties();
        for (int i = 0; i < length; i++) {
            if (getFixedProperty(i).getName().equalsIgnoreCase(name))
                return i;
        }
        return -1;
    }

    /**
     * Returns true if this type supports dynamic properties, false otherwise.
     */
    boolean supportsDynamicProperties();

    /**
     * Returns true if this type supports optimistic locking, false otherwise.
     */
    boolean supportsOptimisticLocking();

    /**
     * Gets the ID property name.
     */
    String getIdPropertyName();

    /**
     * Gets the routing property name.
     */
    String getRoutingPropertyName();

    /**
     * Gets the fifo grouping property
     */
    String getFifoGroupingPropertyPath();

    /**
     * Gets the fifo grouping indexes
     */
    Set<String> getFifoGroupingIndexesPaths();

    /**
     * Gets the indexes of the type, mapped by their names.
     *
     * @see com.gigaspaces.metadata.index.SpaceIndex
     */
    Map<String, SpaceIndex> getIndexes();

    /**
     * Get the type storage type
     */
    StorageType getStorageType();

    /**
     * Gets if this is a concrete type, i.e representing a concrete class (Pojo).
     */
    boolean isConcreteType();

    /**
     * Gets the autoGenerateId property
     */
    boolean isAutoGenerateId();

    /**
     * Gets the super type name
     */
    String getSuperTypeName();

    /**
     * When cache policy is "BLOB_STORE" only the indices + minimal administrative information will
     * be kept in cache per entry, while the data will be kept as blobs. This attribute when set to
     * "false" disables blob-store of classes and keeping the entries entirely  cached.
     *
     * @return true if blobStore will be enabled
     */

    boolean isBlobstoreEnabled();

    boolean isBroadcast();

    boolean hasSequenceNumber();

    int getSequenceNumberFixedPropertyID();

    TypeQueryExtensions getQueryExtensions();

    String[] getPropertiesNames();

    String[] getPropertiesTypes();

    boolean[] getPropertiesIndexTypes();
}
