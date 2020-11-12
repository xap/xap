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

/**
 * Encapsulates information about a property of a space type.
 *
 * @author Niv Ingberg
 * @see com.gigaspaces.metadata.SpaceTypeDescriptor
 * @since 8.0
 */
public interface SpacePropertyDescriptor {
    /**
     * Gets the property name.
     *
     * @return The property name.
     */
    String getName();

    /**
     * Gets the property type name.
     *
     * @return The property type name.
     */
    String getTypeName();

    /**
     * Gets the property display type name.
     *
     * @return The property display type name.
     * @since 9.0.1
     */
    String getTypeDisplayName();

    /**
     * Gets the property type.
     *
     * @return The property type.
     */
    Class<?> getType();

    /**
     * Gets the document support setting of this property.
     *
     * @return The document support setting of this property
     * @since 8.0.1
     */
    SpaceDocumentSupport getDocumentSupport();

    /**
     * @return The property storage type
     * @since 9.0.0
     */
    public StorageType getStorageType();

    String getStorageAdapterName();
}
