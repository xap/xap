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
package com.gigaspaces.annotation.pojo;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.metadata.ClassBinaryStorageLayout;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that entries of this class will be stored in-memory in a packed binary format. Binary storage reduces
 * memory consumption, but requires unpacking when accessing the data at the server side (e.g. matching or aggregation).
 *
 * By default, non-indexed properties are stored in binary format, whereas indexed properties are stored in object format,
 * providing a balanced tradeoff between performance and memory footprint. You can override that default per property
 * using the {@link SpacePropertyStorage} annotation.
 *
 * @see SpacePropertyStorage
 * @author Yael Nahon, Niv Ingberg
 * @since 15.8
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface SpaceClassBinaryStorage {
    /**
     * Determines the layout of the binary storage
     */
    @ExperimentalApi
    ClassBinaryStorageLayout layout() default ClassBinaryStorageLayout.DEFAULT;
}
