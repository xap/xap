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
import com.gigaspaces.annotation.pojo.SpaceClass.IncludeProperties;
import com.gigaspaces.metadata.StorageType;

/**
 * Defines default values used by both annotations and gs.xml.
 *
 * @author Niv Ingberg
 * @since 7.0.1
 */
public abstract class PojoDefaults {
    public static final boolean PERSIST = true;
    public static final boolean REPLICATE = true;
    public static final boolean FIFO = false;
    public static final boolean INHERIT_INDEXES = true;
    public static final FifoSupport FIFO_SUPPORT = FifoSupport.OFF;
    public static final IncludeProperties INCLUDE_PROPERTIES = IncludeProperties.IMPLICIT;
    public static final StorageType STORAGE_TYPE = StorageType.DEFAULT;
    public static final boolean BLOBSTORE_ENABLED = true;
    public static final boolean BROADCAST = false;
}
