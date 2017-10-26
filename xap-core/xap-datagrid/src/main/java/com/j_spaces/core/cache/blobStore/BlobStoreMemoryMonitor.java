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

//
package com.j_spaces.core.cache.blobStore;

/**
 * monitor against possible out of (secondary) memory in blob store implementation
 *
 * @author yechiel
 * @since 10.1
 */
public abstract class BlobStoreMemoryMonitor {

    public static String MEMORY_MONITOR_PROPERTY_NAME = "blobstore.memory.monitor";

    /**
     * verify that the designated threshold was not exceeded and throw
     * BlobStoreMemoryShortageException
     *
     * @param id the blob-store id of the object about to be put/replaced
     */
    public abstract void onMemoryAllocation(java.io.Serializable id) throws BlobStoreMemoryShortageException;
}
