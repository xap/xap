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

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.server.blobstore.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

/**
 * provide an extention to the BlobStoreStorageHandler
 *
 * @author yechiel
 * @since 10.1
 */
public abstract class BlobStoreExtendedStorageHandler extends BlobStoreStorageHandler{
    /**
     * get the   preFetchThread pool
     */
    public abstract ExecutorService getPreFetchPool();

    /**
     * Returns the data to which the specified id is mapped,
     *
     * @param id              id with which the specified data is to be associated
     * @param position        an optional position object which will be used as a pointer of
     *                        locating the id, or null if irrelevant
     * @param objectType      the object type - one of  BlobStoreObjectType values
     * @param indexesPartOnly if true only the indexed fields are returned
     * @return the data to which the specified id is mapped,
     * @throws com.gigaspaces.server.blobstore.BlobStoreException if the specified id does not
     *                                                            exist.
     */
    public abstract java.io.Serializable get(java.io.Serializable id, Object position, BlobStoreObjectType objectType, boolean indexesPartOnly);

    /**
     * Removes the id (and its corresponding data) from this FDF. return false if no such object
     * exists
     *
     * @param id         the key that needs to be removed
     * @param position   an optional position object which will be used as a pointer of locating the
     *                   id, or null if irrelevant
     * @param objectType the object type - one of  BlobStoreObjectType values
     * @throws com.gigaspaces.server.blobstore.BlobStoreException if the id does not exist.
     */
    public abstract void removeIfExists(java.io.Serializable id, Object position, BlobStoreObjectType objectType);

    /**
     * Creates an iterator over the blobstore objects in space initial load stage only.
     */
    public abstract DataIterator<BlobStoreGetBulkOperationResult> initialLoadIterator();

}
