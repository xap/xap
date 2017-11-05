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

package org.openspaces.core.space;

import com.gigaspaces.server.blobstore.BlobStoreException;
import com.gigaspaces.server.blobstore.BlobStoreStorageHandler;
import com.j_spaces.core.Constants;
import com.j_spaces.core.client.SQLQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * A cache policy that stores data blobStore and indexes onheap.
 *
 * @author yechielf, Kobi
 */
public class BlobStoreDataCachePolicy implements CachePolicy {
    private static final Logger _logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_CONFIG);

    private Integer avgObjectSizeBytes;
    private Integer cacheEntriesPercentage;
    private Boolean persistent;
    private List<SQLQuery> sqlQueryList;

    private BlobStoreStorageHandler blobStoreHandler;

    private static final int DEFAULT_AVG_OBJECT_SIZE_BYTES = 5 * 1024;
    private static final int DEFAULT_CACHE_ENTRIES_PERCENTAGE = 20;

    public BlobStoreDataCachePolicy() {
        sqlQueryList = new ArrayList<SQLQuery>();
    }

    public void setBlobStoreHandler(BlobStoreStorageHandler blobStoreHandler) {
        this.blobStoreHandler = blobStoreHandler;
    }

    public void setAvgObjectSizeKB(Integer avgObjectSizeKB) {
        this.avgObjectSizeBytes = avgObjectSizeKB * 1024;
    }

    public void setAvgObjectSizeBytes(Integer avgObjectSizeBytes) {
        this.avgObjectSizeBytes = avgObjectSizeBytes;
    }

    public void setCacheEntriesPercentage(Integer cacheEntriesPercentage) {
        this.cacheEntriesPercentage = cacheEntriesPercentage;
    }

    public void setPersistent(Boolean persistent) {
        this.persistent = persistent;
    }

    public void addCacheQuery(SQLQuery sqlQuery){
        sqlQueryList.add(sqlQuery);
    }

    public Properties toProps() {
        Properties props = new Properties();
        props.setProperty(Constants.CacheManager.FULL_CACHE_POLICY_PROP, "" + Constants.CacheManager.CACHE_POLICY_BLOB_STORE);

        long blobStoreCacheSize;

        if (cacheEntriesPercentage == null) {
            cacheEntriesPercentage = DEFAULT_CACHE_ENTRIES_PERCENTAGE;
        }
        if (avgObjectSizeBytes == null) {
            avgObjectSizeBytes = DEFAULT_AVG_OBJECT_SIZE_BYTES;
        }
        assertPropPositive("cacheEntriesPercentage", cacheEntriesPercentage);
        assertPropPositive("avgObjectSizeBytes", avgObjectSizeBytes);
        assertPropNotZero("avgObjectSizeBytes", avgObjectSizeBytes);

        if (cacheEntriesPercentage != 0) {
            long maxMemoryInBytes = Runtime.getRuntime().maxMemory();
            if (maxMemoryInBytes == Long.MAX_VALUE) {
                blobStoreCacheSize = Long.parseLong(Constants.CacheManager.CACHE_MANAGER_BLOBSTORE_CACHE_SIZE_DELAULT);
                _logger.fine("Blob Store Cache size [ " + blobStoreCacheSize + " ]");
            } else {
                double percentage = (double) cacheEntriesPercentage / 100;
                blobStoreCacheSize = (long) ((maxMemoryInBytes * percentage) / (avgObjectSizeBytes));
            }
        } else {
            blobStoreCacheSize = 0;
        }

        props.setProperty(Constants.CacheManager.FULL_CACHE_MANAGER_BLOBSTORE_CACHE_SIZE_PROP, String.valueOf(blobStoreCacheSize));
        _logger.info("Blob Store Cache size [ " + blobStoreCacheSize + " ]");


        if (persistent != null) {
            props.put(Constants.CacheManager.FULL_CACHE_MANAGER_BLOBSTORE_PERSISTENT_PROP, String.valueOf(persistent));
        } else {
            throw new BlobStoreException("persistent attribute in Blobstore space must be configured");
        }

        if (blobStoreHandler != null) {
            props.put(Constants.CacheManager.CACHE_MANAGER_BLOBSTORE_STORAGE_HANDLER_PROP, blobStoreHandler);
        } else {
            throw new BlobStoreException("blobStoreHandler attribute in Blobstore space must be configured");
        }

        if (sqlQueryList.size() > 0)
            props.put(Constants.CacheManager.FULL_CACHE_MANAGER_BLOBSTORE_CACHE_FILTER_QUERIES_PROP, sqlQueryList);

        return props;
    }

    private void assertPropPositive(String propName, long propValue) {
        if (propValue < 0) {
            throw new IllegalArgumentException(propName + " can not be negative");
        }
    }

    private void assertPropNotZero(String propName, long propValue) {
        if (propValue == 0) {
            throw new IllegalArgumentException(propName + " can not be zero");
        }
    }

}
