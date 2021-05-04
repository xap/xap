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
package org.openspaces.core.config;

import com.gigaspaces.server.blobstore.BlobStoreException;
import com.gigaspaces.server.blobstore.BlobStoreStorageHandler;
import com.j_spaces.core.client.SQLQuery;
import org.openspaces.core.space.BlobStoreDataCachePolicy;
import org.openspaces.core.space.CachePolicy;

import java.util.List;

/**
 * @author Niv Ingberg
 * @since 12.3
 */
public class BlobStoreDataPolicyConfigurer {
    private BlobStoreStorageHandler blobStoreHandler;
    private List<SQLQuery> cacheQueries;
    private Boolean persistent;
    private Integer cacheEntriesPercentage;
    private Integer avgObjectSizeKB;
    private Integer avgObjectSizeBytes;

    public BlobStoreStorageHandler getBlobStoreHandler() {
        return blobStoreHandler;
    }
    public BlobStoreDataPolicyConfigurer setBlobStoreHandler(BlobStoreStorageHandler blobStoreHandler) {
        this.blobStoreHandler = blobStoreHandler;
        return this;
    }

    public List<SQLQuery> getCacheQueries() {
        return cacheQueries;
    }
    public BlobStoreDataPolicyConfigurer setCacheQueries(List<SQLQuery> cacheQueries) {
        this.cacheQueries = cacheQueries;
        return this;
    }

    public Boolean getPersistent() {
        return persistent;
    }
    public BlobStoreDataPolicyConfigurer setPersistent(Boolean persistent) {
        this.persistent = persistent;
        return this;
    }

    public Integer getCacheEntriesPercentage() {
        return cacheEntriesPercentage;
    }
    public BlobStoreDataPolicyConfigurer setCacheEntriesPercentage(Integer cacheEntriesPercentage) {
        this.cacheEntriesPercentage = cacheEntriesPercentage;
        return this;
    }


    public BlobStoreDataPolicyConfigurer setAvgObjectSizeBytes(Integer avgObjectSizeBytes) {
        this.avgObjectSizeBytes = avgObjectSizeBytes;
        return this;
    }

    public BlobStoreDataPolicyConfigurer setAvgObjectSizeKB(Integer avgObjectSizeKB) {
        this.avgObjectSizeKB = avgObjectSizeKB;
        return this;
    }

    public CachePolicy asCachePolicy() {
        final BlobStoreDataCachePolicy policy = new BlobStoreDataCachePolicy();
        if (avgObjectSizeKB != null && avgObjectSizeBytes != null) {
            throw new BlobStoreException("avgObjectSizeKB and avgObjectSizeBytes cannot be used together");
        }
        if (avgObjectSizeKB != null)
            policy.setAvgObjectSizeKB(avgObjectSizeKB);
        if (avgObjectSizeBytes != null)
            policy.setAvgObjectSizeBytes(avgObjectSizeBytes);
        if (cacheEntriesPercentage != null)
            policy.setCacheEntriesPercentage(cacheEntriesPercentage);
        if (persistent != null) {
            policy.setPersistent(persistent);
        }
        if (blobStoreHandler != null) {
            policy.setBlobStoreHandler(blobStoreHandler);
        } else {
            throw new BlobStoreException("blobStoreHandler attribute in Blobstore space must be configured");
        }
        if(cacheQueries != null)
            for(SQLQuery sqlQuery : cacheQueries){
                policy.addCacheQuery(sqlQuery);
            }
        return policy;
    }
}
