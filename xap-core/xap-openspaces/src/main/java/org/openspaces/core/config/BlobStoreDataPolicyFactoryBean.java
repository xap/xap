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
 * A factory for creating {@link org.openspaces.core.space.BlobStoreDataCachePolicy} instance.
 *
 * @author Kobi
 * @since 10.0.0, 10.2.0
 */
public class BlobStoreDataPolicyFactoryBean {

    private final BlobStoreDataPolicyConfigurer configurer = new BlobStoreDataPolicyConfigurer();

    public void setAvgObjectSizeKB(Integer avgObjectSizeKB) {
        configurer.setAvgObjectSizeKB(avgObjectSizeKB);
    }

    public void setAvgObjectSizeBytes(Integer avgObjectSizeBytes) {
        configurer.setAvgObjectSizeBytes(avgObjectSizeBytes);
    }

    public Integer getCacheEntriesPercentage() {
        return configurer.getCacheEntriesPercentage();
    }

    public void setCacheEntriesPercentage(Integer cacheEntriesPercentage) {
        configurer.setCacheEntriesPercentage(cacheEntriesPercentage);
    }

    public Boolean getPersistent() {
        return configurer.getPersistent();
    }

    public void setPersistent(Boolean persistent) {
        configurer.setPersistent(persistent);
    }

    public BlobStoreStorageHandler getBlobStoreHandler() {
        return configurer.getBlobStoreHandler();
    }

    public void setBlobStoreHandler(BlobStoreStorageHandler blobStoreHandler) {
        configurer.setBlobStoreHandler(blobStoreHandler);
    }

    public List<SQLQuery> getBlobstoreCacheQueries() {
        return configurer.getCacheQueries();
    }

    public void setBlobstoreCacheQueries(List<SQLQuery> blobstoreCacheQueries) {
        configurer.setCacheQueries(blobstoreCacheQueries);
    }

    public CachePolicy asCachePolicy() {
        return configurer.asCachePolicy();
    }
}
