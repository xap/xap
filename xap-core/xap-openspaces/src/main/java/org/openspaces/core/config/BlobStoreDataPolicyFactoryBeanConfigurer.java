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

import com.gigaspaces.server.blobstore.BlobStoreStorageHandler;
import org.openspaces.core.space.EmbeddedSpaceFactoryBean;
import org.openspaces.core.space.EmbeddedSpaceFactoryBeanConfigurer;
import org.openspaces.pu.service.ServiceDetailsProvider;
import org.springframework.core.env.PropertyResolver;

/**
 * @author Niv Ingberg
 * @since 15.0
 */
public abstract class BlobStoreDataPolicyFactoryBeanConfigurer implements EmbeddedSpaceFactoryBeanConfigurer {

    @Override
    public void configure(EmbeddedSpaceFactoryBean factoryBean, PropertyResolver propertyResolver) {
        BlobStoreDataPolicyFactoryBean blobStoreDataPolicy = new BlobStoreDataPolicyFactoryBean();
        blobStoreDataPolicy.setPersistent(propertyResolver.getProperty("space.mx.persistent", Boolean.class));
        blobStoreDataPolicy.setCacheEntriesPercentage(propertyResolver.getProperty("space.mx.cache-entries-percentage", Integer.class));
        blobStoreDataPolicy.setAvgObjectSizeBytes(propertyResolver.getProperty("space.mx.avg-object-size", Integer.class));
        BlobStoreStorageHandler blobStoreStorageHandler = createBlobStoreStorageHandler(propertyResolver);
        blobStoreDataPolicy.setBlobStoreHandler(blobStoreStorageHandler);
        if (blobStoreStorageHandler instanceof ServiceDetailsProvider)
            factoryBean.addServiceDetails((ServiceDetailsProvider)blobStoreStorageHandler);
        factoryBean.setBlobStoreDataPolicy(blobStoreDataPolicy);
    }

    protected abstract BlobStoreStorageHandler createBlobStoreStorageHandler(PropertyResolver propertyResolver);
}
