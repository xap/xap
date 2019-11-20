package org.openspaces.core.config;

import com.gigaspaces.server.blobstore.BlobStoreStorageHandler;
import org.openspaces.core.space.EmbeddedSpaceFactoryBean;
import org.openspaces.core.space.EmbeddedSpaceFactoryBeanConfigurer;
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
        blobStoreDataPolicy.setBlobStoreHandler(createBlobStoreStorageHandler(propertyResolver));
        factoryBean.setBlobStoreDataPolicy(blobStoreDataPolicy);
    }

    protected abstract BlobStoreStorageHandler createBlobStoreStorageHandler(PropertyResolver propertyResolver);
}
