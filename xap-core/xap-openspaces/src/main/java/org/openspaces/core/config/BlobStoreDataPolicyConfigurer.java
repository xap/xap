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
