package com.gigaspaces.internal.server.space.executors;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.SpaceDataSource;
import com.gigaspaces.internal.space.requests.DataSourceLoadSpaceRequestInfo;
import com.gigaspaces.internal.space.responses.DataSourceLoadSpaceResponseInfo;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.security.authorities.SpaceAuthority;

/**
 * @author Alon Shoham
 * @since 15.5.0
 */
public class SpaceDataSourceLoadExecutor extends SpaceActionExecutor {

    @Override
    public SpaceResponseInfo execute(SpaceImpl space, SpaceRequestInfo spaceRequestInfo) {
        DataSourceLoadSpaceRequestInfo requestInfo = (DataSourceLoadSpaceRequestInfo) spaceRequestInfo;
        DataSourceLoadSpaceResponseInfo responseInfo = new DataSourceLoadSpaceResponseInfo();
        SpaceDataSource spaceDataSource = requestInfo.getSpaceDataSource(space.getEngine().getPartitionRoutingInfo());
        if (spaceDataSource != null){
            DataIterator<SpaceTypeDescriptor> metaDataIterator = spaceDataSource.initialMetadataLoad();
            if(metaDataIterator!= null) {
                while (metaDataIterator.hasNext()) {
                    try {
                        space.getSingleProxy().registerTypeDescInServers((ITypeDesc) metaDataIterator.next());
                    } catch (Exception e) {
                        responseInfo.exception = e;
                    }
                }
            }
            if(responseInfo.exception != null)
                return responseInfo;
            DataIterator<Object> dataIterator = spaceDataSource.initialDataLoad();
            if(dataIterator != null) {
                while (dataIterator.hasNext()) {
                    try {
                        Object object = dataIterator.next();
                        if(object != null) {
                            space.getSingleProxy().write(object, null, Long.MAX_VALUE);
                        }
                    } catch (Exception e) {
                        responseInfo.exception = e;
                    }
                }
            }
        }
        return responseInfo;
    }

    @Override
    public SpaceAuthority.SpacePrivilege getPrivilege() {
        return SpaceAuthority.SpacePrivilege.WRITE;
    }
}
