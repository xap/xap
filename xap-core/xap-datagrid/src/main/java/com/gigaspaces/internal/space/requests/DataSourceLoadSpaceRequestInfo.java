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

package com.gigaspaces.internal.space.requests;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.cluster.PartitionRoutingInfo;
import com.gigaspaces.datasource.SpaceDataSource;
import com.gigaspaces.datasource.SpaceDataSourceFactory;
import com.gigaspaces.datasource.SpaceDataSourceLoadResult;
import com.gigaspaces.datasource.SpaceTypeSchemaAdapter;
import com.gigaspaces.internal.server.space.schema_evolution.SchemaAdapterSpaceDataSource;
import com.gigaspaces.internal.server.space.schema_evolution.SpaceTypeSchemaAdapterContainer;
import com.gigaspaces.internal.space.responses.DataSourceLoadSpaceResponseInfo;
import com.gigaspaces.logger.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Map;

/**
 * @author Alon Shoham
 * @since 15.5.0
 */
@com.gigaspaces.api.InternalApi
public class DataSourceLoadSpaceRequestInfo extends AbstractSpaceRequestInfo {
    private static final long serialVersionUID = 1L;
    private static final Logger _devLogger = LoggerFactory.getLogger(Constants.LOGGER_DEV);
    private SpaceDataSourceFactory spaceDataSourceFactory;
    private Map<String,SpaceTypeSchemaAdapter> adaptersMap;

    public DataSourceLoadSpaceRequestInfo(SpaceDataSourceFactory spaceDataSourceFactory, Map<String,SpaceTypeSchemaAdapter> adaptersMap) {
        this.spaceDataSourceFactory = spaceDataSourceFactory;
        this.adaptersMap = adaptersMap;
    }

    /**
     * Required for Externalizable.
     */
    public DataSourceLoadSpaceRequestInfo() {
    }

    public SpaceDataSource getSpaceDataSource(PartitionRoutingInfo partitionRoutingInfo) {
        SpaceDataSource spaceDataSource = spaceDataSourceFactory.create();
        spaceDataSource.setPartitionRoutingInfo(partitionRoutingInfo);
        return new SchemaAdapterSpaceDataSource(spaceDataSource, new SpaceTypeSchemaAdapterContainer(adaptersMap));
    }



    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);
        out.writeObject(spaceDataSourceFactory);
        out.writeObject(adaptersMap);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);
        spaceDataSourceFactory = (SpaceDataSourceFactory) in.readObject();
        adaptersMap = (Map<String, SpaceTypeSchemaAdapter>) in.readObject();
    }

    public SpaceDataSourceLoadResult reduce(List<AsyncResult<DataSourceLoadSpaceResponseInfo>> asyncResults) throws Exception{
        for(AsyncResult<DataSourceLoadSpaceResponseInfo> result: asyncResults){
            if(result.getException() != null)
                throw result.getException();
        }
        return new SpaceDataSourceLoadResult();
    }
}
