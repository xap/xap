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

package com.gigaspaces.persistency;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.DataIteratorAdapter;
import com.gigaspaces.datasource.DataSourceIdQuery;
import com.gigaspaces.datasource.DataSourceIdsQuery;
import com.gigaspaces.datasource.DataSourceQuery;
import com.gigaspaces.datasource.SpaceDataSource;
import com.gigaspaces.metadata.SpacePropertyDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.persistency.datasource.DefaultMongoDataIterator;
import com.gigaspaces.persistency.datasource.MongoInitialDataLoadIterator;
import com.gigaspaces.persistency.datasource.MongoSqlQueryDataIterator;
import com.gigaspaces.persistency.metadata.DefaultSpaceDocumentMapper;
import com.gigaspaces.persistency.metadata.SpaceDocumentMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.core.cluster.ClusterInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * A MongoDB implementation of {@link com.gigaspaces.datasource.SpaceDataSource}
 *
 * @author Shadi Massalha
 */
public class MongoSpaceDataSource extends SpaceDataSource {

    private static final Log logger = LogFactory.getLog(MongoSpaceDataSource.class);

    private final MongoClientConnector mongoClient;

    protected ClusterInfo clusterInfo;

    public MongoSpaceDataSource(MongoClientConnector mongoClient, ClusterInfo clusterInfo) {

        if (mongoClient == null) {
            throw new IllegalArgumentException("Argument cannot be null - mongoClient");
        }
        this.mongoClient = mongoClient;
        this.clusterInfo = clusterInfo;
        if(clusterInfo != null){
            setNumberOfInstances(clusterInfo.getNumberOfInstances());
            setInstanceId(clusterInfo.getInstanceId());
        }
    }

    public void close() throws IOException {

        if (logger.isDebugEnabled())
            logger.debug("MongoSpaceDataSource.close()");

        mongoClient.close();
    }

    /**
     * Inheritance is not supported.
     */
    @Override
    public boolean supportsInheritance() {
        return false;
    }

    @Override
    public DataIterator<SpaceTypeDescriptor> initialMetadataLoad() {

        if (logger.isDebugEnabled())
            logger.debug("MongoSpaceDataSource.initialMetadataLoad()");

        Collection<SpaceTypeDescriptor> sortedCollection = mongoClient.loadMetadata();

        return new DataIteratorAdapter<SpaceTypeDescriptor>(sortedCollection.iterator());
    }

    @Override
    public DataIterator<Object> initialDataLoad() {

        if (logger.isDebugEnabled())
            logger.debug("MongoSpaceDataSource.initialDataLoad()");

        return new MongoInitialDataLoadIterator(this, mongoClient);
    }

    public DBObject getInitialQuery(SpaceTypeDescriptor typeDescriptor) {
        DBObject query = new BasicDBObject();
        String routingPropertyName = typeDescriptor.getRoutingPropertyName();
        if(routingPropertyName == null || numberOfInstances == null || instanceId == null)
            return query;
        if (numberOfInstances > 1) {
            SpacePropertyDescriptor routingPropDesc = typeDescriptor.getFixedProperty(routingPropertyName);
            if (Integer.class.isAssignableFrom(routingPropDesc.getType())) {
                ArrayList l = new ArrayList();
                l.add(numberOfInstances);
                l.add(instanceId - 1);
                query.put(routingPropertyName, new BasicDBObject("$mod", l));
            }
        }
        return query;
    }

    @Override
    public DataIterator<Object> getDataIterator(DataSourceQuery query) {

        if (logger.isDebugEnabled())
            logger.debug("MongoSpaceDataSource.getDataIterator(" + query + ")");

        return new MongoSqlQueryDataIterator(mongoClient, query);
    }

    @Override
    public Object getById(DataSourceIdQuery idQuery) {

        if (logger.isDebugEnabled())
            logger.debug("MongoSpaceDataSource.getById(" + idQuery + ")");

        SpaceDocumentMapper<DBObject> mapper = new DefaultSpaceDocumentMapper(idQuery.getTypeDescriptor(), mongoClient.prefferPojo());

        BasicDBObjectBuilder documentBuilder = BasicDBObjectBuilder.start().add(Constants.ID_PROPERTY, mapper.toObject(idQuery.getId()));

        DBCollection mongoCollection = mongoClient.getCollection(idQuery.getTypeDescriptor().getTypeName());

        DBObject result = mongoCollection.findOne(documentBuilder.get());

        return mapper.toDocument(result);

    }

    @Override
    public DataIterator<Object> getDataIteratorByIds(DataSourceIdsQuery idsQuery) {

        if (logger.isDebugEnabled())
            logger.debug("MongoSpaceDataSource.getDataIteratorByIds(" + idsQuery + ")");

        DBObject[] ors = new DBObject[idsQuery.getIds().length];

        for (int i = 0; i < ors.length; i++)
            ors[i] = BasicDBObjectBuilder.start().add(Constants.ID_PROPERTY, idsQuery.getIds()[i]).get();

        DBObject document = QueryBuilder.start().or(ors).get();

        DBCollection mongoCollection = mongoClient.getCollection(idsQuery.getTypeDescriptor().getTypeName());

        DBCursor results = mongoCollection.find(document);

        return new DefaultMongoDataIterator(results, idsQuery.getTypeDescriptor(), mongoClient.prefferPojo());
    }
}
