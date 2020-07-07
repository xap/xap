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

package com.gigaspaces.persistency.datasource;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.persistency.MongoClientConnector;
import com.gigaspaces.persistency.MongoSpaceDataSource;
import com.gigaspaces.persistency.metadata.DefaultSpaceDocumentMapper;
import com.gigaspaces.persistency.metadata.SpaceDocumentMapper;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import java.util.Iterator;

/**
 * @author Shadi Massalha
 */
public class MongoInitialDataLoadIterator implements DataIterator<Object> {

    private DBCursor currentCursor;
    private final MongoClientConnector mongoClient;
    private final Iterator<SpaceTypeDescriptor> types;
    private SpaceDocumentMapper<DBObject> pojoMapper;
    private final MongoSpaceDataSource mongoSpaceDataSource;

    public MongoInitialDataLoadIterator(MongoSpaceDataSource mongoSpaceDataSource, MongoClientConnector client) {
        this.mongoSpaceDataSource = mongoSpaceDataSource;
        if (client == null)
            throw new IllegalArgumentException("mongo client can not be null");

        this.mongoClient = client;
        this.types = client.getSortedTypes().iterator();
        this.currentCursor = nextDataIterator();
    }

    public void close() {
        if (currentCursor != null)
            currentCursor.close();
    }

    public boolean hasNext() {

        while (currentCursor != null && !currentCursor.hasNext()) {
            currentCursor = nextDataIterator();
        }
        return currentCursor != null;
    }

    public Object next() {
        return pojoMapper.toDocument(currentCursor.next());
    }

    public void remove() {
        currentCursor.remove();
    }

    private DBCursor nextDataIterator() {

        if (!types.hasNext())
            return null;

        SpaceTypeDescriptor typeDescriptor = types.next();

        DBObject query = mongoSpaceDataSource.getInitialQuery(typeDescriptor);

        this.pojoMapper = new DefaultSpaceDocumentMapper(typeDescriptor, mongoClient.prefferPojo());

        return mongoClient.getCollection(typeDescriptor.getTypeName()).find(query);
    }
}
