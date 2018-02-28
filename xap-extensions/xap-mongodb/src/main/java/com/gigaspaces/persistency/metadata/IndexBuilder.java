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

package com.gigaspaces.persistency.metadata;

import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.gigaspaces.persistency.MongoClientConnector;
import com.gigaspaces.sync.AddIndexData;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import java.util.Map;

public class IndexBuilder {

    private final MongoClientConnector client;

    public IndexBuilder(MongoClientConnector client) {
        this.client = client;
    }

    public void ensureIndexes(SpaceTypeDescriptor spaceTypeDescriptor) {

        Map<String, SpaceIndex> indexes = spaceTypeDescriptor.getIndexes();

        String id = spaceTypeDescriptor.getIdPropertyName();
        String routing = spaceTypeDescriptor.getRoutingPropertyName();

        for (SpaceIndex idx : indexes.values()) {

            if (idx.getIndexType() == SpaceIndexType.NONE
                    || idx.getName().equals(id)
                    || idx.getName().equals(routing))
                continue;

            createIndex(spaceTypeDescriptor.getTypeName(), idx);
        }

        if (id != null && !id.equals(routing)) {
            createIndex(spaceTypeDescriptor.getTypeName(), routing,
                    SpaceIndexType.EQUAL, BasicDBObjectBuilder.start());
        }
    }

    public void ensureIndexes(AddIndexData addIndexData) {
        for (SpaceIndex idx : addIndexData.getIndexes()) {

            if (idx.getIndexType() == SpaceIndexType.NONE)
                continue;

            createIndex(addIndexData.getTypeName(), idx);
        }
    }

    private void createIndex(String collectionName, SpaceIndex idx) {
        createIndex(collectionName, idx.getName(), idx.getIndexType(),
                BasicDBObjectBuilder.start());
    }

    private void createIndex(String typeSimpleName, String routing,
                             SpaceIndexType type, BasicDBObjectBuilder option) {
        DBCollection c = client.getCollection(typeSimpleName);

        DBObject key;

        if (type == SpaceIndexType.BASIC || type == SpaceIndexType.EQUAL) {
            key = BasicDBObjectBuilder.start(routing, "hashed").get();
        } else {
            key = BasicDBObjectBuilder.start(routing, 1).get();
        }

        c.createIndex(key, option.get());
    }
}
