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
import com.gigaspaces.datasource.DataSourceQuery;
import com.gigaspaces.persistency.MongoClientConnector;
import com.gigaspaces.persistency.error.UnSupportedQueryException;
import com.gigaspaces.persistency.metadata.DefaultSpaceDocumentMapper;
import com.gigaspaces.persistency.metadata.SpaceDocumentMapper;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;

/**
 * @author Shadi Massalha
 */
public class MongoSqlQueryDataIterator implements DataIterator<Object> {

    private static final Log logger = LogFactory
            .getLog(MongoSqlQueryDataIterator.class);

    private final MongoClientConnector client;
    private final DataSourceQuery query;
    private final SpaceDocumentMapper<DBObject> pojoMapper;
    private DBCursor cursor;

    public MongoSqlQueryDataIterator(MongoClientConnector client,
                                     DataSourceQuery query) {
        if (client == null)
            throw new IllegalArgumentException(
                    "Argument cannot be null - client");
        if (query == null)
            throw new IllegalArgumentException(
                    "Argument cannot be null - query");
        if (!(query.supportsAsSQLQuery() || query.supportsTemplateAsDocument()))
            throw new UnSupportedQueryException("not sql query");

        this.client = client;
        this.query = query;
        this.pojoMapper = new DefaultSpaceDocumentMapper(
                query.getTypeDescriptor(), client.prefferPojo());
    }

    public boolean hasNext() {
        if (cursor == null) {
            init();
        }

        return cursor.hasNext();
    }

    public Object next() {

        return pojoMapper.toDocument(cursor.next());
    }

    private void init() {
        DBCollection collection = client.getCollection(query
                .getTypeDescriptor().getTypeName());

        BasicDBObjectBuilder q = BasicDBObjectBuilder.start();

        logger.debug(query);

        if (query.supportsAsSQLQuery())
            q = MongoQueryFactory.create(query);
        else if (query.supportsTemplateAsDocument()) {
            Map m = new DefaultSpaceDocumentMapper(query.getTypeDescriptor(), client.prefferPojo())
                    .toDBObject(query.getTemplateAsDocument()).toMap();

            q = BasicDBObjectBuilder.start(m);
        }

        cursor = collection.find(q.get());
    }

    public void remove() {
        if (cursor != null)
            cursor.remove();
    }

    public void close() {
        if (cursor != null)
            cursor.close();
    }
}
