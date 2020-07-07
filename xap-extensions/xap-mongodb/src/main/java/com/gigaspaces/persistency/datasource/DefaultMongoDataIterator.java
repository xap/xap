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
import com.gigaspaces.persistency.metadata.DefaultSpaceDocumentMapper;
import com.gigaspaces.persistency.metadata.SpaceDocumentMapper;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * @author Shadi Massalha
 */
public class DefaultMongoDataIterator implements DataIterator<Object> {

    private DBCursor iterator;
    private SpaceDocumentMapper<DBObject> mapper;

    public DefaultMongoDataIterator(DBCursor iterator, SpaceTypeDescriptor typeDescriptor, boolean prefferPojo) {
        if (iterator == null)
            throw new IllegalArgumentException("iterator can not be null");
        if (typeDescriptor == null)
            throw new IllegalArgumentException("typeDescriptor can not be null");

        this.iterator = iterator;
        this.mapper = new DefaultSpaceDocumentMapper(typeDescriptor, prefferPojo);
    }

    public boolean hasNext() {
        return iterator.hasNext();
    }

    public Object next() {
        return mapper.toDocument(iterator.next());
    }

    public void remove() {
        iterator.remove();
    }

    public void close() {
        iterator.close();
    }
}
