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
package com.gigaspaces.internal.server.space.schema_evolution;

import com.gigaspaces.datasource.*;
import com.gigaspaces.metadata.SpaceTypeDescriptor;

public class SchemaAdapterSpaceDataSource extends SpaceDataSource {
    private final SpaceDataSource spaceDataSource;
    private final SpaceTypeSchemaAdapterContainer spaceTypeSchemaAdapterContainer;

    public SchemaAdapterSpaceDataSource(SpaceDataSource spaceDataSource, SpaceTypeSchemaAdapterContainer spaceTypeSchemaAdapterContainer) {
        this.spaceDataSource = spaceDataSource;
        this.spaceTypeSchemaAdapterContainer = spaceTypeSchemaAdapterContainer;
    }

    @Override
    public DataIterator<SpaceTypeDescriptor> initialMetadataLoad() {
        return new SchemaAdapterDataIterator<>(spaceDataSource.initialMetadataLoad());
    }

    @Override
    public DataIterator<Object> initialDataLoad() {
        return new SchemaAdapterDataIterator<>(spaceDataSource.initialDataLoad());
    }

    @Override
    public DataIterator<Object> getDataIterator(DataSourceQuery query) {
        return new SchemaAdapterDataIterator<>(spaceDataSource.getDataIterator(query));
    }

    @Override
    public Object getById(DataSourceIdQuery idQuery) {
        return spaceTypeSchemaAdapterContainer.adapt(spaceDataSource.getById(idQuery));
    }

    @Override
    public DataIterator<Object> getDataIteratorByIds(DataSourceIdsQuery idsQuery) {
        return new SchemaAdapterDataIterator<>(spaceDataSource.getDataIteratorByIds(idsQuery));
    }

    @Override
    public boolean supportsInheritance() {
        return spaceDataSource.supportsInheritance();
    }

    class SchemaAdapterDataIterator<T> implements DataIterator<T>{
        DataIterator<T> dataIterator;

        public SchemaAdapterDataIterator(DataIterator<T> dataIterator) {
            this.dataIterator = dataIterator;
        }

        @Override
        public void close() {
            if(dataIterator != null)
                dataIterator.close();
        }

        @Override
        public boolean hasNext() {
            if(dataIterator != null)
                return dataIterator.hasNext();
            return false;
        }

        @Override
        public T next() {
            if(dataIterator == null)
                return null;
            return (T) spaceTypeSchemaAdapterContainer.adapt(dataIterator.next()); }
    }
}
