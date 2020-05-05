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

package com.gigaspaces.datasource;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * A {@link SpaceDataSource} is used for reading data into the Space upon a request from the Space
 * or on Space initialization.<br/>
 *
 * @author eitany
 * @author Idan Moyal
 * @since 9.1.1
 */
public abstract class SpaceDataSource {
    /**
     * This method is invoked on space initialization (before {@link #initialDataLoad()} is invoked)
     * and is used for introducing data types kept in the data source to the space. <p>Please note
     * that derived types should be returned after their super types from the iterator.</p> <p>It is
     * possible to create a {@link SpaceTypeDescriptor} using {@link SpaceTypeDescriptorBuilder}.</p>
     *
     * @return A {@link DataIterator} instance which contains all {@link SpaceTypeDescriptor}
     * instances to be introduced to the Space upon its initialization - null is treated as an empty
     * iterator.
     */
    public DataIterator<SpaceTypeDescriptor> initialMetadataLoad() {
        return null;
    }

    /**
     * This method is invoked after {@link #initialMetadataLoad()} and is used for pre fetching data
     * from the data source on space initialization. <p>The returned {@link DataIterator} instance
     * should contain POJOs or {@link SpaceDocument}s which their type was previously introduced to
     * the space on {@link #initialMetadataLoad()} invocation.</p>
     *
     * @return A {@link DataIterator} instance which contains all data to be written to Space upon
     * its initialization - null is treated as an empty iterator.
     */
    public DataIterator<Object> initialDataLoad() {
        return null;
    }

    /**
     * This method is invoked whenever the space needs to read data which matches the provided
     * {@link DataSourceQuery} from the space data source. <p>If this implementation doesn't
     * supports types inheritance this method will be invoked for each type in the inheritance tree
     * of the queried type.</p> <p>The implementation's type inheritance support can be determined
     * by overriding the {@link SpaceDataSource#supportsInheritance()} method.
     *
     * @param query The {@link DataSourceQuery} to get results for.
     * @return A {@link DataIterator} instance contains results for the provided {@link
     * DataSourceQuery}.
     */
    public DataIterator<Object> getDataIterator(DataSourceQuery query) {
        return null;
    }

    /**
     * This method is invoked whenever the space needs to read an entry from the data source
     * according to its Id. <p>The returned value can be either a POJO or a {@link SpaceDocument}
     * instance.</p> <p>The default implementation of this method is delegated to the {@link
     * #getDataIterator(DataSourceQuery)} method so if the data source does not have an optimized
     * way of reading an entity by its Id - the default implementation may suffice.</p>
     *
     * @param idQuery The {@link DataSourceIdQuery} to get a result for.
     * @return A result from the {@link SpaceDataSource} which matches the provided {@link
     * DataSourceIdQuery}.
     */
    public Object getById(DataSourceIdQuery idQuery) {
        final DataIterator<Object> iterator = getDataIterator((DataSourceQuery) idQuery);
        try {
            return iterator != null && iterator.hasNext() ? iterator.next()
                    : null;
        } finally {
            if (iterator != null)
                iterator.close();
        }
    }

    /**
     * This method is invoked whenever the space needs to read several entries from the data source
     * according to their ids. <p>The returned values can be either POJOs or {@link SpaceDocument}
     * instances.</p> <p>The default implementation of this method is delegated to the {@link
     * #getById(DataSourceIdQuery)} method for each id, so if the data source does not have an
     * optimized way of reading an serveral entities by their ids - the default implementation may
     * suffice.</p>
     *
     * @param idsQuery The {@link DataSourceIdsQuery} to get a result for.
     * @return A {@link DataIterator} instance contains results for the provided {@link
     * DataSourceIdsQuery}.
     */
    public DataIterator<Object> getDataIteratorByIds(DataSourceIdsQuery idsQuery) {
        List<Object> results = Collections.emptyList();
        Object[] ids = idsQuery.getIds();
        for (int i = 0; i < ids.length; i++) {
            DataSourceIdQuery dataSourceIdQuery = ((InternalDataSourceIdsQuery) idsQuery).getDataSourceIdQuery(i);
            Object result = getById(dataSourceIdQuery);
            if (result != null) {
                if (results.isEmpty())
                    results = new LinkedList<Object>();
                results.add(result);
            }
        }
        return new DataIteratorAdapter<Object>(results.iterator());
    }

    /**
     * Determines whether this implementation supports types inheritance. <p>If types inheritance is
     * not supported - {@link #getDataIterator(DataSourceQuery)} will be invoked for each derived
     * type of the query's data type. Otherwise a single {@link #getDataIterator(DataSourceQuery)}
     * invocation will be made for the query.</p>
     *
     * @return true if this {@link SpaceDataSource} implementation supports types inheritance,
     * otherwise false.
     */
    public boolean supportsInheritance() {
        return true;
    }

    protected Integer numberOfInstances;

    protected Integer instanceId;

    public void setNumberOfInstances(Integer numberOfInstances) {
        this.numberOfInstances = numberOfInstances;
    }

    public void setInstanceId(Integer instanceId) {
        this.instanceId = instanceId;
    }
}
