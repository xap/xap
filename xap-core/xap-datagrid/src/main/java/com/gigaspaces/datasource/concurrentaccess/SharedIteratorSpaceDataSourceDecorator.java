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

package com.gigaspaces.datasource.concurrentaccess;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.DataSourceException;
import com.gigaspaces.datasource.DataSourceIdQuery;
import com.gigaspaces.datasource.DataSourceIdsQuery;
import com.gigaspaces.datasource.DataSourceQuery;
import com.gigaspaces.datasource.DataSourceSQLQuery;
import com.gigaspaces.datasource.SpaceDataSource;
import com.gigaspaces.datasource.SpaceDataSourceException;
import com.gigaspaces.metadata.SpaceTypeDescriptor;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decorates a {@link SpaceDataSource} with additional property, concurrent request for an iterator
 * which is based on the same query will only open one underlying iterator against the decorates
 * provider, this iterator will be shared amont the concurrent requests as if each of them has its
 * own iterator. the purpose is to reduce the load on the data source.
 *
 * @author eitany
 * @author Idan Moyal
 * @since 9.5
 */
@com.gigaspaces.api.InternalApi
public class SharedIteratorSpaceDataSourceDecorator
        extends SpaceDataSource
        implements ISharedDataIteratorSourceStateChangedListener<Object> {
    private final static Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_PERSISTENT_SHARED_ITERATOR);

    private final ConcurrentHashMap<DataSourceSQLQuery, SharedDataIteratorSource<Object>> _queryToSources;
    private final ConcurrentHashMap<SharedDataIteratorSource<Object>, DataSourceSQLQuery> _sourcesToQuery;
    private final long _timeToLive;
    private final SpaceDataSource _spaceDataSource;


    /**
     * Create a new instance
     *
     * @param sqlDataProvider provider to decorate
     * @param timeToLive      liveness time of a created mediators, once this time expired this
     *                        mediator will not be used for new iterator creations
     */
    public SharedIteratorSpaceDataSourceDecorator(SpaceDataSource spaceDataSource, long timeToLive) {
        _spaceDataSource = spaceDataSource;
        _timeToLive = timeToLive;
        _queryToSources = new ConcurrentHashMap<DataSourceSQLQuery, SharedDataIteratorSource<Object>>();
        _sourcesToQuery = new ConcurrentHashMap<SharedDataIteratorSource<Object>, DataSourceSQLQuery>();
    }

    @Override
    public DataIterator<Object> getDataIterator(DataSourceQuery query) {
        try {
            if (!query.supportsAsSQLQuery())
                return _spaceDataSource.getDataIterator(query);

            final DataSourceSQLQuery dataSourceSQLQuery = query.getAsSQLQuery();
            SharedDataIteratorSource<Object> sharedDataIteratorSource = _queryToSources.get(dataSourceSQLQuery);
            if (sharedDataIteratorSource == null)
                //If there's no source, create a new one
                sharedDataIteratorSource = createSource(query, dataSourceSQLQuery);

            while (true) {
                try {
                    return sharedDataIteratorSource.getIterator();
                } catch (SharedDataIteratorSourceClosedException e) {
                    if (_logger.isTraceEnabled())
                        _logger.trace("shared iterator source is already closed, creating a new one");
                    //The mediator was closed exactly at this point, create a new one
                    sharedDataIteratorSource = createSource(query, dataSourceSQLQuery);
                } catch (SharedDataIteratorSourceExpiredException e) {
                    if (_logger.isTraceEnabled())
                        _logger.trace("shared iterator source is already expired, creating a new one");
                    //The mediator has expired exactly at this point, create a new one
                    sharedDataIteratorSource = createSource(query, dataSourceSQLQuery);
                }
            }
        } catch (DataSourceException e) {
            throw new SpaceDataSourceException(e);
        }
    }

    public void shutdown() throws DataSourceException {
        if (_logger.isDebugEnabled())
            _logger.debug("shutdown shared iterator SQL data provider decorator");
        _queryToSources.clear();
        _sourcesToQuery.clear();
    }

    private SharedDataIteratorSource<Object> createSource(final DataSourceQuery dataSourceQuery, final DataSourceSQLQuery dataSourceSQLQuery) throws DataSourceException {
        //create a new mediator over a data iterator created by the underlying provider
        SharedDataIteratorSource<Object> sharedDataIteratorSource = new SharedDataIteratorSource<Object>(dataSourceSQLQuery, new ISourceDataIteratorProvider<Object>() {
            public DataIterator<Object> getSourceIterator() throws DataSourceException {
                return _spaceDataSource.getDataIterator(dataSourceQuery);
            }
        }
                , _timeToLive);
        //subscribe to notifications
        sharedDataIteratorSource.addStateChangedListener(this);
        SharedDataIteratorSource<Object> previousMediator = _queryToSources.putIfAbsent(dataSourceSQLQuery, sharedDataIteratorSource);
        if (previousMediator != null)
            sharedDataIteratorSource = previousMediator;
        else {
            if (_logger.isTraceEnabled())
                _logger.trace("create new shared iterator source for query " + dataSourceSQLQuery.toString());
            _sourcesToQuery.put(sharedDataIteratorSource, dataSourceSQLQuery);
        }
        return sharedDataIteratorSource;
    }

    public void onClosed(SharedDataIteratorSource<Object> sender) {
        handleSourceStateChangedEvent(sender);
    }

    public void onExpired(SharedDataIteratorSource<Object> sender) {
        handleSourceStateChangedEvent(sender);
    }

    private void handleSourceStateChangedEvent(SharedDataIteratorSource<Object> sender) {
        DataSourceSQLQuery query = _sourcesToQuery.remove(sender);
        if (_logger.isTraceEnabled() && query != null)
            _logger.trace("shared iterator source is closed or expired, detaching from local table [" + query.toString() + "]");
        if (query != null)
            _queryToSources.remove(query);
    }

    @Override
    public DataIterator<Object> initialDataLoad() {
        return _spaceDataSource.initialDataLoad();
    }

    @Override
    public DataIterator<SpaceTypeDescriptor> initialMetadataLoad() {
        return _spaceDataSource.initialMetadataLoad();
    }

    @Override
    public Object getById(DataSourceIdQuery idQuery) {
        return _spaceDataSource.getById(idQuery);
    }

    @Override
    public DataIterator<Object> getDataIteratorByIds(DataSourceIdsQuery idsQuery) {
        return _spaceDataSource.getDataIteratorByIds(idsQuery);
    }

    @Override
    public boolean supportsInheritance() {
        return _spaceDataSource.supportsInheritance();
    }


}
