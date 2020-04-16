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
import com.gigaspaces.datasource.SQLDataProvider;
import com.j_spaces.core.client.SQLQuery;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decorates a {@link SQLDataProvider} with additional property, concurrent request for an iterator
 * which is based on the same query will only open one underlying iterator against the decorates
 * provider, this iterator will be shared amont the concurrent requests as if each of them has its
 * own iterator. the purpose is to reduce the load on the data source.
 *
 * @author eitany
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class SharedIteratorSQLDataProviderDecorator<T> implements SQLDataProvider<T>, ISharedDataIteratorSourceStateChangedListener<T> {
    private final static Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_PERSISTENT_SHARED_ITERATOR);

    private final SQLDataProvider<T> _sqlDataProvider;
    private final ConcurrentHashMap<SQLQuery<T>, SharedDataIteratorSource<T>> _queryToSources;
    private final ConcurrentHashMap<SharedDataIteratorSource<T>, SQLQuery<T>> _sourcesToQuery;
    private final long _timeToLive;

    /**
     * Create a new instance
     *
     * @param sqlDataProvider provider to decorate
     * @param timeToLive      liveness time of a created mediators, once this time expired this
     *                        mediator will not be used for new iterator creations
     */
    public SharedIteratorSQLDataProviderDecorator(SQLDataProvider<T> sqlDataProvider, long timeToLive) {
        _sqlDataProvider = sqlDataProvider;
        _timeToLive = timeToLive;
        _queryToSources = new ConcurrentHashMap<SQLQuery<T>, SharedDataIteratorSource<T>>();
        _sourcesToQuery = new ConcurrentHashMap<SharedDataIteratorSource<T>, SQLQuery<T>>();
    }

    public DataIterator<T> iterator(SQLQuery<T> query) throws DataSourceException {
        SharedDataIteratorSource<T> sharedDataIteratorSource = _queryToSources.get(query);
        if (sharedDataIteratorSource == null)
            //If there's no source, create a new one
            sharedDataIteratorSource = createSource(query);

        while (true) {
            try {
                return sharedDataIteratorSource.getIterator();
            } catch (SharedDataIteratorSourceClosedException e) {
                if (_logger.isTraceEnabled())
                    _logger.trace("shared iterator source is already closed, creating a new one");
                //The mediator was closed exactly at this point, create a new one
                sharedDataIteratorSource = createSource(query);
            } catch (SharedDataIteratorSourceExpiredException e) {
                if (_logger.isTraceEnabled())
                    _logger.trace("shared iterator source is already expired, creating a new one");
                //The mediator has expired exactly at this point, create a new one
                sharedDataIteratorSource = createSource(query);
            }
        }
    }

    public void init(Properties prop) throws DataSourceException {
        if (_logger.isDebugEnabled())
            _logger.debug("init shared iterator SQL data provider decorator");
        _sqlDataProvider.init(prop);
    }

    public DataIterator<T> initialLoad() throws DataSourceException {
        if (_logger.isTraceEnabled())
            _logger.trace("initialLoad called on shared iterator SQL data provider decorator");
        return _sqlDataProvider.initialLoad();
    }

    public void shutdown() throws DataSourceException {
        if (_logger.isDebugEnabled())
            _logger.debug("shutdown shared iterator SQL data provider decorator");
        _queryToSources.clear();
        _sourcesToQuery.clear();
        _sqlDataProvider.shutdown();
    }

    private SharedDataIteratorSource<T> createSource(final SQLQuery<T> query) throws DataSourceException {
        //create a new mediator over a data iterator created by the underlying provider
        SharedDataIteratorSource<T> sharedDataIteratorSource = new SharedDataIteratorSource<T>(query, new ISourceDataIteratorProvider<T>() {
            public DataIterator<T> getSourceIterator() throws DataSourceException {
                return _sqlDataProvider.iterator(query);
            }
        }
                , _timeToLive);
        //subscribe to notifications
        sharedDataIteratorSource.addStateChangedListener(this);
        SharedDataIteratorSource<T> previousMediator = _queryToSources.putIfAbsent(query, sharedDataIteratorSource);
        if (previousMediator != null)
            sharedDataIteratorSource = previousMediator;
        else {
            if (_logger.isTraceEnabled())
                _logger.trace("create new shared iterator source for query " + query.toString());
            _sourcesToQuery.put(sharedDataIteratorSource, query);
        }
        return sharedDataIteratorSource;
    }

    public void onClosed(SharedDataIteratorSource<T> sender) {
        handleSourceStateChangedEvent(sender);
    }

    public void onExpired(SharedDataIteratorSource<T> sender) {
        handleSourceStateChangedEvent(sender);
    }

    private void handleSourceStateChangedEvent(SharedDataIteratorSource<T> sender) {
        SQLQuery<T> query = _sourcesToQuery.remove(sender);
        if (_logger.isTraceEnabled())
            _logger.trace("shared iterator source is closed or expired, detaching from local table [" + query.toString() + "]");
        if (query != null)
            _queryToSources.remove(query);
    }

}
