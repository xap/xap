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


package com.j_spaces.sadapter.datasource;

import com.gigaspaces.datasource.BulkDataPersister;
import com.gigaspaces.datasource.BulkItem;
import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.DataPersister;
import com.gigaspaces.datasource.DataProvider;
import com.gigaspaces.datasource.DataSourceException;
import com.gigaspaces.datasource.ManagedDataSource;
import com.gigaspaces.datasource.SQLDataProvider;
import com.gigaspaces.logger.LogUtils;
import com.j_spaces.core.client.SQLQuery;

import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Data Storage is a delegator over user defined data source-interface implementation. <p>
 * Operations defined by each interface are delegated to the implementation - only if it exists.
 *
 * {@link DataStorage} supports both space API and the JCache API
 *
 * @author anna
 * @version 1.0
 * @since 6.0
 */
@com.gigaspaces.api.InternalApi
public class DataStorage<T>
        implements DataProvider<T>,
        SQLDataProvider<T>, DataPersister<T>,
        BulkDataPersister, ManagedDataSource<T> {
    private final static Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_PERSISTENT);


    // Space data interfaces
    private final DataProvider<T> _saDataProvider;
    private final DataPersister<T> _saDataPersister;
    private SQLDataProvider<T> _saSQLDataProvider;
    private final BulkDataPersister _saBulkDataPersister;
    private ManagedDataSource<T> _saManagedDataSource;

    /**
     * DataStorage instantiation of cache and space data interfaces implementation,
     *
     * @param store The data source instance
     */
    public DataStorage(Object store) {

        // Data API
        if (store instanceof DataProvider)
            _saDataProvider = (DataProvider<T>) store;
        else
            _saDataProvider = null;

        if (store instanceof DataPersister)
            _saDataPersister = (DataPersister<T>) store;
        else
            _saDataPersister = null;

        if (store instanceof SQLDataProvider)
            _saSQLDataProvider = (SQLDataProvider<T>) store;
        else
            _saSQLDataProvider = null;

        if (store instanceof BulkDataPersister)
            _saBulkDataPersister = (BulkDataPersister) store;
        else
            _saBulkDataPersister = null;

        if (store instanceof ManagedDataSource)
            _saManagedDataSource = (ManagedDataSource) store;
        else
            _saManagedDataSource = null;


        if (_logger.isDebugEnabled()) {

            _logger.debug("\n\t Space Data Storage <" + store + "> Loaded"
                    + "\n\t\t " + DataProvider.class.getSimpleName() + "              : " + (isDataProvider() ? "Implemented" : "-")
                    + "\n\t\t " + DataPersister.class.getSimpleName() + "             : " + (isDataPersister() ? "Implemented" : "-")
                    + "\n\t\t " + BulkDataPersister.class.getSimpleName() + "         : " + (isBulkDataPersister() ? "Implemented" : "-")
                    + "\n\t\t " + SQLDataProvider.class.getSimpleName() + "           : " + (isSQLDataProvider() ? "Implemented" : "-")
                    + "\n\t\t " + ManagedDataSource.class.getSimpleName() + "         : " + (isManagedDataSource() ? "Implemented" : "-")
                    + "\n");
        }

    }


    /**
     * @return <code>true</code> if DataStorage implements {@link DataProvider}; <code>false</code>
     * otherwise.
     */
    public boolean isDataProvider() {
        return _saDataProvider != null;
    }

    /**
     * @return <code>true</code> if DataStorage implements {@link DataPersister}; <code>false</code>
     * otherwise.
     */
    public boolean isDataPersister() {
        return _saDataPersister != null;
    }

    /**
     * @return <code>true</code> if DataStorage implements SQLDataProvider; <code>false</code>
     * otherwise.
     */
    public boolean isSQLDataProvider() {
        return _saSQLDataProvider != null;
    }

    /**
     * @return <code>true</code> if DataStorage implements {@link BulkDataPersister};
     * <code>false</code> otherwise.
     */
    public boolean isBulkDataPersister() {
        return _saBulkDataPersister != null;
    }

    /**
     * @return <code>true</code> if DataStorage implements {@link ManagedDataSource};
     * <code>false</code> otherwise.
     */
    public boolean isManagedDataSource() {
        return _saManagedDataSource != null;
    }


    /* (non-Javadoc)
     * @see com.gigaspaces.datasource.DataProvider#read(java.lang.Object)
     */
    public T read(T template) throws DataSourceException {
        if (_logger.isDebugEnabled())
            LogUtils.entering(_logger, DataStorage.class,"DataProvider#read", template);

        T result = _saDataProvider.read(template);

        if (_logger.isDebugEnabled())
            LogUtils.exiting(_logger, DataStorage.class,"DataProvider#read", result);

        return result;
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.datasource.DataProvider#iterator(java.lang.Object)
     */
    public DataIterator<T> iterator(T template)
            throws DataSourceException {
        if (_logger.isDebugEnabled())
            LogUtils.entering(_logger, DataStorage.class, "DataProvider#iterator", template);

        DataIterator<T> iter = _saDataProvider.iterator(template);

        if (_logger.isDebugEnabled())
            LogUtils.exiting(_logger, DataStorage.class, "DataProvider#iterator", iter);

        return iter;
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.datasource.SQLDataProvider#iterator(com.j_spaces.core.client.SQLQuery)
     */
    public DataIterator<T> iterator(SQLQuery<T> query)
            throws DataSourceException {
        if (_logger.isDebugEnabled())
            LogUtils.entering(_logger, DataStorage.class, "SQLDataProvider#iterator", query);

        DataIterator<T> iter = _saSQLDataProvider.iterator(query);

        if (_logger.isDebugEnabled())
            LogUtils.exiting(_logger, DataStorage.class, "SQLDataProvider#iterator", iter);

        return iter;
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.datasource.DataPersister#remove(java.lang.Object)
     */
    public void remove(T object) throws DataSourceException {
        if (_logger.isDebugEnabled())
            LogUtils.entering(_logger, DataStorage.class,"DataPersister#remove", object);

        _saDataPersister.remove(object);

    }

    /* (non-Javadoc)
     * @see com.gigaspaces.datasource.DataPersister#update(java.lang.Object)
     */
    public void update(T object) throws DataSourceException {
        if (_logger.isDebugEnabled())
            LogUtils.entering(_logger, DataStorage.class, "DataPersister#update", object);

        _saDataPersister.update(object);

    }

    /* (non-Javadoc)
     * @see com.gigaspaces.datasource.DataPersister#write(java.lang.Object)
     */
    public void write(T object) throws DataSourceException {
        if (_logger.isDebugEnabled())
            LogUtils.entering(_logger, DataStorage.class, "DataPersister#write", object);

        _saDataPersister.write(object);
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.datasource.DataPersister#writeBatch(java.util.List)
     */
    public void writeBatch(List<T> objects)
            throws DataSourceException {
        if (_logger.isDebugEnabled())
            LogUtils.entering(_logger, DataStorage.class, "DataPersister#writeBatch", objects);

        _saDataPersister.writeBatch(objects);
    }


    /* (non-Javadoc)
     * @see com.gigaspaces.datasource.BulkDataPersister#executeBulk(java.util.List)
     */
    public void executeBulk(List<BulkItem> bulk)
            throws DataSourceException {
        if (_logger.isDebugEnabled())
            LogUtils.entering(_logger, DataStorage.class, "BulkDataPersister#executeBulk", bulk);

        _saBulkDataPersister.executeBulk(bulk);
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.datasource.ManagedDataSource#init(java.util.Properties)
     */
    public void init(Properties configuration)
            throws DataSourceException {

        if (_logger.isDebugEnabled())
            LogUtils.entering(_logger, DataStorage.class,"ManagedDataSource#init", configuration);

        _saManagedDataSource.init(configuration);
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.datasource.ManagedDataSource#shutdown()
     */
    public void shutdown() throws DataSourceException {
        if (_logger.isDebugEnabled())
            LogUtils.entering(_logger, DataStorage.class, "ManagedDataSource#shutdown");

        _saManagedDataSource.shutdown();

    }


    /**
     * Returns true is the underlying data source doesn't support data changes
     *
     * @return true if Data Storage is read elsy, esle return false
     */
    public boolean isReadOnly() {

        return !(isDataPersister() || isBulkDataPersister());
    }


    /* (non-Javadoc)
     * @see com.gigaspaces.datasource.ManagedDataSource#iterator()
     */
    public DataIterator<T> initialLoad() throws DataSourceException {
        if (_logger.isDebugEnabled())
            LogUtils.entering(_logger, DataStorage.class, "ManagedDataSource#initialLoad");

        DataIterator<T> iter = _saManagedDataSource.initialLoad();

        if (_logger.isDebugEnabled())
            LogUtils.exiting(_logger, DataStorage.class, "ManagedDataSource#initialLoad");

        return iter;
    }


    /**
     * Override the internal sql data provider Method is synchronized to force the thread to flush
     * its memory
     */
    public synchronized void setSQLDataProvider(SQLDataProvider<T> sqlDataProvider) {
        _saSQLDataProvider = sqlDataProvider;
    }

    /**
     * Override the internal managed data source Method is synchronized to force the thread to flush
     * its memory
     */
    public synchronized void setManagedDataSource(ManagedDataSource<T> managedDataSource) {
        _saManagedDataSource = managedDataSource;
    }

}
