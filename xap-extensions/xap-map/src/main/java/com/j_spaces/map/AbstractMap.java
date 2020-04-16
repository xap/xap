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

package com.j_spaces.map;

import com.gigaspaces.internal.client.cache.ISpaceCache;
import com.gigaspaces.internal.client.cache.SpaceCacheException;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.client.utils.SerializationUtil;
import com.gigaspaces.metadata.StorageType;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.CacheException;
import com.j_spaces.core.client.ReadModifiers;
import com.j_spaces.core.client.version.map.MapVersionTable;

import net.jini.core.lease.Lease;
import net.jini.core.transaction.Transaction;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all Map implementations.
 *
 * @author Guy Korland
 * @version 1.0
 * @since 5.0
 */
abstract public class AbstractMap implements IMap {

    final public static int NONE_COMPRESSION = 0;
    final public static int ZIP_COMPRESSION = 1;

    protected Transaction _transaction;
    protected long _timeToLive = Lease.FOREVER;
    protected long _timeout = 0;

    protected ISpaceProxy _spaceProxy;

    final protected StorageType _compression;

    protected boolean _isVersioned;

    protected MapVersionTable _entryInfos;

    //logger
    final static private Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_CACHE);

    /**
     * AbstractMap constructor.
     *
     * @param space       Space proxy or DCache proxy.
     * @param timeToLive  Time to live for <code>value</code> object in cache.
     * @param txn         if not <code>null</code> all operation will be executed with the desired
     *                    transaction.
     * @param compression NONE = 0 , ZIP = 1
     * @param isVersioned <code>true</code> if versioned
     *
     *                    To set new transaction use: setTransaction(txn) To reset the transaction
     *                    use: setTransaction(null) To get the current transaction use:
     *                    getTransaction()
     **/
    public AbstractMap(IJSpace space, long timeToLive, Transaction txn, int compression, boolean isVersioned) {
        _spaceProxy = (ISpaceProxy) space;
        _transaction = txn;
        _timeToLive = timeToLive == Lease.ANY ? Lease.FOREVER : timeToLive;
        _isVersioned = isVersioned;

        switch (compression) {
            case ZIP_COMPRESSION:
                _compression = StorageType.COMPRESSED;
                break;
            case NONE_COMPRESSION:
            default:
                _compression = StorageType.OBJECT;
        }

        setVersioned(isVersioned);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final public void setVersioned(boolean versioned) {
        _isVersioned = versioned;
        _spaceProxy.setOptimisticLocking(versioned);

        if (_isVersioned && _entryInfos == null) {
            _entryInfos = MapVersionTable.getInstance();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final public boolean isVersioned() {
        return _isVersioned;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final public void setTimeToLive(long millisecond) {
        _timeToLive = millisecond == Lease.ANY ? Lease.FOREVER : millisecond;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final public long getTimeToLive() {
        return _timeToLive;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final public void setWaitForResponse(long timeout) {
        _timeout = timeout;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    final public long getWaitForResponse() {
        return _timeout;
    }

    /**
     * @param key        key for the <code>value</code>.
     * @param value      object(~ entry)
     * @param txn        transaction
     * @param timeToLive time to leave in master cache
     * @return previous value associated with specified key.
     */
    abstract protected Object internalPut(Object key, Object value, Transaction txn, long timeToLive, long timeout) throws CacheException;

    /**
     * Gets the mapping for this key from this map if it is present.
     *
     * @param key             key whose mapping is to be removed from the map.
     * @param waitForResponse to wait for response
     * @return previous value associated with specified key.
     */
    abstract protected Object internalGet(Object key, Transaction txn, long waitForResponse, int readModifiers) throws CacheException;

    /**
     * Removes the mapping for this key from this map if it is present.
     *
     * @param key             key whose mapping is to be removed from the map.
     * @param waitForResponse to wait for response
     * @return previous value associated with specified key, or <tt>null</tt> if there was no
     * mapping for key.
     */
    abstract protected Object internalRemove(Object key, Transaction txn, long waitForResponse) throws CacheException;

    /**
     * {@inheritDoc}
     */
    @Override
    final public Object put(Object key, Object value) {
        return put(key, value, _timeToLive);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final public Object put(Object key, Object value, long timeToLive) {
        return put(key, value, null, timeToLive);
    }

    @Override
    public Object put(Object key, Object value, long timeToLive, long timeout) {
        return put(key, value, null, timeToLive, timeout);
    }

    @Override
    final public Object put(Object key, Object value, Transaction txn, long timeToLive) {
        return put(key, value, txn, timeToLive, _timeout);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final public Object put(Object key, Object value, Transaction txn, long timeToLive, long timeout) {
        // Make sure the value/key are not null
        if (value == null || key == null)
            throw new IllegalArgumentException("Key or value can not be null.");
        // GS-10859 - Make sure lease is not negative.
        if (timeToLive < 0)
            throw new IllegalArgumentException("timeToLive cannot be less than zero");

        if (!(value instanceof Serializable))
            throw new SpaceCacheException("Error putting Object " + value.getClass() + " in the cache. " + value.getClass() + " must be serializable.");

        Object compressedValue;
        try {
            compressedValue = SerializationUtil.serializeFieldValue(value, _compression);
        } catch (IOException e) {
            throw new SpaceCacheException("Failed to serialize value", e);
        }
        return internalPut(key, compressedValue, prepareTransaction(txn), timeToLive, timeout);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putAll(Map map) {
        putAll(map, getTransaction(), getTimeToLive());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putAll(Map map, Transaction txn, long timeToLive) {
       /* TODO put putMulti */
        if (map instanceof GSMapImpl)
            throw new java.lang.IllegalArgumentException("Only java.util.Map or classes derived from java.util.Map are allowed.");

        Set<Map.Entry> set = map.entrySet();
        for (Map.Entry entry : set) {
            put(entry.getKey(), entry.getValue(), txn, timeToLive);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map getAll(Collection keys) throws com.j_spaces.javax.cache.CacheException {
         /* TODO use getMulti */
        Map<Object, Object> map = new HashMap<Object, Object>(keys.size());

        for (Object key : keys) {
            map.put(key, get(key));
        }

        return map;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object get(Object key) {
        return get(key, null, _timeout, ReadModifiers.REPEATABLE_READ);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object get(Object key, long waitForResponse) {
        return get(key, null, waitForResponse, ReadModifiers.REPEATABLE_READ);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object get(Object key, Transaction txn, long waitForResponse, int readModifiers) {
        return internalGet(key, prepareTransaction(txn), waitForResponse, readModifiers);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        try {
            return _spaceProxy.count(MapEntryFactory.create(), _transaction);
        } catch (Exception e) {
            throw new SpaceCacheException("Failed to get space cache size", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final public boolean isEmpty() {
        return (size() == 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object remove(Object key) {
        return remove(key, null, _timeout);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object remove(Object key, long waitForResponse) {
        return remove(key, null, waitForResponse);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object remove(Object key, Transaction txn, long waitForResponse) {
        return internalRemove(key, prepareTransaction(txn), waitForResponse);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final public void setTransaction(Transaction transaction) {
        _transaction = transaction;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final public Transaction getTransaction() {
        return _transaction;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void load(Object key) throws com.j_spaces.javax.cache.CacheException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void loadAll(Collection keys) throws com.j_spaces.javax.cache.CacheException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final public IJSpace getMasterSpace() {
        if (_spaceProxy instanceof ISpaceCache)
            return ((ISpaceCache) _spaceProxy).getRemoteSpace();
        return _spaceProxy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    final public IJSpace getLocalSpace() {
        if (_spaceProxy instanceof ISpaceCache)
            return ((ISpaceCache) _spaceProxy).getLocalSpace();

        return null;
    }

    /**
     * Build Envelope using ClientUIDHandler utility class and adds the version if needed.
     */
    protected SpaceMapEntry buildEnvelope(Object key, Object value, String cacheID) {
        SpaceMapEntry envelope = MapEntryFactory.create(key, value, cacheID);

        // get Entry version from value if exists
        if (_isVersioned)
            envelope.setVersion(_entryInfos.getEntryVersion(value, key));

        return envelope;
    }

    protected void log(String description, Exception ex) {
        if (_logger.isDebugEnabled()) {
            if (ex != null)
                _logger.debug(toString() + " " + description + " | ", ex);
            else
                _logger.debug(description);
        }
    }

    protected void log(String description) {
        log(description, null);
    }

    private Transaction prepareTransaction(Transaction txn) {
        return txn == null ? _transaction : txn;
    }
}
