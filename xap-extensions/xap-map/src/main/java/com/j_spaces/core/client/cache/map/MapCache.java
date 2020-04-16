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

/*
 * Created on 02/06/2005
 */
package com.j_spaces.core.client.cache.map;

import com.gigaspaces.events.AbstractDataEventSession;
import com.gigaspaces.events.DataEventSessionFactory;
import com.gigaspaces.events.EventSessionConfig;
import com.gigaspaces.events.NotifyActionType;
import com.gigaspaces.internal.client.cache.SpaceCacheException;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.client.utils.SerializationUtil;
import com.gigaspaces.internal.server.space.SpaceConfigReader;
import com.j_spaces.core.Constants;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.MemoryManager;
import com.j_spaces.core.admin.IRemoteJSpaceAdmin;
import com.j_spaces.core.admin.SpaceConfig;
import com.j_spaces.core.client.CacheTimeoutException;
import com.j_spaces.core.client.EntryArrivedRemoteEvent;
import com.j_spaces.core.client.EntryNotInSpaceException;
import com.j_spaces.core.client.FinderException;
import com.j_spaces.core.client.NotifyModifiers;
import com.j_spaces.core.client.OperationTimeoutException;
import com.j_spaces.core.client.ReadModifiers;
import com.j_spaces.core.client.SpaceFinder;
import com.j_spaces.core.client.SpaceURL;
import com.j_spaces.core.client.UpdateModifiers;
import com.j_spaces.javax.cache.CacheEntry;
import com.j_spaces.javax.cache.CacheException;
import com.j_spaces.javax.cache.CacheListener;
import com.j_spaces.javax.cache.EvictionStrategy;
import com.j_spaces.kernel.JSpaceUtilities;
import com.j_spaces.kernel.SizeConcurrentHashMap;
import com.j_spaces.kernel.log.JProperties;
import com.j_spaces.kernel.weaklistener.WeakLeaseListener;
import com.j_spaces.kernel.weaklistener.WeakRemoteEventListener;
import com.j_spaces.map.AbstractMap;
import com.j_spaces.map.MapEntryFactory;
import com.j_spaces.map.SpaceMapEntry;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.event.RemoteEvent;
import net.jini.core.event.RemoteEventListener;
import net.jini.core.event.UnknownEventException;
import net.jini.core.transaction.Transaction;
import net.jini.lease.LeaseListener;
import net.jini.lease.LeaseRenewalEvent;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides a lightweight implementation to GigaSpaces IMap interface (A JCache standard
 * extension), for Distributed Caching using java.util.Map API.
 *
 * Simply call Map.get and Map.put methods to store and retrieve values into or from the cache.
 *
 * @author Guy Korland
 * @version 1.0
 * @since 5.0
 */
final public class MapCache extends AbstractMap implements RemoteEventListener, LeaseListener {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_CACHE);
    /**
     * The default number of retires to reconnect to server when fault is discovered.
     */
    public static final int DEFAULT_MAX_CONNECTION_RETRY = 3;

    /**
     * how long the entries will live in the local space (~cache).
     */
    private int _maxConnectionRetry = DEFAULT_MAX_CONNECTION_RETRY;
    private long _delayRetryTime = 1000 * 5;
    private static final long DEFAULT_TEMPLATE_LEASE = 1000 * 60;

    private final MemoryManager _memoryManager;

    private final SizeConcurrentHashMap<Object, CacheEntry> _cache;

    // TODO should be updated
    //final private CacheStatistics            _cacheStatistics;
    private final Set<CacheListener> _cacheListenerTable;
    private final int _updateMode;
    private final EvictionStrategy _evictionStrategy;
    private final String _cacheID;
    private final boolean _putFirst;
    private final int _sizeLimit;
    private final String _spaceURL;

    private final EventSessionConfig _sessionConfig;
    private AbstractDataEventSession _eventSession;

    /**
     * Constructor.
     *
     * @param space            proxy to space
     * @param isVersioned      <code>true</code> if a version should be kept for the values
     * @param updateMode       <code>SpaceURL.UPDATE_MODE_PULL</code> or <code>SpaceURL.UPDATE_MODE_PUSH</code>
     * @param evictionStrategy eviction strategy to use in Cache
     * @param putFirst         <code>true</code> if an unloaded value should be kept in cache on its
     *                         first put
     * @throws RemoteException might be thrown in case of network exception or server connection
     *                         fail
     */
    public MapCache(IJSpace space, boolean isVersioned, int updateMode, EvictionStrategy evictionStrategy,
                    boolean putFirst, int sizeLimit, int compression)
            throws RemoteException {
        super(space, -1, null, compression, isVersioned);
        String containerName = space.getContainerName();
        String fullSpaceName = JSpaceUtilities.createFullSpaceName(containerName, space.getName());
        if (JProperties.getSpaceProperties(fullSpaceName) == null) {
            SpaceConfig spProp = ((IRemoteJSpaceAdmin) ((ISpaceProxy) space).getPrivilegedAdmin()).getConfig();
            JProperties.setSpaceProperties(fullSpaceName, spProp.getDCacheProperties());
        }

        EvictionCacheManager cacheManager = new EvictionCacheManager(evictionStrategy, this);
        String dCacheSpaceName = fullSpaceName + Constants.DCache.SPACE_SUFFIX;
        _memoryManager = new MemoryManager(dCacheSpaceName, containerName, cacheManager, null, true);

        _sizeLimit = sizeLimit;
        _spaceURL = space.getFinderURL().getURL();

        // Create UNIQUE Cache ID
        if (space instanceof IDirectSpaceProxy)
            _cacheID = ((IDirectSpaceProxy) space).getRemoteJSpace().getUniqueID();
        else {
            //TODO
            //      else if( space instanceof LocalCacheImpl )
            //         _cacheID = ((LocalCacheImpl)space).getRemoteSpace().getUniqueID();
            _cacheID = "";
        }
        _cache = new SizeConcurrentHashMap<Object, CacheEntry>();

        // TODO GuyK: change data structure
        _cacheListenerTable = Collections.synchronizedSet(new HashSet<CacheListener>());
        // _cacheStatistics     = new MapCacheStatistics( 0);
        _putFirst = putFirst;
        _updateMode = updateMode;
        _evictionStrategy = evictionStrategy;

        SpaceConfigReader configReader = new SpaceConfigReader(dCacheSpaceName);
        String findRetryConn = configReader.getSpaceProperty(Constants.DCache.RETRY_CONNECTIONS_PROP, null);
        String delayRetryConn = configReader.getSpaceProperty(Constants.DCache.DELAY_BETWEEN_RETRIES_PROP, null);
        _maxConnectionRetry = (findRetryConn != null) ? Integer.parseInt(findRetryConn) : _maxConnectionRetry;
        _delayRetryTime = (delayRetryConn != null) ? Long.parseLong(delayRetryConn) : _delayRetryTime;

        _sessionConfig = new EventSessionConfig();
        _sessionConfig.setAutoRenew(true, new WeakLeaseListener(this));
        _eventSession = initEventSession();
    }

    private AbstractDataEventSession initEventSession() throws RemoteException {
        final Object template = MapEntryFactory.create();
        RemoteEventListener listener = new WeakRemoteEventListener(this);
        NotifyActionType notifyType = NotifyActionType.NOTIFY_LEASE_EXPIRATION.or(NotifyActionType.NOTIFY_TAKE.or(NotifyActionType.NOTIFY_UPDATE));
        AbstractDataEventSession eventSession = (AbstractDataEventSession) DataEventSessionFactory.create(
                _spaceProxy, _sessionConfig);
        eventSession.addListener(template, listener, DEFAULT_TEMPLATE_LEASE, notifyType);
        return eventSession;
    }

    /*
     * (non-Javadoc)
     * @see com.j_spaces.map.AbstractMap#internalPut(java.lang.Object, java.lang.Object, com.j_spaces.map.Attribute[], long)
     */
    @Override
    protected Object internalPut(Object key, Object value, Transaction txn, long timeToLive, long timeout) {
        SpaceMapEntry newEnvelope = buildEnvelope(key, value, _cacheID);

        try {
            //TODO support write behind
            SpaceMapEntry oldEntry = (SpaceMapEntry) _spaceProxy.update(newEnvelope, txn, timeToLive,
                    timeout, UpdateModifiers.UPDATE_OR_WRITE);

            // build old value, prepare old before saving new version to avoid issues with embedded
            Object oldValue = oldEntry == null ? null : prepareValue(key, oldEntry);

            if (_isVersioned) {
                /* save the value version which was passed as parameter */
                _entryInfos.setEntryVersion(value, key, newEnvelope.getVersion());
            }

            if (txn == null) {
                // add to eviction Strategy
                CacheEntry cacheEntry = _evictionStrategy.createEntry(key, value, Long.MAX_VALUE, newEnvelope.getVersion());

                // put to local cache
                if (_isVersioned) {
                    putLocalVersioned(newEnvelope, key, value, cacheEntry);
                } else {
                    putLocalNotVersioned(key, cacheEntry);
                }

                _memoryManager.monitorMemoryUsage(true);

                // call listeners on put
                if (!_cacheListenerTable.isEmpty()) {
                    for (CacheListener listener : _cacheListenerTable) {
                        listener.onPut(key);
                    }
                }
            } else {
                evict(key);
            }
            // return old value
            return oldValue;
        } catch (OperationTimeoutException e) {
            throw new CacheTimeoutException(key);
        } catch (Exception e) {
            throw new SpaceCacheException("Failed to put value in space cache", e);
        }
    }

    /**
     * Save version value in local cache
     */
    private void putLocalVersioned(SpaceMapEntry newEnvelope, Object key,
                                   Object value, CacheEntry cacheEntry) {

        // update local cache
        VersionCacheEntry versionEntry = new VersionCacheEntry(newEnvelope.getVersion());
        boolean replaced = _cache.replace(key, versionEntry, cacheEntry);

        //key not in cache (!isChecked()) & should put first
        if (!versionEntry.isChecked() && _putFirst) {
            if (_cache.size() >= _sizeLimit)
                _evictionStrategy.evict(this);

            Object old = _cache.putIfAbsent(key, cacheEntry); // try to put

            if (old != null) // try to update again
            {
                replaced = _cache.replace(key, versionEntry, cacheEntry);
                // if replaced=false shouldn't be saved in local cache
            }
        }

        if (replaced) // remove old entry from eviction
        {
            Object entry = versionEntry.getOldEntry();
            if (!(entry instanceof DummyCacheEntry))
                _evictionStrategy.discardEntry((CacheEntry) entry);
        }
    }

    /**
     * Save non-vesrioned value in local cache
     */
    private void putLocalNotVersioned(Object key, CacheEntry cacheEntry) {
        //update local cache
        CacheEntry oldCacheEntry;
        if (_putFirst) {
            oldCacheEntry = _cache.put(key, cacheEntry);
        } else {
            oldCacheEntry = _cache.replace(key, cacheEntry);
        }


        if (oldCacheEntry != null) // remove old entry from eviction
        {
            if (!(oldCacheEntry instanceof DummyCacheEntry))
                _evictionStrategy.discardEntry(oldCacheEntry);
        } else if (_putFirst && _cache.size() >= _sizeLimit) //key not in cache & should put first
        {
            _evictionStrategy.evict(this);
        }
    }


    /* (non-Javadoc)
     * @see com.j_spaces.javax.cache.Cache#size()
     */
    @Override
    public int size() {
        return _cache.size();
    }

    /* (non-Javadoc)
     * @see com.j_spaces.javax.cache.Cache#clear()
     */
    @Override
    public void clear() {
        clear(false);
    }

    /*
     * (non-Javadoc)
     * @see com.j_spaces.map.IMap#clear(boolean)
     */
    @Override
    public void clear(boolean clearMaster) {

        if (clearMaster) {
            try {
                _spaceProxy.clear(MapEntryFactory.create(), null);
            } catch (Exception e) {
                throw new SpaceCacheException("Failed to clear space cache", e);
            }
        }

        _cache.clear();
        // Removed to allow optimistic locking after clear
        //      if( _entryInfos != null)
        //         _entryInfos.clear();

        //    _cacheStatistics.clearStatistics();
        _evictionStrategy.clear();


        if (_cacheListenerTable.size() != 0) {
            for (CacheListener listener : _cacheListenerTable) {
                listener.onClear();
            }
        }
    }

    /* (non-Javadoc)
     * @see com.j_spaces.javax.cache.Cache#containsKey(java.lang.Object)
     */
    @Override
    public boolean containsKey(Object key) {
        return _cache.containsKey(key);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.javax.cache.Cache#containsValue(java.lang.Object)
     */
    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * @see com.j_spaces.javax.cache.Cache#values()
     */
    @Override
    public Collection values() {

        Collection<CacheEntry> collection = _cache.values();

        ArrayList<Object> values = new ArrayList<Object>(_cache.size());

        for (CacheEntry entry : collection) {
            if (!(entry instanceof DummyCacheEntry))
                values.add(entry.getValue());
        }

        return values;
    }

    /*
     * (non-Javadoc)
     * @see com.j_spaces.javax.cache.Cache#entrySet()
     */
    @Override
    public Set entrySet() {
        Collection<CacheEntry> collection = _cache.values();
        HashSet<CacheEntry> set = new HashSet<CacheEntry>(_cache.size());
        for (CacheEntry entry : collection) {
            if (!(entry instanceof DummyCacheEntry))
                set.add(entry);
        }
        return set;
    }

    /*
     * (non-Javadoc)
     * @see com.j_spaces.javax.cache.Cache#keySet()
     */
    public Set keySet() {
        Collection<CacheEntry> collection = _cache.values();
        HashSet<Object> set = new HashSet<Object>(_cache.size());
        for (CacheEntry entry : collection) {
            if (!(entry instanceof DummyCacheEntry)) {
                set.add(entry.getKey());
            }
        }
        return set;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.javax.cache.Cache#addListener(javax.cache.CacheListener)
     */
    @Override
    public void addListener(CacheListener listener) {
        _cacheListenerTable.add(listener);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.javax.cache.Cache#evict()
     */
    @Override
    public boolean evict(Object key) {
        boolean evicted = _cache.remove(key) != null;

        if (evicted && !_cacheListenerTable.isEmpty()) {
            for (CacheListener listener : _cacheListenerTable) {
                listener.onEvict(key);
            }
        }
        return evicted;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.javax.cache.Cache#getCacheEntry(java.lang.Object)
     */
    @Override
    public CacheEntry getCacheEntry(Object key) {
        return _cache.get(key);
    }

	/* (non-Javadoc)
     * @see com.j_spaces.javax.cache.Cache#getCacheStatistics()
	 */
    //   public CacheStatistics getCacheStatistics() {
    //      return _cacheStatistics;
    //   }

    /* (non-Javadoc)
     * @see com.j_spaces.javax.cache.Cache#load(java.lang.Object)
     */
    @Override
    public void load(Object key) throws CacheException {
        internalGet(key, null, Long.MAX_VALUE, ReadModifiers.REPEATABLE_READ);
    }

    /*
     * (non-Javadoc)
     * @see com.j_spaces.map.AbstractMap#loadAll(java.util.Collection)
     */
    @Override
    public void loadAll(Collection keys) throws CacheException {
        // TODO use getMul
        for (Object key : keys) {
            load(key);
        }
    }

    /* (non-Javadoc)
     * @see com.j_spaces.javax.cache.Cache#peek(java.lang.Object)
     */
    @Override
    public Object peek(Object key) {

        CacheEntry cacheEntry = _cache.get(key);

        if (cacheEntry != null)
            return cacheEntry.getValue();

        return null;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.javax.cache.Cache#removeListener(javax.cache.CacheListener)
     */
    @Override
    public void removeListener(CacheListener listener) {
        _cacheListenerTable.remove(listener);
    }

    /* (non-Javadoc)
     * @see net.jini.core.event.RemoteEventListener#notify(net.jini.core.event.RemoteEvent)
     */
    @Override
    public void notify(RemoteEvent remoteEvent) throws UnknownEventException, RemoteException {
        // TODO [@author guy] should be moved to thread pool ???
        try {
            consumeRequestCommand((EntryArrivedRemoteEvent) remoteEvent);
        } catch (ClassNotFoundException e) {
            throw new SpaceCacheException("Failed to consume data event in space cache", e);
        } catch (IOException e) {
            throw new SpaceCacheException("Failed to consume data event in space cache", e);
        } catch (UnusableEntryException e) {
            throw new SpaceCacheException("Failed to consume data event in space cache", e);
        }
    }

    private void consumeRequestCommand(EntryArrivedRemoteEvent commandEvent)
            throws ClassNotFoundException, IOException, UnusableEntryException {
        // since we are using NotifyDelegator, we can obtain the entry that triggered the event
        SpaceMapEntry entry = (SpaceMapEntry) commandEvent.getObject();

        //if an update operation and is a self origin, ignore this event.
        //otherwise, update local cache.
        if (commandEvent.getNotifyType() != NotifyModifiers.NOTIFY_TAKE
                && commandEvent.getNotifyType() != NotifyModifiers.NOTIFY_LEASE_EXPIRATION) {
            String cacheId = entry.getCacheId();

            // ignore self notifications
            if (_cacheID.equals(cacheId))
                return;
        }

        Object key = entry.getKey();

        if (_updateMode == SpaceURL.UPDATE_MODE_PUSH &&
                commandEvent.getNotifyType() == NotifyModifiers.NOTIFY_UPDATE) {
            if (!_cache.containsKey(key))
                return;
			/* TODO notify the local Listeners */
            Object value = prepareValue(key, entry);

            CacheEntry cacheEntry = _evictionStrategy.createEntry(key, value, Long.MAX_VALUE, entry.getVersion());

            // replace current value only if the new version is bigger than current
            if (_isVersioned)
                _cache.replace(key, new VersionCacheEntry(entry.getVersion()), cacheEntry);
            else
                _cache.replace(key, cacheEntry);

        } else // UPDATE_MODE_PULL
        {
            // remove from local
            _cache.remove(key);
        }
    }

    /*
     * (non-Javadoc)
     * @see com.j_spaces.map.AbstractMap#internalRemove(java.lang.Object, long)
     */
    @Override
    protected Object internalRemove(Object key, Transaction txn, long waitForResponse) {
        try {
            //       bring from remote space
            SpaceMapEntry template = buildEnvelope(key, null, null);
            final boolean ifExists = txn != null;
            SpaceMapEntry entry = (SpaceMapEntry) _spaceProxy.take(template, txn, waitForResponse, ReadModifiers.MATCH_BY_ID, ifExists);

            if (entry == null) {
                if (txn == null)
                    return null;

                throw new CacheTimeoutException(key);
            }

            return prepareValue(key, entry); //TODO check that old entry won't get into the space
        } catch (EntryNotInSpaceException ex) {
            return null;
        } catch (CacheTimeoutException e) {
            throw e;
        } catch (Exception e) {
            throw new SpaceCacheException("Failed to remove value from space cache", e);
        } finally {
            CacheEntry cacheEntry = _cache.remove(key);
            if (cacheEntry != null && !(cacheEntry instanceof DummyCacheEntry)) {
                _evictionStrategy.discardEntry(cacheEntry);

                if (!_cacheListenerTable.isEmpty()) {
                    for (CacheListener listener : _cacheListenerTable) {
                        listener.onRemove(key);
                    }
                }
            }
        }
    }

    private Object getResult(CacheEntry obj) {
        _evictionStrategy.touchEntry(obj);
        return obj.getValue();
    }

    /*
     * (non-Javadoc)
     * @see com.j_spaces.map.AbstractMap#internalGet(java.lang.Object, long, boolean)
     */
    @Override
    protected Object internalGet(Object key, Transaction txn, long waitForResponse, int readModifiers) {
        try {
            if (txn == null)
                return getNotUnderTransaction(key, waitForResponse);

            return getUnderTransaction(key, waitForResponse, txn);
        } catch (EntryNotInSpaceException e) {
            return null;
        } catch (CacheTimeoutException e) {
            throw e;
        } catch (Exception e) {
            throw new SpaceCacheException("Failed to get value from space cache", e);
        }
    }

    private Object getUnderTransaction(Object key, long waitForResponse, Transaction txn) throws Exception {
        try {
            // bring from remote space
            SpaceMapEntry template = MapEntryFactory.create(key);

            _memoryManager.monitorMemoryUsage(true);

            SpaceMapEntry entry = (SpaceMapEntry) _spaceProxy.readIfExists(template, txn,
                    waitForResponse, ReadModifiers.MATCH_BY_ID);

            if (entry == null) // can't find in remote space
                throw new CacheTimeoutException(key);

            return prepareValue(key, entry);
        } finally // evicts key from cache to avoid consistency problems
        {
            evict(key);
        }
    }

    private Object prepareValue(Object key, SpaceMapEntry entry)
            throws ClassNotFoundException, IOException {
        // add to local cache
        Object value = entry.getValue();
        value = SerializationUtil.deSerializeFieldValue(value, _compression);
        if (_isVersioned)
            _entryInfos.setEntryVersion(value, key, entry.getVersion());

        return value;
    }

    private Object getNotUnderTransaction(Object key, long waitForResponse) throws Exception {
        DummyCacheEntry readMark = null;
        CacheEntry cacheEntry = _cache.get(key);
        if (cacheEntry == null) {
            readMark = new DummyCacheEntry();
            // used in case that a remote get needed (mark)
            cacheEntry = _cache.putIfAbsent(key, readMark);

            if (cacheEntry != null && !(cacheEntry instanceof DummyCacheEntry)) // maybe another get returned
            {
                return getResult(cacheEntry);
            } // else need to read from space
        } else // key was found & not under transaction
        {
            if (!(cacheEntry instanceof DummyCacheEntry)) // return the value
            {
                return getResult(cacheEntry);
            }// else _readMark
        }

        // bring from remote space
        SpaceMapEntry template = MapEntryFactory.create(key);

        _memoryManager.monitorMemoryUsage(true);

        SpaceMapEntry entry = (SpaceMapEntry) _spaceProxy.read(template,
                (Transaction) null, waitForResponse, ReadModifiers.MATCH_BY_ID);

        if (entry == null) // can't find in remote space
        {
            if (readMark != null) {
                // remove readMark (notice parallel read won't remove the mark)
                _cache.remove(key, readMark);
            }
            return null;
        }

        // add to local cache
        if (_cache.size() >= _sizeLimit) {
            _evictionStrategy.evict(this);
        }

        Object value = prepareValue(key, entry);

        CacheEntry newCacheEntry = _evictionStrategy.createEntry(key, value, Long.MAX_VALUE, entry.getVersion());

		/*
		 * Tries to add to local cache, if the key already there tries to update only if the 
		 * version is greater than current version or the old value is _readMark.   
		 * If fails to update it means that a parallel read/take/notify was happened.
		 */
        if (!_cache.replace(key, new VersionCacheEntry(entry.getVersion(), readMark), newCacheEntry)) {
            _evictionStrategy.discardEntry(newCacheEntry); // fail to update local cache
        } else {
            if (!_cacheListenerTable.isEmpty()) {
                for (CacheListener listener : _cacheListenerTable) {
                    listener.onLoad(key);
                }
            }
        }

        return value;
    }

    /**
     * Dummy class used only on replace. We use this class to replace current object only with
     * bigger version
     */
    final static private class VersionCacheEntry extends DummyCacheEntry {
        final private int _version;
        final private DummyCacheEntry _mark;
        private Object _oldEntry;
        private boolean _checked = false;

        public VersionCacheEntry(int version) {
            this(version, null);
        }

        public VersionCacheEntry(int version, DummyCacheEntry mark) {
            _version = version;
            _mark = mark;
        }

        @Override
        public boolean equals(Object obj) {
            _checked = true;
            _oldEntry = obj;
            if (!(obj instanceof DummyCacheEntry)) {
                long version = ((CacheEntry) obj).getCacheEntryVersion();
                return _version > version;
            }
            return _mark == obj; // read mark
        }

        public Object getOldEntry() {
            return _oldEntry;
        }

        public boolean isChecked() {
            return _checked;
        }
    }

    /*
     * (non-Javadoc)
     * @see net.jini.lease.LeaseListener#notify(net.jini.lease.LeaseRenewalEvent)
     */
    public void notify(LeaseRenewalEvent event) {
        log("LRM: Failed to renew lease.");
        try {
            try {
                _eventSession.close();
            } catch (Throwable e) {
				/* don't care any more this lease */
            }

            reconnectToSpace();

            _eventSession = initEventSession(); //start over
            clear();
            log("Reinitialized successfully.");
        } catch (Exception e) {
            log("Failed to initialize space", e);
        }
    }

    private void reconnectToSpace() throws InterruptedException, CacheException {
        // something went wrong, get the remote space again and clean the local cache
        boolean connected = false;

        // try to locate the remote space again
        int tryCount = 0;
        boolean isPinged = false;
        while (!connected && tryCount++ < _maxConnectionRetry) {
            try {
                // check whether the master space still alive and it's was just clean operation
                if (!isPinged)
                    _spaceProxy.ping();
                else
                    _spaceProxy = (ISpaceProxy) SpaceFinder.find(_spaceURL);

                connected = true;
            } catch (FinderException e) {
                log("SpaceFinder Retry: " + tryCount, e);
                Thread.sleep(_delayRetryTime);

            } catch (RemoteException e) {
                tryCount = 0;
                isPinged = true;
                log("Ping failed to : " + _spaceURL, e);
            }
        }

        if (!connected)
            throw new CacheException("_remoteSpace [" + _spaceURL + "] is not accessible. Giving up after trying " + _maxConnectionRetry + " times.");
    }

    @Override
    protected void finalize() throws Throwable {
        _memoryManager.close();
        if (_eventSession != null)
            _eventSession.close();
    }
}
