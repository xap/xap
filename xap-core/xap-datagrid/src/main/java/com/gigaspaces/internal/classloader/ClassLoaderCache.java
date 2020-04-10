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

package com.gigaspaces.internal.classloader;

import com.gigaspaces.internal.utils.collections.ConcurrentHashSet;
import com.gigaspaces.internal.utils.collections.CopyOnUpdateMap;
import com.gigaspaces.internal.utils.collections.SelfCleaningTable;
import com.gigaspaces.internal.utils.collections.SelfCleaningTable.ICleanerListener;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.kernel.ClassLoaderHelper;

import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class loaders cache, each class loader is associated with a long key value All the class loader
 * are kept as weak references and can be explicitly removed
 *
 * @author eitany
 * @since 7.0.1
 */
@com.gigaspaces.api.InternalApi
public class ClassLoaderCache
        implements ICleanerListener<Long> {
    private enum Represent {
        REGULAR, REMOVED_EXPLICIT, REMOVED_IMPLICIT
    }

    private static class ClassLoaderContext {

        private final WeakReference<ClassLoader> _classLoaderRef;
        private final Set<WeakReference<IClassLoaderCacheStateListener>> _specificListeners;
        private volatile boolean _dispatchingRemoved;
        private final long _timeStamp;
        private final Represent _represents;

        ClassLoaderContext(ClassLoader classLoader, Represent represents) {
            _represents = represents;
            _classLoaderRef = new WeakReference<>(classLoader);
            _specificListeners = new ConcurrentHashSet<>();
            _timeStamp = SystemTime.timeMillis();
        }

        private WeakReference<ClassLoader> getClassLoaderRef() {
            return _classLoaderRef;
        }

        /**
         * Adds a listener to this class loader removal event
         *
         * @return true if the listener is added and the class loader is not removed yet.
         */
        public boolean addListener(IClassLoaderCacheStateListener listener) {
            _specificListeners.add(new WeakReference<>(listener));
            // If dispatchRemoved is already called, this class loader is being
            // removed.
            // The listener might get or may not the event of removal due to
            // concurrent traversal on the listener map,
            // regardless the operation is considered as failed
            return !_dispatchingRemoved;
        }

        public void removeListener(IClassLoaderCacheStateListener listener) {
            Iterator<WeakReference<IClassLoaderCacheStateListener>> iterator = _specificListeners.iterator();
            while (iterator.hasNext()) {
                WeakReference<IClassLoaderCacheStateListener> weakListener = iterator.next();
                IClassLoaderCacheStateListener actualListener = weakListener.get();
                if (actualListener == null
                        || actualListener == listener) {
                    iterator.remove();
                }
            }
        }

        void dispatchRemoved(Long classLoaderKey, boolean explicit) {
            // Mark dispatched true to protect from concurrent call to
            // addListener
            _dispatchingRemoved = true;
            for (WeakReference<IClassLoaderCacheStateListener> weakListener : _specificListeners) {
                try {
                    IClassLoaderCacheStateListener listener = weakListener.get();
                    if (listener != null)
                        listener.onClassLoaderRemoved(classLoaderKey, explicit);
                } catch (Exception e) {
                    if (_logger.isErrorEnabled())
                        _logger.error(
                                "received exception while dispatching class loader removed event, class loader id"
                                        + classLoaderKey,
                                e);
                    // Ignore exceptions, continue dispatching events
                }
            }
        }

        public Represent getRepresents() {
            return _represents;
        }

        public long getTimeStamp() {
            return _timeStamp;
        }

    }

    // minute
    private final static ClassLoaderCache _cache = new ClassLoaderCache(60 * 1000); //1 minute
    private final static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_CLASSLOADERS_CACHE);

    private final CopyOnUpdateMap<Long, ClassLoaderContext> _classLoaders;
    private final SelfCleaningTable<ClassLoader, Long> _classLoaderToIdMap;
    private final Set<WeakReference<IClassLoaderCacheStateListener>> _listeners;

    private final AtomicLong _classLoaderKeyGenerator = new AtomicLong();
    private final Object _lock = new Object();
    private final long _removedRelevantWindow;

    protected ClassLoaderCache(long removedRelevantWindow) {
        _removedRelevantWindow = removedRelevantWindow;
        _classLoaders = new CopyOnUpdateMap<>();
        _classLoaderToIdMap = new SelfCleaningTable<>("ClassLoaderCache",
                this,
                new CopyOnUpdateMap());
        _listeners = new ConcurrentHashSet<>();
    }

    /**
     * Gets the singleton cache instance
     */
    public static ClassLoaderCache getCache() {
        return _cache;
    }

    public void shutdown() {
        _classLoaderToIdMap.close();
    }

    /**
     * Add new class loader to the cache, if the class loader is already present, the existing key
     * will be returned
     *
     * @return identifier key for the specified class loader
     */
    public Long putClassLoader(ClassLoader classLoader) {
        if (classLoader == null)
            throw new IllegalArgumentException("Argument cannot be null - 'classLoader'.");

        Long previousId = _classLoaderToIdMap.get(classLoader);
        if (previousId != null)
            return previousId;
        synchronized (_lock) {
            previousId = _classLoaderToIdMap.get(classLoader);
            if (previousId != null)
                return previousId;

            removeMarkers();

            long id = generateClassLoaderKey();
            if (_logger.isDebugEnabled())
                _logger.debug("introducing new class loader to cache ["
                        + ClassLoaderHelper.getClassLoaderLogName(classLoader)
                        + "] to the class provider, class loader designated id is "
                        + id);
            // Should be this order since put is not locked
            // for class loaders that are already in the map
            _classLoaders.put(id, new ClassLoaderContext(classLoader,
                    Represent.REGULAR));
            _classLoaderToIdMap.put(classLoader, id);
            return id;
        }
    }

    private void removeMarkers() {
        long currentTime = SystemTime.timeMillis();
        for (Map.Entry<Long, ClassLoaderContext> entry : _classLoaders.entrySet()) {
            // Do not remove markers that were created lately
            if (currentTime - entry.getValue().getTimeStamp() <= _removedRelevantWindow)
                continue;
            if (entry.getValue().getRepresents() == Represent.REMOVED_EXPLICIT
                    || entry.getValue().getRepresents() == Represent.REMOVED_IMPLICIT) {
                _classLoaders.remove(entry.getKey());
            }
        }
    }

    /**
     * Gets class loader by key
     *
     * @return null if none associated to the given key
     */
    public ClassLoader getClassLoader(Long key) {
        ClassLoaderContext classLoaderContext = _classLoaders.get(key);
        if (classLoaderContext == null)
            return null;

        WeakReference<ClassLoader> weakReference = classLoaderContext.getClassLoaderRef();

        ClassLoader classLoader = weakReference.get();
        if (classLoader == null) {
            _classLoaders.remove(key);
        }
        return classLoader;
    }

    /**
     * Remove the specified class loader
     */
    public void removeClassLoader(ClassLoader classLoader) {
        if (_logger.isDebugEnabled())
            _logger.debug("removing class loader from cache ["
                    + ClassLoaderHelper.getClassLoaderLogName(classLoader)
                    + "]");
        Long removedClassLoadedId = _classLoaderToIdMap.remove(classLoader);
        if (removedClassLoadedId != null) {
            ClassLoaderContext removedContext = _classLoaders.put(removedClassLoadedId,
                    new ClassLoaderContext(null,
                            Represent.REMOVED_EXPLICIT));
            if (removedContext == null) return;
            // Already removed
            if (removedContext.getRepresents() == Represent.REMOVED_EXPLICIT
                    || removedContext.getRepresents() == Represent.REMOVED_IMPLICIT)
                return;
            dispatchClassLoaderRemovedEvent(removedClassLoadedId,
                    removedContext,
                    true);
        } else {
            _logger.debug("class loader ["
                    + ClassLoaderHelper.getClassLoaderLogName(classLoader)
                    + "] is not present in cache");
        }

    }

    private long generateClassLoaderKey() {
        return _classLoaderKeyGenerator.incrementAndGet();
    }

    public void weakEntryRemoved(Long value) {
        if (_logger.isDebugEnabled())
            _logger.debug("class loader with key " + value
                    + " is removed because it has no more active references");
        ClassLoaderContext removedContext = _classLoaders.put(value,
                new ClassLoaderContext(null,
                        Represent.REMOVED_IMPLICIT));
        if (removedContext == null) return;
        // Already removed
        if (removedContext.getRepresents() == Represent.REMOVED_EXPLICIT
                || removedContext.getRepresents() == Represent.REMOVED_IMPLICIT)
            return;
        dispatchClassLoaderRemovedEvent(value, removedContext, false);
    }

    /**
     * Gets the key for the specified class loader
     */
    public Long getClassLoaderKey(ClassLoader classLoader) {
        return _classLoaderToIdMap.get(classLoader);
    }

    /**
     * Register for cache state changed events, the listener is kept as weak reference, all
     * subscribers must keep a strong reference to the listener to keep this notification active,
     * this will trigger history events of at most last 100 removed class loader
     */
    public void registerCacheStateListener(
            IClassLoaderCacheStateListener listener) {
        WeakReference<IClassLoaderCacheStateListener> weakListener = new WeakReference<>(listener);
        _listeners.add(weakListener);
        Set<Long> alreadyRemovedExplicitListeners = new HashSet<>();
        Set<Long> alreadyRemovedImplicitListeners = new HashSet<>();
        synchronized (_lock) {
            long currentTime = SystemTime.timeMillis();
            for (Map.Entry<Long, ClassLoaderContext> entry : _classLoaders.entrySet()) {
                // Do not invoke events for markers that were created long ago
                if (currentTime - entry.getValue().getTimeStamp() > _removedRelevantWindow)
                    continue;

                if (entry.getValue().getRepresents() == Represent.REMOVED_EXPLICIT)
                    alreadyRemovedExplicitListeners.add(entry.getKey());
                if (entry.getValue().getRepresents() == Represent.REMOVED_IMPLICIT)
                    alreadyRemovedImplicitListeners.add(entry.getKey());
            }
        }
        dispatchRemovedHelper(listener,
                weakListener,
                alreadyRemovedExplicitListeners,
                true);
        dispatchRemovedHelper(listener,
                weakListener,
                alreadyRemovedImplicitListeners,
                false);
    }

    private void dispatchRemovedHelper(IClassLoaderCacheStateListener listener,
                                       WeakReference<IClassLoaderCacheStateListener> weakListener,
                                       Set<Long> alreadyRemovedExplicitListeners, boolean explicit) {
        boolean caughtException = false;
        for (Long removedClKey : alreadyRemovedExplicitListeners) {
            try {
                listener.onClassLoaderRemoved(removedClKey, explicit);
            } catch (Exception e) {
                if (_logger.isErrorEnabled())
                    _logger.error(
                            "received exception while dispatching class loader removed event, class loader id"
                                    + removedClKey,
                            e);
                // Ignore exceptions, continue dispatching events
                caughtException = true;
            }
        }
        if (caughtException) {
            _listeners.remove(weakListener);
        }
    }

    /**
     * Register for a specified class loader key state events, the listener is kept as weak
     * reference, all subscribers must keep a strong reference to the listener to keep this
     * notification active
     *
     * @return true if registration successful, which means the specified class loader key exists in
     * the cache
     */
    public boolean registerClassLoaderStateListener(Long classLoaderKey,
                                                    IClassLoaderCacheStateListener listener) {
        ClassLoaderContext classLoaderContext = _classLoaders.get(classLoaderKey);
        if (classLoaderContext == null)
            return false;
        if (classLoaderContext.getRepresents() != Represent.REGULAR)
            return false;

        return classLoaderContext.addListener(listener);
    }

    private void dispatchClassLoaderRemovedEvent(Long classLoaderKey,
                                                 ClassLoaderContext removedContext, boolean explicit) {
        Iterator<WeakReference<IClassLoaderCacheStateListener>> iterator = _listeners.iterator();
        while (iterator.hasNext()) {
            WeakReference<IClassLoaderCacheStateListener> weakListener = iterator.next();
            IClassLoaderCacheStateListener listener = weakListener.get();
            if (listener == null) {
                iterator.remove();
                continue;
            }
            try {
                listener.onClassLoaderRemoved(classLoaderKey, explicit);
            } catch (Exception e) {
                if (_logger.isErrorEnabled())
                    _logger.error(
                            "received exception while dispatching class loader removed event, class loader id"
                                    + classLoaderKey,
                            e);
                // Ignore exceptions, continue dispatching events
                iterator.remove();
            }
        }
        // Dispatch event for the one who listens to this class loader change
        // only
        removedContext.dispatchRemoved(classLoaderKey, explicit);
    }

    /**
     * Remove a specific listener from a specific class listener
     */
    public void removeClassLoaderStateListener(Long classLoaderKey,
                                               IClassLoaderCacheStateListener listener) {
        ClassLoaderContext classLoaderContext = _classLoaders.get(classLoaderKey);
        if (classLoaderContext == null)
            return;
        classLoaderContext.removeListener(listener);
    }
}
