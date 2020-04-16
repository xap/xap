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
import com.gigaspaces.time.SystemTime;

import java.util.ArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to create {@link SharedDataIterator} over a single {@link DataIterator}, the iterator source
 * is in charge of mediating between the shared iterators, always only one shared iterator is
 * accessing the actual iterator
 *
 * @author eitany
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class SharedDataIteratorSource<T> {
    private final static Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_PERSISTENT_SHARED_ITERATOR);

    private boolean _initialized = false;
    private DataIterator<T> _sourceDataIterator;
    private final long _createdTime;
    private final ReadWriteLock _sharedObjectListLock = new ReentrantReadWriteLock(false);
    private final ArrayList<T> _accumulatedItems = new ArrayList<T>();
    private final long _timeToLive;

    private int _numberOfConsumers = 0;

    private boolean _sourceIteratorExhausted;
    private boolean _closed;
    private boolean _expired;

    private ISharedDataIteratorSourceStateChangedListener<T> _listener;

    private final Object _identifier;

    private final ISourceDataIteratorProvider<T> _sourceDataIteratorProvider;

    /**
     * Construct a new source
     *
     * @param sourceDataIterator data iterator to wrap
     * @param timeToLive         time in miliseconds that once elapsed this iterator source is
     *                           considered expired and will no longer receive requests for new
     *                           iterators
     */
    public SharedDataIteratorSource(Object identifier, ISourceDataIteratorProvider<T> sourceDataIteratorProvider, long timeToLive) {
        if (sourceDataIteratorProvider == null)
            throw new IllegalArgumentException("sourceDataIteratorProvider cannot be null");

        _identifier = identifier;
        _sourceDataIteratorProvider = sourceDataIteratorProvider;
        _timeToLive = timeToLive;

        _createdTime = SystemTime.timeMillis();
    }

    /**
     * Gets a new shared data iterator over the source data iterator
     */
    public DataIterator<T> getIterator() throws SharedDataIteratorSourceClosedException, SharedDataIteratorSourceExpiredException, DataSourceException {
        if (_logger.isTraceEnabled())
            _logger.trace("requesting a shared iterator from the shared iterator source [" + _identifier + "]");

        _sharedObjectListLock.writeLock().lock();
        try {
            //Check if expired, throw exception
            checkIfSourceIsValid();

            if (!_initialized) {
                _sourceDataIterator = _sourceDataIteratorProvider.getSourceIterator();
                _initialized = true;
            }

            _numberOfConsumers++;
            return new SharedDataIterator<T>(this);
        } finally {
            _sharedObjectListLock.writeLock().unlock();
        }
    }

    private void checkIfSourceIsValid()
            throws SharedDataIteratorSourceExpiredException, SharedDataIteratorSourceClosedException {
        if (SystemTime.timeMillis() - _createdTime > _timeToLive) {
            //the iterator is expired
            if (!_expired) {
                if (_logger.isTraceEnabled())
                    _logger.trace("shared iterator source is expired [" + _identifier + "]");
                //trigger expired event only once
                triggerExpiredEvent();
                _expired = true;
            }
            throw new SharedDataIteratorSourceExpiredException();
        }
        //Could be that the underlying source iterator is closed exactly before entering this method
        if (_closed)
            throw new SharedDataIteratorSourceClosedException();
    }

    /**
     * Triggers the source expired event
     */
    private void triggerExpiredEvent() {
        if (_listener != null)
            _listener.onExpired(this);

    }

    /**
     * Signal the source this shared data iterator is closed
     */
    public void closeSharedIterator() {
        _sharedObjectListLock.writeLock().lock();
        try {
            _numberOfConsumers--;
            if (_closed)
                return;

            if (_sourceIteratorExhausted || _numberOfConsumers == 0) {
                if (_logger.isTraceEnabled())
                    _logger.trace("closed shared iterator source [" + _identifier + "]");
                //If the underlying iterator is exhausted or there aren't any consumers left
                triggerClosedEvent();
                if (_sourceDataIterator != null) {
                    _sharedObjectListLock.writeLock().lock();
                    try {
                        _sourceDataIterator.close();
                    } finally {
                        _sharedObjectListLock.writeLock().unlock();
                    }
                }
                _closed = true;
            }
        } finally {
            _sharedObjectListLock.writeLock().unlock();
        }
    }

    private void triggerClosedEvent() {
        if (_listener != null)
            _listener.onClosed(this);
    }

    /**
     * Called once the source iterator has no more objects
     */
    private void sourceIteratorExhaushted() {
        if (_logger.isTraceEnabled())
            _logger.trace("shared iterator source wrapped iterator is exhausted [" + _identifier + "]");
        _sourceIteratorExhausted = true;
    }

    /**
     * Called by the shared iterators, this method blocks until a next element is available
     *
     * @param iteration each iterator is at a different iteration number, specify the iteration
     *                  number
     * @return if has next according to the given iteration
     */
    boolean waitForNext(int iteration) {
        try {
            _sharedObjectListLock.readLock().lock();

            if (_accumulatedItems.size() > iteration) {
                //there are enough accumulated items to satisfy the next request
                if (_logger.isTraceEnabled())
                    _logger.trace("getting next item from the shared iterator source buffer [" + _identifier + "]");
                return true;
            }
        } finally {
            _sharedObjectListLock.readLock().unlock();
        }
        try {
            _sharedObjectListLock.writeLock().lock();

            if (_accumulatedItems.size() > iteration) {
                //double check if there are enough accumulated items to satisfy the next request
                if (_logger.isTraceEnabled())
                    _logger.trace("getting next item from the shared iterator source buffer [" + _identifier + "]");
                return true;
            }

            if (_closed)
                return false;

            if (_sourceDataIterator == null || !_sourceDataIterator.hasNext()) {
                //source iterator is exhausted
                sourceIteratorExhaushted();
                return false;
            }
            if (_logger.isTraceEnabled())
                _logger.trace("getting next item from the shared iterator source wrapped iterator [" + _identifier + "]");
            //Reached this point, accumulate next element to the list
            T next = _sourceDataIterator.next();
            _accumulatedItems.add(next);
            return true;
        } finally {
            _sharedObjectListLock.writeLock().unlock();
        }
    }

    /**
     * Called by the shared iterators to get their next object according to the specified index
     *
     * @param index specified index
     */
    public T getNext(int index) {
        _sharedObjectListLock.readLock().lock();
        try {
            return _accumulatedItems.get(index);
        } finally {
            _sharedObjectListLock.readLock().unlock();
        }

    }

    /**
     * Subscribe to state changed events
     */
    public void addStateChangedListener(ISharedDataIteratorSourceStateChangedListener<T> listener) {
        _sharedObjectListLock.writeLock().lock();
        try {
            _listener = listener;
        } finally {
            _sharedObjectListLock.writeLock().unlock();
        }
    }
}
