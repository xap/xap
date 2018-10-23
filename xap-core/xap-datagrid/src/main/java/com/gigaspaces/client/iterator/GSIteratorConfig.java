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


package com.gigaspaces.client.iterator;

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.events.EventSessionConfig;
import com.j_spaces.core.client.ReadModifiers;

import net.jini.core.lease.Lease;

/**
 * Configuration class for GSIterator.
 *
 * @author niv
 * @see com.j_spaces.core.client.GSIterator
 * @since 7.0
 * @deprecated Since 14.0
 */
@Deprecated
@InternalApi
public class GSIteratorConfig {
    private int _bufferSize = 100;
    private IteratorScope _iteratorScope = IteratorScope.FUTURE;
    private long _leaseDuration = Lease.FOREVER;
    private EventSessionConfig _eventSessionconfig;
    private int _readModifiers = ReadModifiers.READ_COMMITTED;

    /**
     * Default constructor.
     */
    public GSIteratorConfig() {
    }

    /**
     * Constructor for direct initialization of {@link #setBufferSize(int)} and {@link
     * #setIteratorScope(IteratorScope)}.
     */
    public GSIteratorConfig(int bufferSize, IteratorScope iteratorScope) {
        this._bufferSize = bufferSize;
        this._iteratorScope = iteratorScope;
    }

    /**
     * Gets the iterator's buffer size.
     *
     * @return Currently configured buffer size.
     */
    public int getBufferSize() {
        return _bufferSize;
    }

    /**
     * Sets the iterator's buffer size
     *
     * @param value Buffer size.
     */
    public GSIteratorConfig setBufferSize(int value) {
        _bufferSize = value;
        return this;
    }

    /**
     * Gets the iterator's iteration scope.
     *
     * @return Currently configured iteration scope.
     */
    public IteratorScope getIteratorScope() {
        return _iteratorScope;
    }

    /**
     * Sets the iterator's iteration scope.
     *
     * @param value Iteration scope.
     */
    public GSIteratorConfig setIteratorScope(IteratorScope value) {
        _iteratorScope = value;
        return this;
    }

    /**
     * Gets the iterator's lease duration.
     *
     * @return Currently configured lease duration.
     */
    public long getLeaseDuration() {
        return _leaseDuration;
    }

    /**
     * Sets the iterator's lease duration.
     *
     * @param value lease duration in milliseconds.
     */
    public GSIteratorConfig setLeaseDuration(long value) {
        _leaseDuration = value;
        return this;
    }

    /**
     * Gets the iterator's event session configuration.
     *
     * @return Current event session configuration.
     */
    public EventSessionConfig getEventSessionConfig() {
        return _eventSessionconfig;
    }

    /**
     * Sets the iterator's event session configuration.
     *
     * @param value Event session configuration.
     */
    public GSIteratorConfig setEventSessionConfig(EventSessionConfig value) {
        _eventSessionconfig = value;
        return this;
    }

    /**
     * Gets the read modifiers the iterator will use to read entries.
     *
     * @since 8.0.3
     */
    public int getReadModifiers() {
        return _readModifiers;
    }

    /**
     * Sets the read modifiers the iterator will use to read entries.
     *
     * @since 8.0.3
     */
    public GSIteratorConfig setReadModifiers(int readModifiers) {
        this._readModifiers = readModifiers;
        return this;
    }
}
