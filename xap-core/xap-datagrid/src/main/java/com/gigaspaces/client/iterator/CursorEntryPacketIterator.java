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

import com.gigaspaces.client.iterator.server_based.SpaceIteratorBatchResultsManager;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.metadata.ObjectType;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.client.iterator.server_based.SpaceIteratorException;
import com.j_spaces.jdbc.builder.SQLQueryTemplatePacket;

import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Alon Shoham
 * @since 15.2
 */
@com.gigaspaces.api.InternalApi
public class CursorEntryPacketIterator implements IEntryPacketIterator {
    private static final Logger _logger = Logger.getLogger(Constants.LOGGER_GSITERATOR);
    private final ISpaceProxy _spaceProxy;
    private final long _serverLookupTimeout;
    private final ITemplatePacket _queryPacket;
    private final List<IEntryPacket> _buffer;
    private Iterator<IEntryPacket> _bufferIterator;
    private boolean _closed;
    private SpaceIteratorBatchResultsManager _spaceIteratorBatchResultsManager;

    public CursorEntryPacketIterator(ISpaceProxy spaceProxy, Object query, SpaceIteratorConfiguration spaceIteratorConfiguration) {
        if (spaceProxy == null)
            throw new IllegalArgumentException("space argument must not be null.");
        if (query == null)
            throw new IllegalArgumentException("query argument must not be null.");
        int batchSize = spaceIteratorConfiguration.getBatchSize();
        if (batchSize <= 0)
            throw new IllegalArgumentException("batchSize must be greater than zero.");
        if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "SpaceIterator initialized with batchSize=" + batchSize);
        Duration maxInactiveDuration = spaceIteratorConfiguration.getMaxInactiveDuration() == null ? SpaceIteratorConfiguration.getDefaultMaxInactiveDuration() : spaceIteratorConfiguration.getMaxInactiveDuration();
        if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "SpaceIterator initialized with batchSize=" + batchSize);
        this._spaceProxy = spaceProxy;
        this._serverLookupTimeout = _spaceProxy.getDirectProxy().getProxyRouter().getConfig().getActiveServerLookupTimeout();
        this._queryPacket = toTemplatePacket(query);
        this._buffer = new LinkedList<>();
        this._spaceIteratorBatchResultsManager = new SpaceIteratorBatchResultsManager(_spaceProxy, batchSize, spaceIteratorConfiguration.getReadModifiers().getCode(), _queryPacket, maxInactiveDuration.toMillis());
        this._bufferIterator = getNextBatch();
    }

    private ITemplatePacket toTemplatePacket(Object template) {
        ObjectType objectType = ObjectType.fromObject(template);
        ITemplatePacket templatePacket = _spaceProxy.getDirectProxy().getTypeManager().getTemplatePacketFromObject(template, objectType);
        if (templatePacket instanceof SQLQueryTemplatePacket)
            templatePacket = _spaceProxy.getDirectProxy().getQueryManager().getSQLTemplate((SQLQueryTemplatePacket) templatePacket, null);
        return templatePacket;
    }

    public ITemplatePacket getQueryPacket() {
        return _queryPacket;
    }

    @Override
    public boolean hasNext() {
        if (_closed) {
            _logger.log(Level.FINER, "hasNext() returned false - iterator is closed.");
            return false;
        }
        // If null, we either reached end of iterator or this is the first time.
        if (_bufferIterator == null) {
            _bufferIterator = getNextBatch();
            // If still null, there's no pending entries:
            if (_bufferIterator == null) {
                if (_logger.isLoggable(Level.FINER))
                    _logger.log(Level.FINER, "hasNext() returned false.");
                return false;
            }
        }
        // otherwise, we use the iterator's hasNext method.
        boolean result;
        if (_bufferIterator.hasNext())
            result = true;
        else {
            // Reset and call recursively:
            _bufferIterator = null;
            result = hasNext();
        }
        if (_logger.isLoggable(Level.FINER))
            _logger.log(Level.FINER, "hasNext() returned " + result);
        return result;
    }

    @Override
    public IEntryPacket next() {
        IEntryPacket entryPacket = hasNext() ? _bufferIterator.next() : null;
        if (_logger.isLoggable(Level.FINER))
            _logger.log(Level.FINER, "next() returned " + (entryPacket == null ? "null" : "object with uid=" + entryPacket.getUID()));
        return entryPacket;
    }

    public Object nextEntry() {
        IEntryPacket entryPacket = next();
        return entryPacket != null
                ? _spaceProxy.getDirectProxy().getTypeManager().convertQueryResult(entryPacket, _queryPacket, false)
                : null;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove is not supported");
    }

    @Override
    public void close() {
        if (!_closed) {
            _closed = true;
            if (_bufferIterator != null) {
                while (_bufferIterator.hasNext())
                    _bufferIterator.next();
            }
            _buffer.clear();
            _spaceIteratorBatchResultsManager.close();
        }
    }

    private Iterator<IEntryPacket> getNextBatch() throws SpaceIteratorException {
        _buffer.clear();
        try {
            Object[] entries =  _spaceIteratorBatchResultsManager.getNextBatch(_serverLookupTimeout);
            if(entries == null)
                return null;
            if (_logger.isLoggable(Level.FINE))
                _logger.log(Level.FINE, "getNextBatch returns with a buffer of " + entries.length + " entries.");
            for (Object entry : entries)
                _buffer.add((IEntryPacket) entry);
            return _buffer.iterator();
        } catch (InterruptedException e) {
            processNextBatchFailure(e);
        }
        return null;
    }

    private void processNextBatchFailure(Exception e) {
        if (_logger.isLoggable(Level.SEVERE))
            _logger.log(Level.SEVERE, "Failed to get next data batch for iterator.", e);

    }
}
