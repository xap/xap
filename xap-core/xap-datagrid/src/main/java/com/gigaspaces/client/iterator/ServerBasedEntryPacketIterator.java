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
import com.gigaspaces.internal.client.SpaceIteratorBatchResult;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.metadata.ObjectType;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.logger.Constants;
import com.j_spaces.jdbc.builder.SQLQueryTemplatePacket;

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
public class ServerBasedEntryPacketIterator extends AbstractEntryPacketIterator {
    private static final Logger _logger = Logger.getLogger(Constants.LOGGER_GSITERATOR);
    private static final long TIMEOUT = 5000;
    private final ISpaceProxy _spaceProxy;
    private final ITemplatePacket _queryPacket;
    private final List<IEntryPacket> _buffer;
    private Iterator<IEntryPacket> _bufferIterator;
    private boolean _closed;
    private SpaceIteratorBatchResultsManager _spaceIteratorBatchResultsManager;

    public ServerBasedEntryPacketIterator(ISpaceProxy spaceProxy, Object query, int batchSize, int modifiers) {
        if (spaceProxy == null)
            throw new IllegalArgumentException("space argument must not be null.");
        if (query == null)
            throw new IllegalArgumentException("query argument must not be null.");
        if (batchSize <= 0)
            throw new IllegalArgumentException("batchSize argument must be greater than zero.");

        if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "SpaceIterator initialized with batchSize=" + batchSize);
        this._spaceProxy = spaceProxy;
        this._queryPacket = toTemplatePacket(query);
        this._buffer = new LinkedList<>();
        this._spaceIteratorBatchResultsManager = new SpaceIteratorBatchResultsManager(_spaceProxy, batchSize, modifiers, _queryPacket);
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
            _logger.log(Level.FINER, "hasNext() returned false - iterator is closed");
            return false;
        }

        boolean result;
        // If null, we either reached end of iterator or this is the first time.
        if (_bufferIterator == null)
            _bufferIterator = getNextBatch();

        // If still null, there's no pending entries:
        if (_bufferIterator == null) {
            result = false;
        } else {
            // otherwise, we use the iterator's hasNext method.
            if (_bufferIterator.hasNext())
                result = true;
            else {
                // Reset and call recursively:
                _bufferIterator = null;
                result = hasNext();
            }
        }

        if (_logger.isLoggable(Level.FINER))
            _logger.log(Level.FINER, "hasNext() returned " + result);

        if (!result)
            close();
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
    /*
    Get next batch flow:
        - Check if all partitions finished
            - If finished successfully, return null
            - If finished with exception throw exception
        -----Not finished yet-------
        - Get next batch result from manager
        - If result is failed
            - Deactivate partition result arrived from
            - Call method recursively
        - If result is finished, deactivate partition result arrived from
        - If batch result is empty, call method recursively
     */
    public Iterator<IEntryPacket> getNextBatch() {
        _buffer.clear();
        Iterator<IEntryPacket> result = null;
        if(_spaceIteratorBatchResultsManager.allFinished()){
            if(_spaceIteratorBatchResultsManager.allFinishedSuccessfully()) {
                if(_logger.isLoggable(Level.INFO))
                    _logger.info("Space Iterator has finished successfully");
                return result;
            }
            if(_spaceIteratorBatchResultsManager.anyFinishedExceptionally()) {
                if(_logger.isLoggable(Level.SEVERE))
                    _logger.severe("Space Iterator finished with exception, not all entries were retrieved");
                // TODO throw IteratorException
                return result;
            }
        }
        try {
            SpaceIteratorBatchResult spaceIteratorBatchResult = _spaceIteratorBatchResultsManager.getNextBatch(TIMEOUT);
            if(spaceIteratorBatchResult.isFailed()){
                if(_logger.isLoggable(Level.FINE))
                    _logger.fine("Space Iterator batch result " + spaceIteratorBatchResult + " returned with exception " + spaceIteratorBatchResult.getException().getMessage());
                _spaceIteratorBatchResultsManager.deactivatePartition(spaceIteratorBatchResult);
                return getNextBatch();
            }
            if(spaceIteratorBatchResult.isFinished()){
                if(_logger.isLoggable(Level.FINE))
                    _logger.fine("Space Iterator batch result " + spaceIteratorBatchResult + " has finished");
                _spaceIteratorBatchResultsManager.deactivatePartition(spaceIteratorBatchResult);
            }
            Object[] entries = spaceIteratorBatchResult.getEntries();
            if(entries == null || entries.length == 0) {
                if(_logger.isLoggable(Level.FINE))
                    _logger.fine("Space Iterator batch result " + spaceIteratorBatchResult + " contains no entries");
                return getNextBatch();
            }
            _buffer.clear();
            for (Object entry : entries)
                _buffer.add((IEntryPacket) entry);
            result = _buffer.iterator();
        } catch (Exception e) {
            processNextBatchFailure(e);
        }
        if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "getNextBatch returns with a buffer of " + _buffer.size() + " entries.");
        return result;
    }

    private void processNextBatchFailure(Exception e) {
        if (_logger.isLoggable(Level.SEVERE))
            _logger.log(Level.SEVERE, "Failed to build iterator data", e);

    }
}
