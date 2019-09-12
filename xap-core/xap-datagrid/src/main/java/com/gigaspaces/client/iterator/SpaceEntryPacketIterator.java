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

import com.gigaspaces.SpaceRuntimeException;
import com.gigaspaces.client.iterator.internal.SpaceIteratorAggregator;
import com.gigaspaces.client.iterator.internal.SpaceIteratorResult;
import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.metadata.ObjectType;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.query.aggregators.AggregationSet;
import com.j_spaces.core.UidQueryPacket;
import com.j_spaces.jdbc.builder.SQLQueryTemplatePacket;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;

import java.io.Closeable;
import java.rmi.RemoteException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class SpaceEntryPacketIterator implements Iterator<IEntryPacket>, Closeable {
    private static final Logger _logger = Logger.getLogger(Constants.LOGGER_GSITERATOR);

    private final ISpaceProxy _spaceProxy;
    private final Transaction _txn;
    private final int _batchSize;
    private final int _readModifiers;
    private final ITemplatePacket _queryPacket;
    private final QueryResultTypeInternal _queryResultType;
    private final SpaceIteratorResult _iteratorResult;
    private final List<IEntryPacket> _buffer;
    private Iterator<IEntryPacket> _bufferIterator;
    private boolean _closed;

    public SpaceEntryPacketIterator(ISpaceProxy spaceProxy, Object query, Transaction txn, int batchSize, int modifiers) {
        if (spaceProxy == null)
            throw new IllegalArgumentException("space argument must not be null.");
        if (query == null)
            throw new IllegalArgumentException("query argument must not be null.");
        if (batchSize <= 0)
            throw new IllegalArgumentException("batchSize argument must be greater than zero.");

        if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "SpaceIterator initialized with batchSize=" + batchSize);

        this._spaceProxy = spaceProxy;
        this._txn = txn;
        this._batchSize = batchSize;
        this._readModifiers = modifiers;
        this._queryPacket = toTemplatePacket(query);
        this._queryResultType = _queryPacket.getQueryResultType();
        this._iteratorResult = initialize();
        this._bufferIterator = _iteratorResult.getEntries().iterator();
        this._buffer = new LinkedList<IEntryPacket>();
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
        if (_bufferIterator == null)
            result = false;
        else {
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
            _iteratorResult.close();
            _buffer.clear();
        }
    }

    private SpaceIteratorResult initialize() {
        final AggregationSet aggregationSet = new AggregationSet().add(new SpaceIteratorAggregator()
                .setBatchSize(_batchSize));
        final SpaceIteratorResult result;

        try {
            result = (SpaceIteratorResult) _spaceProxy.aggregate(_queryPacket, aggregationSet, _txn, _readModifiers).get(0);
        } catch (RemoteException e) {
            throw new SpaceRuntimeException("Failed to initialize iterator", e);
        } catch (TransactionException e) {
            throw new SpaceRuntimeException("Failed to initialize iterator", e);
        } catch (InterruptedException e) {
            throw new SpaceRuntimeException("Failed to initialize iterator", e);
        }

        if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "initialize found " + result.size() + " matching entries.");

        return result;
    }

    private Iterator<IEntryPacket> getNextBatch() {
        _buffer.clear();
        UidQueryPacket template = _iteratorResult.buildQueryPacket(_batchSize, _queryResultType);
        if (template == null)
            return null;

        template.setProjectionTemplate(_queryPacket.getProjectionTemplate());
        Iterator<IEntryPacket> result = null;
        try {
            Object[] entries = _spaceProxy.readMultiple(template, _txn, template.getMultipleUIDs().length, _readModifiers);
            _buffer.clear();
            for (Object entry : entries)
                _buffer.add((IEntryPacket) entry);
            result = _buffer.iterator();
        } catch (RemoteException e) {
            processNextBatchFailure(e);
        } catch (UnusableEntryException e) {
            processNextBatchFailure(e);
        } catch (TransactionException e) {
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
