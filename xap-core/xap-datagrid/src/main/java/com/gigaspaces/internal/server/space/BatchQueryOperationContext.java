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

package com.gigaspaces.internal.server.space;

import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.transport.TemplatePacketFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

/**
 * Context for batch query operations. Accumulates batch results and exceptions.
 *
 * @author anna
 * @since 7.1
 */
public abstract class BatchQueryOperationContext {
    private List<IEntryPacket> _results;
    private int _numResults;
    private List<Throwable> _exceptions;
    private final int _maxEntries;
    private final int _minEntries;
    //make sure entry is not returned more than once due to updates or reinserts
    protected HashSet<String> _processedUids;

    public BatchQueryOperationContext(ITemplatePacket template, int maxEntries, int minEntries) {
        _maxEntries = maxEntries;
        _minEntries = minEntries;
        if (keepResultsInBatchContext())
            _results = createResultList(template, maxEntries);
        createProcessedUidsIfNeeded();
    }

    /**
     * Create a list that will accumulate the batch operation results.
     */
    protected List<IEntryPacket> createResultList(ITemplatePacket template,
                                                int maxEntries) {

        if (template.getMultipleUIDs() != null)
            return new ArrayList<IEntryPacket>(Math.min(template.getMultipleUIDs().length,
                    maxEntries));

        return new LinkedList<IEntryPacket>();
    }

    public void addResult(IEntryPacket result) {
        if (keepResultsInBatchContext())
            _results.add(result);
        _numResults++;
    }

    protected boolean keepResultsInBatchContext() {
        return true;
    }

    public List<IEntryPacket> getResults() {
        return _results;
    }

    public int getNumResults() {
        return _numResults;
    }

    public List<Throwable> getExceptions() {
        return _exceptions;
    }

    public void setExceptions(List<Throwable> exceptions) {
        _exceptions = exceptions;
    }


    public boolean reachedMinEntries() {
        return (_numResults >= _minEntries);
    }

    public boolean reachedMaxEntries() {
        return _numResults >= _maxEntries;
    }

    /**
     * Handles an exception that was thrown while executing the batch operation
     */
    public abstract void onException(Throwable t);

    //should we call onExecption api ??
    public boolean needToProcessExecption() {
        return true;
    }

    //do we have to convert internal representation to external for rendering to proxy ?
    public boolean needProcessEntriesForReturnedResult() {
        return true;
    }

    public IEntryPacket[] processReturnedValueForBatchOperation(ITemplateHolder template) {
        IEntryPacket[] results = null;

        if (template.getBatchOperationContext().needProcessEntriesForReturnedResult()) {
            if (template.isReturnOnlyUid()) {
                String uids[] = new String[template.getBatchOperationContext().getResults().size()];

                int i = 0;
                for (IEntryPacket entryPacket : template.getBatchOperationContext().getResults()) {
                    uids[i++] = entryPacket.getUID();
                }

                results = new ITemplatePacket[]{TemplatePacketFactory.createUidsResponsePacket(uids)};
            } else {
                results = template.getBatchOperationContext().getResults().toArray(new IEntryPacket[template.getBatchOperationContext().getResults().size()]);
            }
        }
        return results;
    }

    public int getMaxEntries() {
        return _maxEntries;
    }

    public int getMinEntries() {
        return _minEntries;
    }

    public boolean hasAnyEntries() {
        return _numResults > 0;
    }

    protected void createProcessedUidsIfNeeded() {
    }

    public boolean isInProcessedUids(String uid) {
        return (_processedUids != null && _processedUids.contains(uid));
    }

    public void addToProcessedUidsIfNeeded(String uid) {
        if (_processedUids != null)
            _processedUids.add(uid);
    }

    public boolean isClear() {
        return false;
    }
}
