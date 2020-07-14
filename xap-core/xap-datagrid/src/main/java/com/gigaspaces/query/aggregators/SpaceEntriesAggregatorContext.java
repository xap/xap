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


package com.gigaspaces.query.aggregators;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.query.RawEntry;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 10.0
 */
public abstract class SpaceEntriesAggregatorContext {

    private final Collection<SpaceEntriesAggregator> aggregators;
    private final Map<String, Object> pathCache;

    protected SpaceEntriesAggregatorContext(Collection<SpaceEntriesAggregator> aggregators) {
        this.aggregators = aggregators;
        pathCache = aggregators.size() > 1 ? new HashMap<String, Object>() : null;
    }

    public Object getPathValue(String path) {
        if (pathCache == null)
            return getPathValueImpl(path);

        if (pathCache.containsKey(path))
            return pathCache.get(path);
        Object value = getPathValueImpl(path);
        pathCache.put(path, value);
        return value;
    }

    public abstract int getPartitionId();

    public abstract String getEntryUid();

    /**
     * @return the raw entry match by this context
     * @see com.gigaspaces.internal.query.EntryHolderAggregatorContext#getRawEntry
     * @since 10.0
     */
    public abstract RawEntry getRawEntry();

    public abstract void applyProjectionTemplate(RawEntry entry);

    protected abstract Object getPathValueImpl(String path);

    public abstract ITypeDesc getTypeDescriptor();

    protected void aggregate() {
        if (pathCache != null)
            pathCache.clear();

        for (SpaceEntriesAggregator aggregator : aggregators)
            aggregator.aggregate(this);
    }
}
