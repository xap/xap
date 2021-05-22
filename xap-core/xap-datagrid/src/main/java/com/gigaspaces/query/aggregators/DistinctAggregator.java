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

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.query.RawEntry;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Aggregator for fetching distinct entries - based on some fields.
 *
 * @author anna
 * @since 10.1
 */

public class DistinctAggregator<T> extends SpaceEntriesAggregator<DistinctResult> implements SmartExternalizable {

    private static final long serialVersionUID = 1L;

    //used to post process the entries and apply projection template
    private transient SpaceEntriesAggregatorContext context;

    private String[] distinctPaths;
    private transient Map<DistinctPropertiesKey, RawEntry> map;
    private transient DistinctPropertiesKey key;
    private int limit = Integer.MAX_VALUE;

    public DistinctAggregator() {
    }


    public DistinctAggregator distinct(String... paths) {
        if (paths.length == 0)
            throw new IllegalArgumentException("No paths were set");
        distinctPaths = paths;
        return this;
    }

    public DistinctAggregator distinct(Integer limit, String... paths) {
        this.limit = limit;
        return distinct(paths);
    }

    @Override
    public String getDefaultAlias() {
        return "distinct (" + Arrays.toString(distinctPaths) + ")";
    }

    @Override
    public void aggregate(SpaceEntriesAggregatorContext context) {
        this.context = context;

        if (map == null) {
            map = new HashMap<DistinctPropertiesKey, RawEntry>();
            key = new DistinctPropertiesKey(distinctPaths.length);
        }

        // Get key from current entry:
        if (!key.initialize(distinctPaths, context))
            return;

        if (map.get(key) == null && map.size() < limit)
            map.put((DistinctPropertiesKey) key.clone(), context.getRawEntry());
    }

    @Override
    public void aggregateIntermediateResult(DistinctResult partitionResult) {
        // Initialize if first time:
        if (map == null) {
            map = new HashMap<DistinctPropertiesKey, RawEntry>();
        }

        if (partitionResult.getMap() == null)
            return;
        // Iterate new results and merge them with existing results:
        for (Map.Entry<DistinctPropertiesKey, RawEntry> entry : partitionResult.getMap().entrySet()) {
            if (map.get(entry.getKey()) == null && map.size() < limit)
                map.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public DistinctResult getIntermediateResult() {
        if (map == null)
            return null;


        for (RawEntry entry : map.values()) {
            context.applyProjectionTemplate(entry);
        }
        return new DistinctResult(map);
    }


    @Override
    public Collection<T> getFinalResult() {
        if (map == null)
            return new ArrayList<T>();

        ArrayList<T> list = new ArrayList<T>();
        for (RawEntry entry : map.values())
            list.add((T) toObject(entry));

        return list;
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, distinctPaths);
        out.writeInt(limit);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        distinctPaths = IOUtils.readObject(in);
        limit = in.readInt();
    }

}
