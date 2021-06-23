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
import com.gigaspaces.internal.query.RawEntryConverter;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 10.0
 */

public class GroupByAggregator extends SpaceEntriesAggregator<GroupByResult> implements SmartExternalizable {

    private static final long serialVersionUID = 1L;

    private String[] groupByPaths;
    private List<SpaceEntriesAggregator> aggregators;

    private transient Map<GroupByKey, SpaceEntriesAggregator[]> map;
    private transient GroupByKey key;
    private transient GroupByFilter filter;

    /**
     * Required for Externalizable
     */
    public GroupByAggregator() {
    }

    @Override
    public String getDefaultAlias() {
        return "groupBy" + Arrays.toString(groupByPaths);
    }

    public GroupByAggregator groupBy(String... paths) {
        this.groupByPaths = paths;
        return this;
    }

    public GroupByAggregator having(GroupByFilter filter) {
        this.filter = filter;
        return this;
    }

    private GroupByAggregator addSelector(SpaceEntriesAggregator aggregator) {
        if (aggregators == null)
            aggregators = new ArrayList<SpaceEntriesAggregator>();
        aggregators.add(aggregator);
        return this;
    }

    protected List<SpaceEntriesAggregator> getSelectAggregators() {
        return aggregators;
    }

    public GroupByAggregator select(SpaceEntriesAggregator... aggregators) {
        for (SpaceEntriesAggregator aggregator : aggregators)
            addSelector(aggregator);
        return this;
    }

    public GroupByAggregator selectCount() {
        return addSelector(new CountAggregator());
    }

    public GroupByAggregator selectCount(String path) {
        return addSelector(new CountAggregator().setPath(path));
    }

    public GroupByAggregator selectSum(String path) {
        return addSelector(new SumAggregator().setPath(path));
    }

    public GroupByAggregator selectAverage(String path) {
        return addSelector(new AverageAggregator().setPath(path));
    }

    public GroupByAggregator selectMaxValue(String path) {
        return addSelector(new MaxValueAggregator().setPath(path));
    }

    public GroupByAggregator selectMaxEntry(String path) {
        return addSelector(new MaxEntryAggregator().setPath(path));
    }

    public GroupByAggregator selectMinValue(String path) {
        return addSelector(new MinValueAggregator().setPath(path));
    }

    public GroupByAggregator selectMinEntry(String path) {
        return addSelector(new MinEntryAggregator().setPath(path));
    }

    @Override
    public void aggregate(SpaceEntriesAggregatorContext context) {
        // Initialize if first time:
        if (map == null) {
            map = new HashMap<GroupByKey, SpaceEntriesAggregator[]>();
            key = new GroupByKey(groupByPaths.length);
        }


        // Get key from current entry:
        if (!key.initialize(groupByPaths, context))
            return;

        // Get aggregated value of this key if exists, or create a new one:
        SpaceEntriesAggregator[] group = getOrCreate(key);

        // Aggregate:
        for (SpaceEntriesAggregator aggregator : group)
            aggregator.aggregate(context);
    }

    private SpaceEntriesAggregator[] getOrCreate(GroupByKey key) {
        SpaceEntriesAggregator[] group = map.get(key);
        if (group == null) {
            group = new SpaceEntriesAggregator[aggregators.size()];
            for (int i = 0; i < group.length; i++)
                group[i] = aggregators.get(i).clone();
            map.put((GroupByKey) key.clone(), group);
        }
        return group;
    }

    @Override
    public void aggregateIntermediateResult(GroupByResult partitionResult) {
        // Initialize if first time:
        if (map == null) {
            map = new HashMap<GroupByKey, SpaceEntriesAggregator[]>();
        }

        // Iterate new results and merge them with existing results:
        for (Map.Entry<GroupByKey, GroupByValue> entry : partitionResult.getMap().entrySet()) {
            SpaceEntriesAggregator[] currValue = getOrCreate(entry.getKey());
            for (int i = 0; i < currValue.length; i++) {
                Serializable currPartitionResult = (Serializable) entry.getValue().get(i);
                if (currPartitionResult != null)
                    currValue[i].aggregateIntermediateResult(currPartitionResult);
            }
        }
    }

    @Override
    public GroupByResult getIntermediateResult() {
        if (map == null)
            return null;
        Map<GroupByKey, GroupByValue> resultMap = new HashMap<GroupByKey, GroupByValue>();
        for (Map.Entry<GroupByKey, SpaceEntriesAggregator[]> entry : map.entrySet()) {
            Object[] values = new Object[aggregators.size()];
            for (int i = 0; i < values.length; i++)
                values[i] = entry.getValue()[i].getIntermediateResult();
            resultMap.put(entry.getKey(), new GroupByValue(values));
        }
        return new GroupByResult(resultMap);
    }

    @Override
    public Object getFinalResult() {
        if (map == null)
            return new GroupByResult(new HashMap<GroupByKey, GroupByValue>());
        final Map<String, Integer> keyNameIndex = new HashMap<String, Integer>();
        for (int i = 0; i < groupByPaths.length; i++)
            keyNameIndex.put(groupByPaths[i], i);
        final Map<String, Integer> valueNameIndex = AggregationInternalUtils.index(aggregators);
        Map<GroupByKey, GroupByValue> resultMap = new HashMap<GroupByKey, GroupByValue>();
        for (Map.Entry<GroupByKey, SpaceEntriesAggregator[]> entry : map.entrySet()) {
            GroupByKey key = entry.getKey();
            key.setNameIndex(keyNameIndex);
            Object[] values = new Object[aggregators.size()];
            for (int i = 0; i < values.length; i++)
                values[i] = entry.getValue()[i].getFinalResult();
            resultMap.put(key, new GroupByValue(values, valueNameIndex, key));
        }
        GroupByResult result = new GroupByResult(resultMap);
        if (filter != null)
            result.filter(filter);

        return result;
    }

    @Override
    protected void setRawEntryConverter(RawEntryConverter rawEntryConverter) {
        super.setRawEntryConverter(rawEntryConverter);
        if (map != null)
            for (SpaceEntriesAggregator[] aggregators : map.values())
                for (SpaceEntriesAggregator aggregator : aggregators)
                    aggregator.setRawEntryConverter(rawEntryConverter);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeStringArray(out, groupByPaths);
        IOUtils.writeObject(out, aggregators);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.groupByPaths = IOUtils.readStringArray(in);
        this.aggregators = IOUtils.readObject(in);
    }

    public List<SpaceEntriesAggregator> getAggregators() {
        return aggregators;
    }

    public String[] getGroupByPaths() {
        return groupByPaths;
    }
}
