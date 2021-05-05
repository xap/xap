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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

/**
 * Aggregator for order by operation. Supports several paths,asc/desc and limited results
 *
 * @author anna
 * @since 10.1
 */

public class OrderByAggregator<T> extends SpaceEntriesAggregator<OrderByAggregator.OrderByScanResult> implements Externalizable {

    //used to post process the entries and apply projection template
    private transient SpaceEntriesAggregatorContext context;

    private static final long serialVersionUID = 1L;

    private transient SortedMap<OrderByKey, ArrayList<RawEntry>> map;
    private transient OrderByKey key;


    private int limit = Integer.MAX_VALUE;
    private transient int aggregatedCount = 0;

    private List<OrderByPath> orderByPaths = new LinkedList<OrderByPath>();

    public OrderByAggregator() {
    }

    public OrderByAggregator(int limit) {
        this.limit = limit;
    }

    public OrderByAggregator orderBy(String path, OrderBy orderBy) {
        return orderBy(path, orderBy, false);
    }

    public OrderByAggregator orderBy(String path) {
        return orderBy(path, OrderBy.ASC, false);
    }

    public OrderByAggregator orderBy(String path, OrderBy orderBy, boolean nullsLast) {

        orderByPaths.add(new OrderByPath(path, orderBy, nullsLast));

        return this;
    }

    public int getLimit() {
        return limit;
    }

    public List<OrderByPath> getOrderByPaths() {
        return Collections.unmodifiableList(orderByPaths);
    }


    @Override
    public String getDefaultAlias() {
        return "order by (" + orderByPaths.toString() + ")";
    }

    @Override
    public void aggregate(SpaceEntriesAggregatorContext context) {
        this.context = context;
        if (map == null) {
            map = new TreeMap<OrderByKey, ArrayList<RawEntry>>();
            key = new OrderByKey(orderByPaths.size());
        }

        // Get key from current entry:
        if (!key.initialize(orderByPaths, this.context))
            return;

        if (map.get(key) == null)
            map.put((OrderByKey) key.clone(), new ArrayList<RawEntry>());

        map.get(key).add(context.getRawEntry());

        aggregatedCount++;
        //if more found more than allowed limit - evict
        if (aggregatedCount > limit) {
            removeLastEntry();
            aggregatedCount--;
        }
    }


    @Override
    public void aggregateIntermediateResult(OrderByScanResult partitionResult) {
        // Initialize if first time:
        if (map == null) {
            map = new TreeMap<OrderByKey, ArrayList<RawEntry>>();
        }

        if (partitionResult.getResultMap() == null)
            return;
        // Iterate new results and merge them with existing results:
        for (Map.Entry<OrderByKey, ArrayList<RawEntry>> entry : partitionResult.getResultMap().entrySet()) {
            ArrayList<RawEntry> entryList = entry.getValue();
            ArrayList<RawEntry> currentEntryList = map.get(entry.getKey());

            if (currentEntryList == null) {
                map.put(entry.getKey(), entryList);
            } else {
                currentEntryList.addAll(entryList);
            }
            aggregatedCount += entryList.size();

            //if more found more than allowed limit - evict
            while (aggregatedCount > limit) {
                removeLastEntry();
                aggregatedCount--;
            }

        }
    }

    @Override
    public OrderByScanResult getIntermediateResult() {

        OrderByScanResult orderByResult = new OrderByScanResult();
        if (map != null) {

            for (List<RawEntry> entriesList : map.values()) {
                for (RawEntry entry : entriesList)
                    context.applyProjectionTemplate(entry);

            }
            orderByResult.setResultMap(map);
        }
        return orderByResult;
    }


    @Override
    public Collection<T> getFinalResult() {

        ArrayList<T> list = new ArrayList<T>(aggregatedCount);
        if (map == null) {
            return list;
        }

        for (List<RawEntry> entriesList : map.values()) {
            for (RawEntry entry : entriesList)
                list.add((T) toObject(entry));
        }

        return list;
    }

    private void removeLastEntry() {

        ArrayList<RawEntry> lastEntries = map.get(map.lastKey());
        RawEntry r = lastEntries.remove(lastEntries.size() - 1);

        if (lastEntries.isEmpty()) {
            map.remove(map.lastKey());
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, orderByPaths);
        out.writeInt(limit);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        orderByPaths = IOUtils.readObject(in);
        limit = in.readInt();
    }


    public static class OrderByScanResult implements Externalizable {

        private static final long serialVersionUID = 1L;

        private Map<OrderByKey, ArrayList<RawEntry>> resultMap;

        public Map<OrderByKey, ArrayList<RawEntry>> getResultMap() {
            return resultMap;
        }

        /**
         * Required for Externalizable
         */
        public OrderByScanResult() {
        }


        public void setResultMap(Map<OrderByKey, ArrayList<RawEntry>> resultMap) {
            this.resultMap = resultMap;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {

            IOUtils.writeObject(out, resultMap);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

            this.resultMap = IOUtils.readObject(in);
        }

    }
}
