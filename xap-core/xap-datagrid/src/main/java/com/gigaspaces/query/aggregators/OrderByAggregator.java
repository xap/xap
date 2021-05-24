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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Aggregator for order by operation. Supports several paths,asc/desc and limited results
 *
 * @author anna
 * @since 10.1
 */

public class OrderByAggregator<T> extends SpaceEntriesAggregator<OrderByAggregator.OrderByScanResult> implements Externalizable {

    private static final long serialVersionUID = 1L;
    //used to post process the entries and apply projection template
    private transient SpaceEntriesAggregatorContext context;
    private transient List<OrderByElement> list;


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

    public List<OrderByPath> getOrderByPaths() {
        return Collections.unmodifiableList(orderByPaths);
    }


    @Override
    public String getDefaultAlias() {
        return "order by (" + orderByPaths.toString() + ")";
    }

    @Override
    public void aggregate(SpaceEntriesAggregatorContext context) { //at each partition (server)
        this.context = context;

        if (list == null) {
            list = new ArrayList<>();
        }

        list.add(new OrderByElement(orderByPaths, context));
        aggregatedCount++;

        //if found more than allowed limit - evict highest
        if (aggregatedCount > limit) {
            list.sort(new OrderByElementComparator(this.orderByPaths));
            list.remove(list.size() - 1);
            aggregatedCount--;
        }

    }


    @Override
    public void aggregateIntermediateResult(OrderByScanResult partitionResult) { //at the client
        // Initialize if first time:
        if (list == null) {
            list = new ArrayList<>();
        }

        List<OrderByElement> partitionResultList = partitionResult.getResultList();
        if (partitionResultList == null) {
            return;
        }
        //collect results from each partitions
        list.addAll(partitionResultList);
        aggregatedCount += partitionResultList.size();
        //if found more than allowed limit - evict highest
        if (aggregatedCount > limit) {
            list.sort(new OrderByElementComparator(this.orderByPaths));
            while (aggregatedCount > limit) {
                list.remove(list.size() - 1);
                aggregatedCount--;
            }
        }
    }

    @Override
    public OrderByScanResult getIntermediateResult() {

        OrderByScanResult orderByResult = new OrderByScanResult();
        if (list != null) {
            list.forEach(orderByElement -> context.applyProjectionTemplate(orderByElement.getRawEntry()));
            list.sort(new OrderByElementComparator(this.orderByPaths));
            orderByResult.setResultList(list);
        }
        return orderByResult;
    }


    @Override
    public Collection<T> getFinalResult() {
        if (list == null) {
            return new ArrayList<>();
        }
        list.sort(new OrderByElementComparator(this.orderByPaths));
        return list.stream()
                .map(orderByElement -> (T) toObject(orderByElement.getRawEntry()))
                .collect(Collectors.toList());
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

        private List<OrderByElement> resultList;

        /**
         * Required for Externalizable
         */
        public OrderByScanResult() {
        }

        public List<OrderByElement> getResultList() {
            return resultList;
        }

        public void setResultList(List<OrderByElement> resultList) {
            this.resultList = resultList;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            IOUtils.writeList(out, resultList);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.resultList = IOUtils.readList(in);
        }
    }

    public static class OrderByElementComparator implements Comparator<OrderByElement> {

        private final List<OrderByPath> orderByPaths;

        public OrderByElementComparator(List<OrderByPath> orderByPaths) {
            this.orderByPaths = orderByPaths;
        }

        @Override
        public int compare(OrderByElement o1, OrderByElement o2) {
            int rc = 0;
            for (int i = 0; i< this.orderByPaths.size() ; i++) {
                Comparable c1 = (Comparable) o1.getValue(i);
                Comparable c2 = (Comparable) o2.getValue(i);

                if (c1 == c2) {
                    continue;
                }
                if (c1 == null) {
                    return this.orderByPaths.get(i).isNullsLast() ? 1 : -1;
                }
                if (c2 == null) {
                    return this.orderByPaths.get(i).isNullsLast() ? -1 : 1;
                }
                rc = c1.compareTo(c2);
                if (rc != 0) {
                    return this.orderByPaths.get(i).getOrderBy() == OrderBy.DESC ? -rc : rc;
                }
            }
            return rc;
        }
    }
}
