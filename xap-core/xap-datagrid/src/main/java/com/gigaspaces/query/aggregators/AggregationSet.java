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
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

/**
 * The set of aggregations to run for matches entries of the aggregate operation.
 *
 * @author Niv Ingberg
 * @since 10.0
 */

public class AggregationSet implements SmartExternalizable {
    private static final long serialVersionUID = 1L;

    private final ArrayList<SpaceEntriesAggregator> aggregators = new ArrayList<SpaceEntriesAggregator>();

    public AggregationSet() {
    }

    public AggregationSet add(SpaceEntriesAggregator aggregator) {
        aggregators.add(aggregator);
        return this;
    }

    /**
     * Counts matching entries.
     */
    public AggregationSet count() {
        return add(new CountAggregator());
    }

    /**
     * Counts matching entries whose path is not null.
     *
     * @param path Path to inspect
     */
    public AggregationSet count(String path) {
        return add(new CountAggregator().setPath(path));
    }

    /**
     * Sums values of paths of matching entries.
     *
     * @param path Path to sum (must be a numeric type)
     */
    public AggregationSet sum(String path) {
        return add(new SumAggregator().setPath(path));
    }

    /**
     * Calculates average of path values of matching entries.
     *
     * @param path Path to average (must be a numeric type)
     */
    public AggregationSet average(String path) {
        return add(new AverageAggregator().setPath(path));
    }

    /**
     * Returns maximum value of path of matching entries.
     *
     * @param path Path to inspect (must be a comparable type)
     * @see #maxEntry(String)
     */
    public AggregationSet maxValue(String path) {
        return add(new MaxValueAggregator().setPath(path));
    }

    /**
     * Returns entry with maximum value of path of matching entries.
     *
     * @param path Path to inspect (must be a comparable type)
     * @see #maxValue(String)
     */
    public AggregationSet maxEntry(String path) {
        return add(new MaxEntryAggregator().setPath(path));
    }

    /**
     * Returns minimum value of path of matching entries.
     *
     * @param path Path to inspect (must be a comparable type)
     * @see #minEntry(String)
     */
    public AggregationSet minValue(String path) {
        return add(new MinValueAggregator().setPath(path));
    }

    /**
     * Returns entry with minimum value of path of matching entries.
     *
     * @param path Path to inspect (must be a comparable type)
     * @see #minEntry(String)
     */
    public AggregationSet minEntry(String path) {
        return add(new MinEntryAggregator().setPath(path));
    }

    public AggregationSet groupBy(GroupByAggregator aggregator) {
        return add(aggregator);
    }

    public AggregationSet orderBy(OrderByAggregator aggregator) {
        return add(aggregator);
    }

    public AggregationSet distinct(DistinctAggregator aggregator) {
        return add(aggregator);
    }

    List<SpaceEntriesAggregator> getAggregators() {
        return aggregators;
    }

    public boolean isEmpty() {
        return this.aggregators.isEmpty();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(aggregators.size());
        for (SpaceEntriesAggregator aggregator : aggregators) {
            IOUtils.writeObject(out, aggregator);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        aggregators.ensureCapacity(size);
        for (int i = 0; i < size; i++) {
            aggregators.add(IOUtils.readObject(in));
        }
    }
}
