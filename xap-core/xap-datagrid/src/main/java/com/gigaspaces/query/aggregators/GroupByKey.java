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

import com.gigaspaces.query.CompoundResult;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * @author Niv Ingberg
 * @since 10.0
 */

public class GroupByKey extends CompoundResult {

    private static final long serialVersionUID = 1L;

    /**
     * Required for Externalizable
     */
    public GroupByKey() {
    }

    public GroupByKey(Object[] values) {
        super(values, null);
    }

    protected GroupByKey(int numOfValues) {
        super(new Object[numOfValues], null);
    }

    protected boolean initialize(String[] groupByPaths, SpaceEntriesAggregatorContext context) {
        hashCode = 0;
        for (int i = 0; i < groupByPaths.length; i++) {
            String groupByPath = groupByPaths[i];

            //Loop over the aggregators in select clause and find the relevant one by path.
            Collection<SpaceEntriesAggregator> aggregators = context.getAggregators(); // in case of GroupBy there is one in this list.
            for (SpaceEntriesAggregator aggregator : aggregators) {
                if (aggregator instanceof GroupByAggregator) {
                    Optional<SingleValueFunctionAggregator> agg = ((GroupByAggregator) aggregator).getAggregators()
                            .stream()
                            .filter(x -> x instanceof SingleValueFunctionAggregator)
                            .map(a -> (SingleValueFunctionAggregator)a)
                            .filter(a -> groupByPath.equals(a.getPath()))
                            .findFirst();
                    if(agg.isPresent())
                        values[i] = agg.get().calculateValue(context.getPathValue(groupByPath));
                    break;
                }
            }

            if (values[i] == null)
                return false;
        }

        return true;
    }

    protected void setNameIndex(Map<String, Integer> nameIndexMap) {
        this.nameIndexMap = nameIndexMap;
    }
}
