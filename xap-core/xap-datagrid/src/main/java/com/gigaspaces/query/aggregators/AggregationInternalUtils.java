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

import com.gigaspaces.client.iterator.internal.SpaceIteratorAggregator;
import com.gigaspaces.internal.query.RawEntryConverter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is intended for internal usage only.
 *
 * @author Niv Ingberg
 * @since 10.0
 */

public class AggregationInternalUtils {

    private static final Set<Class> _certifiedAggregators = initCertifiedAggregators();

    private static Set<Class> initCertifiedAggregators() {
        Set<Class> result = new HashSet<Class>();
        result.add(CountAggregator.class);
        result.add(SumAggregator.class);
        result.add(AverageAggregator.class);
        result.add(MaxValueAggregator.class);
        result.add(MaxEntryAggregator.class);
        result.add(MinValueAggregator.class);
        result.add(MinEntryAggregator.class);
        result.add(SpaceIteratorAggregator.class);
        return result;
    }

    public static List<SpaceEntriesAggregator> getAggregators(AggregationSet aggregationSet) {
        return aggregationSet.getAggregators();
    }

    public static Map<String, Integer> index(List<SpaceEntriesAggregator> aggregators) {
        Map<String, Integer> nameIndex = new HashMap<String, Integer>();
        for (int i = 0; i < aggregators.size(); i++) {
            SpaceEntriesAggregator aggregator = aggregators.get(i);
            String alias = aggregator.getAlias();
            if (alias == null)
                alias = aggregator.getDefaultAlias();
            nameIndex.put(alias, i);
        }
        return nameIndex;
    }

    public static AggregationResult getFinalResult(List<SpaceEntriesAggregator> aggregators, RawEntryConverter rawEntryConverter) {
        Object[] results = new Object[aggregators.size()];
        for (int i = 0; i < results.length; i++) {
            aggregators.get(i).setRawEntryConverter(rawEntryConverter);
            results[i] = aggregators.get(i).getFinalResult();
        }
        return new AggregationResult(results, index(aggregators));
    }

    public static boolean containsCustomAggregators(List<SpaceEntriesAggregator> aggregators) {
        for (SpaceEntriesAggregator aggregator : aggregators) {
            if (aggregator instanceof GroupByAggregator) {
                if (containsCustomAggregators(((GroupByAggregator) aggregator).getSelectAggregators()))
                    return true;
            } else {
                if (!_certifiedAggregators.contains(aggregator.getClass()))
                    return true;
            }
        }

        return false;
    }

    public static List<SpaceEntriesAggregator> getSelectors(GroupByAggregator aggregator) {
        return aggregator.getSelectAggregators();
    }
}
