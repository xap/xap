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
package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.internal.query.*;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.TypeDataIndex;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.CompositeRange;
import com.j_spaces.jdbc.builder.range.Range;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author yael nahon
 * @since 12.0.1
 */
@ExperimentalApi
public class ExplainPlanUtil {

    public static final String  REPORT_START = "******************** Explain plan report ********************";
    public static final String REPORT_END = "*************************************************************";

    public static IndexInfo createIndexInfo(AbstractQueryIndex abstractIndexScanner, TypeDataIndex index, TypeData typeData, int size, boolean usable) {
        if (index == null){
            IndexInfo indexInfo = new IndexInfo();
            indexInfo.setUsable(false);
            return indexInfo;
        }
        if (abstractIndexScanner instanceof RangeIndexScanner) {
            RangeIndexScanner rangeIndexScanner = (RangeIndexScanner) abstractIndexScanner;
            QueryOperator operator = getOperatorForRangeIndex(rangeIndexScanner.getMin(), rangeIndexScanner.getMax(), rangeIndexScanner.isIncludeMin(), rangeIndexScanner.isIncludeMax());
            if (operator == QueryOperator.BETWEEN) {
                return new BetweenIndexInfo(rangeIndexScanner.getIndexName(), size, index.getIndexType(),
                        rangeIndexScanner.getMin(), rangeIndexScanner.isIncludeMin(), rangeIndexScanner.getMax(), rangeIndexScanner.isIncludeMax(), operator, usable);
            }
            if (rangeIndexScanner.getMin() != null) {
                return new IndexInfo(rangeIndexScanner.getIndexName(), size, index.getIndexType(), rangeIndexScanner.getMin(), operator, usable);
            }
            return new IndexInfo(rangeIndexScanner.getIndexName(), size, index.getIndexType(), rangeIndexScanner.getMax(), operator, usable);
        }
        if (abstractIndexScanner instanceof ExactValueIndexScanner) {
            return new IndexInfo(abstractIndexScanner.getIndexName(), size, index.getIndexType(), abstractIndexScanner.getIndexValue(), QueryOperator.EQ, usable);
        }
        if (abstractIndexScanner instanceof InValueIndexScanner) {
            return new IndexInfo(abstractIndexScanner.getIndexName(), size, index.getIndexType(), ((InValueIndexScanner) abstractIndexScanner).get_indexInValueSet(), QueryOperator.IN, usable);
        }
        if (abstractIndexScanner instanceof NotRegexIndexScanner) {
            return new IndexInfo(abstractIndexScanner.getIndexName(), size, index.getIndexType(), abstractIndexScanner.getIndexValue(), QueryOperator.NOT_REGEX, usable);
        }
        if (abstractIndexScanner instanceof RegexIndexScanner) {
            return new IndexInfo(abstractIndexScanner.getIndexName(), size, index.getIndexType(), abstractIndexScanner.getIndexValue(), QueryOperator.REGEX, usable);
        }
        if (abstractIndexScanner instanceof NullValueIndexScanner) {
            return new IndexInfo(abstractIndexScanner.getIndexName(), size, index.getIndexType(), abstractIndexScanner.getIndexValue(), QueryOperator.IS_NULL, usable);
        }
        return null;
    }

    private static QueryOperator getOperatorForRangeIndex(Comparable min, Comparable max, boolean includeMin, boolean includeMax) {
        QueryOperator operator = null;
        if (max != null) {
            if (min != null) {
                operator = QueryOperator.BETWEEN;
            } else if (includeMax) {
                operator = QueryOperator.LE;
            } else {
                operator = QueryOperator.LT;
            }
        } else if (min != null) {
            if (includeMin) {
                operator = QueryOperator.GE;
            } else {
                operator = QueryOperator.GT;
            }
        }
        return operator;

    }

    public static QueryOperationNode BuildMatchCodes(QueryTemplatePacket packet) {
        QueryJunctionNode and = new QueryJunctionNode("AND");
        Object[] fieldValues = packet.getFieldValues();
        short[] extendedMatchCodes = packet.getExtendedMatchCodes();
        Object[] rangeValues = packet.getRangeValues();
        String[] propertiesNames = packet.getTypeDescriptor().getPropertiesNames();
        boolean[] valuesInclusion = packet.getRangeValuesInclusion();

        for (int i = 0; i < fieldValues.length; i++) {
            if (fieldValues[i] == null){
                continue;
            }
            if (rangeValues == null) {
                and.getChildren().add(getMatchNode(propertiesNames[i], fieldValues[i], getQueryOperator(extendedMatchCodes[i]), null, false));
            }else {
                and.getChildren().add(getMatchNode(propertiesNames[i], fieldValues[i], getQueryOperator(extendedMatchCodes[i]), rangeValues[i], valuesInclusion[i]));
            }

        }
        return and;
    }

    private static QueryOperationNode getMatchNode(String propertyName, Object fieldValue, QueryOperator queryOperator, Object rangeValue, boolean includeRangeValue) {
        if(rangeValue == null){
            return  new RangeNode(propertyName, fieldValue, queryOperator, null);
        }
        switch (queryOperator){
            case GT: return new BetweenRangeNode(propertyName, QueryOperator.BETWEEN, null,(Comparable) fieldValue, false, (Comparable) rangeValue, includeRangeValue);
            case GE: return new BetweenRangeNode(propertyName, QueryOperator.BETWEEN, null,(Comparable) fieldValue, true, (Comparable) rangeValue, includeRangeValue);
            case LT: return new BetweenRangeNode(propertyName, QueryOperator.BETWEEN, null, (Comparable) rangeValue, includeRangeValue, (Comparable) fieldValue, false);
            case LE: return new BetweenRangeNode(propertyName, QueryOperator.BETWEEN, null, (Comparable) rangeValue, includeRangeValue, (Comparable) fieldValue, true);
        }
        return  null;
    }

    public static QueryOperator getQueryOperator(short extendedMatchCode) {
        switch (extendedMatchCode){
            case 0: return QueryOperator.EQ;
            case 1: return QueryOperator.NE;
            case 2: return QueryOperator.GT;
            case 3: return QueryOperator.GE;
            case 4: return QueryOperator.LT;
            case 5: return QueryOperator.LE;
            case 6: return QueryOperator.IS_NULL;
            case 7: return QueryOperator.NOT_NULL;
            case 8: return QueryOperator.REGEX;
            case 9: return QueryOperator.CONTAINS_TOKEN;
            case 10: return QueryOperator.NOT_REGEX;
            case 11: return QueryOperator.IN;
            default: return QueryOperator.NOT_SUPPORTED;
        }
    }

    public static QueryOperationNode buildQueryTree(ICustomQuery customQuery) {
        QueryOperationNode currentNode = QueryTypes.getNode(customQuery);
        List<ICustomQuery> subQueries = getSubQueries(customQuery);
        if (subQueries == null) {
            return currentNode;
        }

        List<ICustomQuery> finalSubqueries = new ArrayList<ICustomQuery>();
        finalSubqueries.addAll(subQueries);
        for (ICustomQuery subQuery : subQueries) {
            if (subQuery instanceof CompositeRange) {
                finalSubqueries.remove(subQuery);
                finalSubqueries.addAll(getSubQueries(subQuery));
            }
        }

        for (ICustomQuery subQuery : finalSubqueries) {
            QueryOperationNode son = buildQueryTree(subQuery);
            currentNode.getChildren().add(son);
        }

        return currentNode;
    }

    public static List<ICustomQuery> getSubQueries(ICustomQuery customQuery) {
        if (customQuery instanceof CompoundContainsItemsCustomQuery) {
            return compoundConvertList(((CompoundContainsItemsCustomQuery) customQuery).getSubQueries());
        }
        if (customQuery instanceof CompoundAndCustomQuery) {
            return ((CompoundAndCustomQuery) customQuery).get_subQueries();
        }
        if (customQuery instanceof CompoundOrCustomQuery) {
            return ((CompoundOrCustomQuery) customQuery).get_subQueries();
        }
        if (customQuery instanceof CompositeRange) {
            return rangeConvertList(((CompositeRange) customQuery).get_ranges());
        }
        return null;
    }

    public static List<ICustomQuery> rangeConvertList(LinkedList<Range> ranges) {
        List<ICustomQuery> res = new ArrayList<ICustomQuery>();
        for (Range range : ranges) {
            res.add(range);
        }
        return res;
    }

    public static List<ICustomQuery> compoundConvertList(List<IContainsItemsCustomQuery> subQueries) {
        List<ICustomQuery> res = new ArrayList<ICustomQuery>();
        for (IContainsItemsCustomQuery subQuery : subQueries) {
            res.add(subQuery);
        }
        return res;
    }

    public static boolean notEmpty(String str) {
        return str != null && !str.isEmpty();
    }

}
