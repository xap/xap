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
import com.gigaspaces.internal.query.CompoundAndCustomQuery;
import com.gigaspaces.internal.query.CompoundContainsItemsCustomQuery;
import com.gigaspaces.internal.query.CompoundOrCustomQuery;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.query.predicate.comparison.InSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.NotEqualsSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.NotRegexSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.RegexSpacePredicate;
import com.j_spaces.jdbc.builder.range.CompositeRange;
import com.j_spaces.jdbc.builder.range.ContainsItemValueRange;
import com.j_spaces.jdbc.builder.range.ContainsValueRange;
import com.j_spaces.jdbc.builder.range.EqualValueRange;
import com.j_spaces.jdbc.builder.range.FunctionCallDescription;
import com.j_spaces.jdbc.builder.range.InRange;
import com.j_spaces.jdbc.builder.range.IsNullRange;
import com.j_spaces.jdbc.builder.range.NotEqualValueRange;
import com.j_spaces.jdbc.builder.range.NotNullRange;
import com.j_spaces.jdbc.builder.range.NotRegexRange;
import com.j_spaces.jdbc.builder.range.RegexRange;
import com.j_spaces.jdbc.builder.range.RelationRange;
import com.j_spaces.jdbc.builder.range.SegmentRange;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tamirt on 23/08/16.
 */
@ExperimentalApi
public enum QueryTypes {
    COMPOUND_CONTAINS_ITEMS_CUSTOM_QUERY,
    COMPOUND_AND_CUSTOM_QUERY,
    COMPOUND_OR_CUSTOM_QUERY,
    RELATION_RANGE,
    SEGMENT_RANGE,
    EQUAL_VALUE_RANGE,
    CONTAINS_VALUE_RANGE,
    CONTAINS_ITEM_VALUE_RANGE,
    IN_RANGE,
    IS_NULL_RANGE,
    NOT_NULL_RANGE,
    NOT_EQUAL_VALUE_RANGE,
    REGEX_RANGE,
    NOT_REGEX_RANGE,
    COMPOSITE_RANGE;

    public static final Map<Class<?>, QueryTypes> queryTypes = Collections.unmodifiableMap(initMap());

    private static Map<Class<?>, QueryTypes> initMap() {
        Map<Class<?>, QueryTypes> map = new HashMap<Class<?>, QueryTypes>();
        map.put(CompoundContainsItemsCustomQuery.class, QueryTypes.COMPOUND_CONTAINS_ITEMS_CUSTOM_QUERY);
        map.put(CompoundAndCustomQuery.class, QueryTypes.COMPOUND_AND_CUSTOM_QUERY);
        map.put(CompoundOrCustomQuery.class, QueryTypes.COMPOUND_OR_CUSTOM_QUERY);
        map.put(RelationRange.class, QueryTypes.RELATION_RANGE);
        map.put(SegmentRange.class, QueryTypes.SEGMENT_RANGE);
        map.put(EqualValueRange.class, QueryTypes.EQUAL_VALUE_RANGE);
        map.put(ContainsValueRange.class, QueryTypes.CONTAINS_VALUE_RANGE);
        map.put(InRange.class, QueryTypes.IN_RANGE);
        map.put(IsNullRange.class, QueryTypes.IS_NULL_RANGE);
        map.put(NotNullRange.class, QueryTypes.NOT_NULL_RANGE);
        map.put(NotEqualValueRange.class, QueryTypes.NOT_EQUAL_VALUE_RANGE);
        map.put(RegexRange.class, QueryTypes.REGEX_RANGE);
        map.put(NotRegexRange.class,QueryTypes.NOT_REGEX_RANGE);
        map.put(ContainsItemValueRange.class, QueryTypes.CONTAINS_ITEM_VALUE_RANGE);
        map.put(CompositeRange.class, QueryTypes.COMPOSITE_RANGE);
        return map;
    }

    public static QueryOperationNode getNode(ICustomQuery customQuery) {
        QueryTypes queryType = queryTypes.get(customQuery.getClass());
        switch (queryType) {
            case COMPOUND_CONTAINS_ITEMS_CUSTOM_QUERY:
                return new QueryJunctionNode("CONTAINS");
            case COMPOUND_AND_CUSTOM_QUERY:
                return new QueryJunctionNode("AND");
            case COMPOUND_OR_CUSTOM_QUERY:
                return new QueryJunctionNode("OR");
            case COMPOSITE_RANGE:
                return new QueryJunctionNode("COMPOSITE");
            case RELATION_RANGE:
                RelationRange relation = (RelationRange) customQuery;
                String relationFunc = createFunctionString(relation.getFunctionCallDescription(), relation.getPath());
                return new RangeNode(relation.getPath(), relation.getValue(), getRelationOperator(relation.getRelation()), relationFunc);
            case SEGMENT_RANGE:
                SegmentRange segment = (SegmentRange) customQuery;
                String segmentFunc = createFunctionString(segment.getFunctionCallDescription(), segment.getPath());
                Object value = segment.getMin() != null ? segment.getMin() : segment.getMax();
                if(segment.getMin() != null && segment.getMax() != null ){
                    return new BetweenRangeNode(segment.getPath(), getSegmentOperator(segment), segmentFunc, segment.getMin(),segment.isIncludeMin(),  segment.getMax(), segment.isIncludeMax());
                }
                return new RangeNode(segment.getPath(), value, getSegmentOperator(segment), segmentFunc);
            case EQUAL_VALUE_RANGE:
                EqualValueRange equalValue = (EqualValueRange) customQuery;
                String equalValueFunc = createFunctionString(equalValue.getFunctionCallDescription(), equalValue.getPath());
                return new RangeNode(equalValue.getPath(), equalValue.getValue(), QueryOperator.EQ, equalValueFunc);
            case CONTAINS_VALUE_RANGE:
                ContainsValueRange containsValue = (ContainsValueRange) customQuery;
                String containsValueFunc = createFunctionString(containsValue.getFunctionCallDescription(), containsValue.getPath());
                return new RangeNode(containsValue.getPath(), containsValue.getValue(), getOperartorFromMatchCode(containsValue.getTemplateMatchCode()), containsValueFunc);
            case CONTAINS_ITEM_VALUE_RANGE:
                ContainsItemValueRange containsItemValueRange = (ContainsItemValueRange) customQuery;
                String containsItemValueFunc = createFunctionString(containsItemValueRange.getFunctionCallDescription(), containsItemValueRange.getPath());
                return new RangeNode(containsItemValueRange.getPath(), containsItemValueRange.getValue(), getOperartorFromMatchCode(containsItemValueRange.getTemplateMatchCode()), containsItemValueFunc);
            case IN_RANGE:
                InRange inRange = (InRange) customQuery;
                String inFunc = createFunctionString(inRange.getFunctionCallDescription(), inRange.getPath());
                return new RangeNode(inRange.getPath(), ((InSpacePredicate) inRange.getPredicate()).getInValues(), QueryOperator.IN, inFunc);
            case IS_NULL_RANGE:
                IsNullRange isNull = (IsNullRange) customQuery;
                String isNullFunc = createFunctionString(isNull.getFunctionCallDescription(), isNull.getPath());
                return new RangeNode(isNull.getPath(), null, QueryOperator.IS_NULL, isNullFunc);
            case NOT_NULL_RANGE:
                NotNullRange notNull = (NotNullRange) customQuery;
                String notNullFunc = createFunctionString(notNull.getFunctionCallDescription(), notNull.getPath());
                return new RangeNode(notNull.getPath(), null, QueryOperator.NOT_NULL, notNullFunc);
            case NOT_EQUAL_VALUE_RANGE:
                NotEqualValueRange notEqualValue = (NotEqualValueRange) customQuery;
                String notEqualValueFunc = createFunctionString(notEqualValue.getFunctionCallDescription(), notEqualValue.getPath());
                return new RangeNode(notEqualValue.getPath(), ((NotEqualsSpacePredicate) notEqualValue.getPredicate()).getExpectedValue(), QueryOperator.NE, notEqualValueFunc);
            case REGEX_RANGE:
                RegexRange regex = (RegexRange) customQuery;
                String regexFunc = createFunctionString(regex.getFunctionCallDescription(), regex.getPath());
                return new RangeNode(regex.getPath(), ((RegexSpacePredicate) regex.getPredicate()).getExpectedValue(), QueryOperator.REGEX, regexFunc);
            case NOT_REGEX_RANGE:
                NotRegexRange notRegex = (NotRegexRange) customQuery;
                String notRegexFunc = createFunctionString(notRegex.getFunctionCallDescription(), notRegex.getPath());
                return new RangeNode(notRegex.getPath(), ((NotRegexSpacePredicate) notRegex.getPredicate()).getExpectedValue(), QueryOperator.NOT_REGEX, notRegexFunc);
            default:
                return null;
        }
    }

    private static QueryOperator getOperartorFromMatchCode(short templateMatchCode) {
        switch (templateMatchCode) {
            case 0:
                return QueryOperator.EQ;
            case 1:
                return QueryOperator.NE;
            case 2:
                return QueryOperator.GT;
            case 3:
                return QueryOperator.GE;
            case 4:
                return QueryOperator.LT;
            case 5:
                return QueryOperator.LE;
            case 6:
                return QueryOperator.IS_NULL;
            case 7:
                return QueryOperator.NOT_NULL;
            case 8:
                return QueryOperator.REGEX;
            case 9:
                return QueryOperator.CONTAINS_TOKEN;
            case 10:
                return QueryOperator.NOT_REGEX;
            case 11:
                return QueryOperator.IN;
            case 12:
                return QueryOperator.RELATION;
            default:
                return QueryOperator.NOT_SUPPORTED;
        }
    }

    private static QueryOperator getSegmentOperator(SegmentRange segment) {
        if(segment.getMax() != null){
            if( segment.getMin() != null){
                return QueryOperator.BETWEEN;
            }
            else if(segment.isIncludeMax()){
                return QueryOperator.LE;
            }else {
                return QueryOperator.LT;
            }
        }else if(segment.getMin() != null){
            if(segment.isIncludeMin()){
                return QueryOperator.GE;
            }else {
                return QueryOperator.GT;
            }
        }
        return null;
    }

    private static QueryOperator getRelationOperator(String relation) {
        if ("INTERSECTS".equals(relation)) {
            return QueryOperator.INTERSECTS;
        } else {
            return QueryOperator.WITHIN;
        }
    }

    private static String createFunctionString(FunctionCallDescription functionCallDescription, String path) {
        if (functionCallDescription == null)
            return null;
        StringBuilder res = new StringBuilder(functionCallDescription.getName() + "(" + path + ",");
        int num = functionCallDescription.getNumberOfArguments();
        for (int i = 0; i < num; i++) {
            if (functionCallDescription.getArgument(i) != null) {
                res.append(functionCallDescription.getArgument(i) + ",");
            }
        }
        res.deleteCharAt(res.length() - 1);
        return res + ")";
    }


}
