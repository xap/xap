package com.gigaspaces.jdbc.calcite;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.DateRangeRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GSOptimizerRules {

    private GSOptimizerRules() {
        // No-op
    }

    private static final List<RelOptRule> ABSTRACT_RULES = Arrays.asList(
        CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS,
        CoreRules.UNION_PULL_UP_CONSTANTS,
        PruneEmptyRules.UNION_INSTANCE,
        PruneEmptyRules.INTERSECT_INSTANCE,
        PruneEmptyRules.MINUS_INSTANCE,
        PruneEmptyRules.PROJECT_INSTANCE,
        PruneEmptyRules.FILTER_INSTANCE,
        PruneEmptyRules.SORT_INSTANCE,
        PruneEmptyRules.AGGREGATE_INSTANCE,
        PruneEmptyRules.JOIN_LEFT_INSTANCE,
        PruneEmptyRules.JOIN_RIGHT_INSTANCE,
        PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE,
        CoreRules.UNION_MERGE,
        CoreRules.INTERSECT_MERGE,
        CoreRules.MINUS_MERGE,
        CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW,
        CoreRules.FILTER_MERGE,
        DateRangeRules.FILTER_INSTANCE,
        CoreRules.INTERSECT_TO_DISTINCT
    );

    private static final List<RelOptRule> ABSTRACT_RELATIONAL_RULES = Arrays.asList(
        CoreRules.FILTER_INTO_JOIN,
        CoreRules.JOIN_CONDITION_PUSH,
        AbstractConverter.ExpandConversionRule.INSTANCE,
        CoreRules.JOIN_COMMUTE,
        CoreRules.PROJECT_TO_SEMI_JOIN,
        CoreRules.JOIN_TO_SEMI_JOIN,
        CoreRules.AGGREGATE_REMOVE,
        CoreRules.UNION_TO_DISTINCT,
        CoreRules.PROJECT_REMOVE,
        CoreRules.PROJECT_AGGREGATE_MERGE,
        CoreRules.AGGREGATE_JOIN_TRANSPOSE,
        CoreRules.AGGREGATE_MERGE,
        CoreRules.AGGREGATE_PROJECT_MERGE,
        CoreRules.CALC_REMOVE,
        CoreRules.SORT_REMOVE
    );

    private static final List<RelOptRule> BASE_RULES = Arrays.asList(
        CoreRules.AGGREGATE_STAR_TABLE,
        CoreRules.AGGREGATE_PROJECT_STAR_TABLE,
        CoreRules.AGGREGATE_UNION_TRANSPOSE,
        CoreRules.PROJECT_SET_OP_TRANSPOSE,
        CoreRules.PROJECT_MERGE,
        CoreRules.FILTER_SCAN,
        CoreRules.PROJECT_FILTER_TRANSPOSE,
        CoreRules.FILTER_PROJECT_TRANSPOSE,
        CoreRules.FILTER_INTO_JOIN,
        CoreRules.JOIN_PUSH_EXPRESSIONS,
        CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
        CoreRules.AGGREGATE_CASE_TO_FILTER,
        CoreRules.AGGREGATE_REDUCE_FUNCTIONS,
        CoreRules.FILTER_AGGREGATE_TRANSPOSE,
        CoreRules.PROJECT_WINDOW_TRANSPOSE,
        CoreRules.MATCH,
        CoreRules.JOIN_COMMUTE,
        JoinPushThroughJoinRule.RIGHT,
        JoinPushThroughJoinRule.LEFT,
        CoreRules.SORT_PROJECT_TRANSPOSE,
        CoreRules.SORT_JOIN_TRANSPOSE,
        CoreRules.SORT_REMOVE_CONSTANT_KEYS,
        CoreRules.SORT_UNION_TRANSPOSE,
        CoreRules.EXCHANGE_REMOVE_CONSTANT_KEYS,
        CoreRules.SORT_EXCHANGE_REMOVE_CONSTANT_KEYS
    );

    private static final List<RelOptRule> GS_RULES = Arrays.asList(
        GSTableScanRule.INSTANCE,
        GSFilterRule.INSTANCE,
        GSProjectRule.INSTANCE,
        GSJoinRule.INSTANCE,
        GSAggregateRule.INSTANCE
        //,GSValuesRule.INSTANCE //commented out - not need for adding ValuesRule now
    );

    public static final List<RelOptRule> GS_CALC_RULES = Arrays.asList(
        GSProjectToCalcRule.INSTANCE,
        GSFilterToCalcRule.INSTANCE,
        CoreRules.PROJECT_CALC_MERGE,
        CoreRules.FILTER_CALC_MERGE,
        CoreRules.CALC_MERGE
    );

    public static List<RelOptRule> rules() {
        List<RelOptRule> res = new ArrayList<>();

        res.addAll(ABSTRACT_RULES);
        res.addAll(ABSTRACT_RELATIONAL_RULES);
        res.addAll(BASE_RULES);
        res.addAll(GS_RULES);

        return res;
    }
}
