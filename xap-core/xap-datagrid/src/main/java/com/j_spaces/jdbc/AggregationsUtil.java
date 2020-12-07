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

package com.j_spaces.jdbc;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.query.aggregators.*;
import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.builder.QueryEntryPacket;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.query.ArrayListResult;
import com.j_spaces.jdbc.query.IQueryResultSet;
import com.j_spaces.jdbc.query.ProjectedResultSet;

import net.jini.core.transaction.Transaction;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility to help perform aggregation  queries on space from JDBC
 *
 * @author anna
 * @since 10.0
 */
@com.gigaspaces.api.InternalApi
public class AggregationsUtil {

    /**
     * Create the aggregation set for the query
     *
     * @return AggregationSet
     */
    public static AggregationSet createAggregationSet(SelectQuery query,
                                                      int entriesLimit) {

        List<SelectColumn> selectColumns = query.getQueryColumns();
        List<SelectColumn> groupByColumns = query.getGroupColumn();
        List<OrderColumn> orderColumns = query.getOrderColumns();
        AggregationSet aggregationSet = new AggregationSet();

        // simple aggregations (max,count, etc)
        if (orderColumns == null && groupByColumns == null)
            return createFunctionsAggregationSet(selectColumns, aggregationSet);

        if (groupByColumns != null) {

            String[] groupByColumnNames = new String[groupByColumns.size()];
            for (int i = 0; i < groupByColumnNames.length; i++) {
                groupByColumnNames[i] = groupByColumns.get(i).getName();
            }
            if (query.isAggFunction()) {
                aggregationSet = createFunctionsAggregationSet(selectColumns, aggregationSet);

                GroupByAggregator groupByAggregator = new GroupByAggregator().groupBy(groupByColumnNames);

                List<SpaceEntriesAggregator> aggregators = AggregationInternalUtils
                        .getAggregators(aggregationSet);
                if (!aggregators.isEmpty()) {
                    groupByAggregator = groupByAggregator.select(aggregators.toArray(new SpaceEntriesAggregator[]{}));
                }

                return new AggregationSet().groupBy(groupByAggregator);
            } else {
                //group by as part of SQLQuery is basically just distinct
                //group by+order by is not supported yet, so can't fetch limited result (will return wrong result)
                int limit = orderColumns != null ? Integer.MAX_VALUE : entriesLimit;
                return new AggregationSet().distinct(new DistinctAggregator().distinct(limit
                        , groupByColumnNames));
            }
        } else if (orderColumns != null) {
            String[] orderByColumnNames = new String[orderColumns.size()];
            for (int i = 0; i < orderByColumnNames.length; i++) {
                orderByColumnNames[i] = orderColumns.get(i).getName();
            }

            OrderByAggregator orderByAggregator = new OrderByAggregator(entriesLimit);

            for (OrderColumn orderCol : orderColumns) {

                orderByAggregator = orderByAggregator.orderBy(orderCol.getName(), orderCol.isDesc() ? OrderBy.DESC : OrderBy.ASC, orderCol.areNullsLast());
            }

            aggregationSet = new AggregationSet().orderBy(orderByAggregator);
        }

        return aggregationSet;
    }

    private static AggregationSet createFunctionsAggregationSet(List<SelectColumn> selectColumns, AggregationSet aggregationSet) {
        for (SelectColumn funcColumn : selectColumns) {

            if (!funcColumn.isVisible())
                continue;

            //non aggregated fields are not supported yet
            if (funcColumn.getFunctionName() == null)
                aggregationSet = aggregationSet.add(new SingleValueAggregator().setPath(funcColumn.getName()));
            else if (funcColumn instanceof FunctionCallColumn){
                aggregationSet = aggregationSet.add(new SingleValueFunctionAggregator((FunctionCallColumn) funcColumn).setPath(funcColumn.getName()));
            }
            else if (funcColumn.getFunctionName().equals(SqlConstants.MAX)) {
                aggregationSet = aggregationSet.maxValue(funcColumn.getName());
            } else if (funcColumn.getFunctionName().equals(SqlConstants.MIN)) {
                aggregationSet = aggregationSet.minValue(funcColumn.getName());
            } else if (funcColumn.getFunctionName().equals(SqlConstants.COUNT)) {
                //TBD fix semantics for path
                aggregationSet = aggregationSet.count(funcColumn.getName());
            } else if (funcColumn.getFunctionName().equals(SqlConstants.SUM)) {
                if (funcColumn instanceof SumColumn) {
                    aggregationSet.add(new CompositeSumAggregator(((SumColumn) funcColumn)));
                } else {
                    aggregationSet = aggregationSet.sum(funcColumn.getName());
                }
            } else if (funcColumn.getFunctionName().equals(SqlConstants.AVG)) {
                aggregationSet = aggregationSet.average(funcColumn.getName());
            }
        }
        return aggregationSet;
    }

    /**
     * Execute aggregation query
     *
     * @return IQueryResultSet
     */
    public static IQueryResultSet<IEntryPacket> aggregate(QueryTemplatePacket template,
                                                          AggregationSet aggregationSet, IJSpace space, Transaction txn,
                                                          int modifiers) throws SQLException {

        AggregationResult aggregateResult = null;
        try {
            aggregateResult = ((ISpaceProxy) space).aggregate(template,
                    aggregationSet, txn, modifiers);

            return convertAggregationResult(aggregationSet, aggregateResult);
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }

    }

    private static IQueryResultSet<IEntryPacket> convertAggregationResult(AggregationSet aggregationSet,
                                                                          AggregationResult aggregateResult) {

        ArrayList<String> names = new ArrayList<String>();
        List<SpaceEntriesAggregator> aggregators = AggregationInternalUtils.getAggregators(aggregationSet);

        IQueryResultSet<IEntryPacket> entries;

        if (aggregators.get(0) instanceof GroupByAggregator) {
            entries = new ProjectedResultSet();
            List<SpaceEntriesAggregator> groupByAggregators = AggregationInternalUtils.getSelectors((GroupByAggregator) aggregators.get(0));
            for (int i = 0; i < groupByAggregators.size(); i++) {

                names.add(groupByAggregators.get(i).getDefaultAlias());
            }
            GroupByResult groupByResult = (GroupByResult) aggregateResult.get(0);

            for (GroupByValue groupByValue : groupByResult) {
                Object[] singleEntryArr = new Object[names.size()];

                for (int i = 0; i < names.size(); i++) {

                    singleEntryArr[i] = groupByValue.get(i);
                }

                QueryEntryPacket singleGroupBy = new QueryEntryPacket(
                        names.toArray(new String[names.size()]),
                        singleEntryArr);

                entries.add(singleGroupBy);
            }
        } else if (aggregators.get(0) instanceof OrderByAggregator) {
            entries = new ArrayListResult();
            List result = (List) aggregateResult.get(0);

            if (result != null) {
                for (Object entry : result) {

                    entries.add((IEntryPacket) entry);
                }
            }
        } else if (aggregators.get(0) instanceof DistinctAggregator) {
            entries = new ArrayListResult();

            List result = (List) aggregateResult.get(0);

            if (result != null) {
                for (Object entry : result) {

                    entries.add((IEntryPacket) entry);
                }
            }
        } else {
            for (int i = 0; i < aggregators.size(); i++) {

                names.add(aggregators.get(i).getDefaultAlias());
            }
            ArrayList<Object> values = new ArrayList<Object>();

            for (int i = 0; i < aggregateResult.size(); i++) {

                Object aggregationResult = aggregateResult.get(i);
                values.add(aggregationResult);
            }


            QueryEntryPacket aggregation = new QueryEntryPacket(
                    names.toArray(new String[names.size()]),
                    values.toArray(new Object[values.size()]));

            entries = new ProjectedResultSet();

            entries.add(aggregation);
        }
        return entries;
    }
}
