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

package com.j_spaces.jdbc.executor;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.security.AccessDeniedException;
import com.j_spaces.core.ExternalEntryPacket;
import com.j_spaces.jdbc.*;
import com.j_spaces.jdbc.builder.QueryEntryPacket;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.parser.ExpNode;
import com.j_spaces.jdbc.parser.InnerQueryNode;
import com.j_spaces.jdbc.query.ArrayListResult;
import com.j_spaces.jdbc.query.IQueryResultSet;
import com.j_spaces.jdbc.query.ProjectedResultSet;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author anna
 * @since 7.0
 */
public abstract class AbstractQueryExecutor implements IQueryExecutor {
    private static final long serialVersionUID = 1L;

    /**
     * This private class implements the Comparator and is used to sort. the entries when ORDER BY
     * is used in the query
     */
    protected static class EntriesOrderByComparator implements Comparator<IEntryPacket> {

        private List<OrderColumn> _orderColumns;
        private IQueryResultSet<IEntryPacket> _queryResult;

        public EntriesOrderByComparator(IQueryResultSet<IEntryPacket> queryResult, List<OrderColumn> orderColumns) {
            _orderColumns = orderColumns;
            _queryResult = queryResult;
        }

        public int compare(IEntryPacket o1, IEntryPacket o2) {
            int rc = 0;


            for (int i = 0; i < _orderColumns.size(); i++) {
                OrderColumn orderCol = _orderColumns.get(i);

                Comparable c1 = (Comparable) _queryResult.getFieldValue(orderCol, o1);
                Comparable c2 = (Comparable) _queryResult.getFieldValue(orderCol, o2);

                if (c1 == c2)
                    continue;

                if (c1 == null)
                    return -1;

                if (c2 == null)
                    return 1;

                rc = c1.compareTo(c2);
                if (rc != 0)
                    return orderCol.isDesc() ? -rc : rc;

            }

            return rc;
        }
    }

    protected final static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_QUERY);

    protected AbstractDMLQuery query;
    private HashMap<ExpNode, IQueryResultSet<IEntryPacket>> _intermediateResults = new HashMap<ExpNode, IQueryResultSet<IEntryPacket>>();

    // Required for Externalizable
    public AbstractQueryExecutor() {
    }

    public AbstractQueryExecutor(AbstractDMLQuery query) {
        super();
        this.query = query;
    }

    public int clear(QueryTemplatePacket template, ISpaceProxy space, Transaction txn, int modifiers)
            throws RemoteException, TransactionException, UnusableEntryException {
        return space.clear(template, txn, modifiers);
    }

    public int count(QueryTemplatePacket template, ISpaceProxy space, Transaction txn, int readModifier)
            throws SQLException {
        try {
            return space.count(template, txn, readModifier);
        } catch (Exception e) {
            if (_logger.isErrorEnabled())
                _logger.error(e.getMessage(), e);
            throw new SQLException("Failed to execute count: " + e.getMessage(), "GSP", -111);
        }
    }

    public AbstractDMLQuery getQuery() {
        return query;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.executor.IQueryExecutor#extractResults(com.j_spaces.jdbc.parser.ExpNode)
     */
    public IQueryResultSet<IEntryPacket> extractResults(ExpNode node) {
        return _intermediateResults.remove(node);

    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.executor.IQueryExecutor#setResults(com.j_spaces.jdbc.parser.ExpNode, com.j_spaces.jdbc.IQueryResultSet<IEntryPacket>)
     */
    public void setResults(ExpNode node, IQueryResultSet<IEntryPacket> results) {
        _intermediateResults.put(node, results);
    }

    /**
     * Traverse the binary expression tree non-recursively using a custom stack The tree has to be
     * traversed in postorder - the parent is traversed after its children.
     */
    protected IQueryResultSet<IEntryPacket> traverseExpressionTree(ExpNode root, ISpaceProxy space, Transaction txn, int readModifier, int max) throws SQLException {

        try {
            Stack<ExpNode> tempStack = new Stack<ExpNode>();
            Stack<ExpNode> nodesToProcessStack = new Stack<ExpNode>();

            tempStack.push(root);
            while (!tempStack.isEmpty()) {

                ExpNode curr = tempStack.pop();
                nodesToProcessStack.push(curr);

                // Don't traverse nodes that are already aggregated at the parent level.
                if (curr.getTemplate() == null) {
                    if (curr.getLeftChild() != null)
                        tempStack.push(curr.getLeftChild());
                    if (curr.getRightChild() != null)
                        tempStack.push(curr.getRightChild());
                }
            }

            while (!nodesToProcessStack.isEmpty()) {
                ExpNode node = nodesToProcessStack.pop();

                // special handling for the root 
                // if the root is executed it can optimize the query by limiting the result set
                // this can't be done on intermediate results
                if (nodesToProcessStack.isEmpty())
                    node.accept(this, space, txn, readModifier, max);
                else
                    node.accept(this,
                            space,
                            txn,
                            readModifier,
                            Integer.MAX_VALUE);
            }

            return extractResults(root);
        } finally {
            _intermediateResults.clear();
        }

    }

    public IQueryResultSet readAll(ISpaceProxy space, Transaction txn) throws SQLException {
        return executeTemplate(new QueryTemplatePacket(query.getTableData(), query.getQueryResultType()), space, txn, query.getReadModifier(), Integer.MAX_VALUE);

    }

    public IQueryResultSet<IEntryPacket> executeTemplate(QueryTemplatePacket template, ISpaceProxy space, Transaction txn,
                                                         int readModifier, int max) throws SQLException {

        try {
            // empty set
            if (template == null)
                return new ArrayListResult();
            template.setExplainPlan(getQuery().getExplainPlan());
            return template.read(space, query, txn, readModifier, max);
        } catch (AccessDeniedException e) {
            throw e;
        } catch (Exception e) {
            if (_logger.isDebugEnabled()) {
                _logger.error(e.getMessage(), e);
            }

            SQLException t = new SQLException("Failed to execute readMultiple: "
                    + e.toString(), "GSP", -111);
            t.initCause(e);
            throw t;
        }

    }

    protected HashMap<ExpNode, IQueryResultSet<IEntryPacket>> getIntermediateResults() {
        return _intermediateResults;
    }

    /**
     * Execute SQL function that processes the result entries and calculates one value -
     * sum,average,min,max
     *
     * @return The result of sum,average,min,max
     */
    public IEntryPacket aggregate(IQueryResultSet<IEntryPacket> entries) throws SQLException {

        Iterator iter = query.getQueryColumns().iterator();
        ArrayList<String> vecFieldNames = new ArrayList<String>();
        ArrayList<Object> vecFieldValues = new ArrayList<Object>();

        while (iter.hasNext()) {
            SelectColumn funcColumn = (SelectColumn) iter.next();

            if (!funcColumn.isVisible())
                continue;

            vecFieldNames.add(funcColumn.toString());

            // Handle non-aggregated columns like group by
            if (funcColumn.getFunctionName() == null || funcColumn instanceof FunctionCallColumn) {
                IEntryPacket first = entries.iterator().next();
                Object value = entries.getFieldValue(funcColumn, first);

                vecFieldValues.add(value);

            } else if (funcColumn.getFunctionName().equals(SqlConstants.MAX)) {
                Object maxMin = minMax(funcColumn, entries, true);


                vecFieldValues.add(maxMin);
            } else if (funcColumn.getFunctionName().equals(SqlConstants.MIN)) {
                Object maxMin = minMax(funcColumn, entries, false);

                vecFieldValues.add(maxMin);
            } else if (funcColumn.getFunctionName().equals(SqlConstants.COUNT)) {

                vecFieldValues.add(entries.size());
            } else if (funcColumn.getFunctionName().equals(SqlConstants.SUM)) {
                Object sum = sum(funcColumn, entries);


                vecFieldValues.add(sum);
            } else if (funcColumn.getFunctionName().equals(SqlConstants.AVG)) {
                Number avg = avg(funcColumn, entries);


                vecFieldValues.add(avg);
            }
        }

        return new QueryEntryPacket(
                vecFieldNames.toArray(new String[vecFieldNames.size()]),
                vecFieldValues.toArray(new Object[vecFieldValues.size()]));
    }


    /**
     * Group the results according to the group by clause
     *
     * @return IQuery result set
     */
    public IQueryResultSet<IEntryPacket> groupByAndKeepOrder(IQueryResultSet<IEntryPacket> entries, List<SelectColumn> groupColumns, List<OrderColumn> orderColumns, int limit) throws SQLException {
        IQueryResultSet<IEntryPacket> currGroup = null;
        IEntryPacket currRow = null, prevRow = null;
        int rc;

        Comparator comparator = getGroupByComparator(entries, groupColumns);
        Collections.sort((List<IEntryPacket>) entries, comparator);

        Iterator<IEntryPacket> iter = entries.iterator();
        List<IQueryResultSet<IEntryPacket>> groupList = new ArrayList<IQueryResultSet<IEntryPacket>>();
        for (int i = 0; i < entries.size(); i++) {
            prevRow = currRow;
            currRow = iter.next();
            rc = comparator.compare(prevRow, currRow);

            if (rc != 0) {
                currGroup = entries.newResultSet();
                groupList.add(currGroup);
                currGroup.add(currRow);
            } else {
                currGroup.add(currRow);
            }
        }

        groupList.sort((o1, o2) -> {
            int rc1 = 0;
            IEntryPacket oo1 = o1.iterator().next();
            IEntryPacket oo2 = o2.iterator().next();

            for (int i = 0; i < orderColumns.size(); i++) {
                OrderColumn orderCol = orderColumns.get(i);

                Comparable c1 = (Comparable) orderCol.getFieldValue(oo1);
                Comparable c2 = (Comparable) orderCol.getFieldValue(oo2);

                if (c1 == c2)
                    continue;

                if (c1 == null)
                    return -1;

                if (c2 == null)
                    return 1;

                rc1 = c1.compareTo(c2);
                if (rc1 != 0)
                    return orderCol.isDesc() ? -rc1 : rc1;

            }

            return rc1;
        });

        groupList = groupList.subList(0, Math.min(groupList.size(), limit));

        IQueryResultSet<IEntryPacket> groupByResult = query.isConvertResultToArray() ? new ProjectedResultSet() : new ArrayListResult();
        for (Iterator<IQueryResultSet<IEntryPacket>> iterator = groupList.iterator(); iterator.hasNext(); ) {
            IQueryResultSet<IEntryPacket> group = iterator.next();
            groupByResult.addAll(group);
        }
        return groupByResult;

    }

    public IQueryResultSet<IEntryPacket> groupBy(IQueryResultSet<IEntryPacket> entries, List<SelectColumn> groupColumns) throws SQLException {
        IQueryResultSet<IEntryPacket> currGroup = null;
        IEntryPacket currRow = null, prevRow = null;
        int rc;

        Comparator comparator = getGroupByComparator(entries, groupColumns);
        Collections.sort((List<IEntryPacket>) entries, comparator);

        Iterator<IEntryPacket> iter = entries.iterator();
        ArrayList<IQueryResultSet<IEntryPacket>> groupList = new ArrayList<IQueryResultSet<IEntryPacket>>();
        for (int i = 0; i < entries.size(); i++) {
            prevRow = currRow;
            currRow = iter.next();
            rc = comparator.compare(prevRow, currRow);

            if (rc != 0) {
                currGroup = entries.newResultSet();
                groupList.add(currGroup);
                currGroup.add(currRow);
            } else {
                currGroup.add(currRow);
            }
        }


        IQueryResultSet<IEntryPacket> groupByResult = query.isConvertResultToArray() ? new ProjectedResultSet() : new ArrayListResult();

        for (Iterator<IQueryResultSet<IEntryPacket>> iterator = groupList.iterator(); iterator.hasNext(); ) {

            IQueryResultSet<IEntryPacket> group = iterator.next();
            //Handle aggregation
            if (query.isJoined() || query.isConvertResultToArray())
                groupByResult.add(aggregate(group));
            else
                groupByResult.add(group.iterator().next());
        }

        return groupByResult;

    }


    /**
     * This private class implements the Comparator and is used to sort the arrays of values when
     * GROUP BY is used in the query.
     */
    static class GroupByComparator implements Comparator<IEntryPacket> {
        private List<SelectColumn> groupColumns;

        private IQueryResultSet<IEntryPacket> _queryResult;

        GroupByComparator(List<SelectColumn> groupCols, IQueryResultSet<IEntryPacket> queryResult) {
            groupColumns = groupCols;
            _queryResult = queryResult;

        }

        /**
         * Compare two arrays of values - only the group by columns are compared
         */
        public int compare(IEntryPacket e1, IEntryPacket e2) {

            if (e1 == null) {
                if (e2 == null)
                    return 0;
                return -1;
            }

            if (e2 == null)
                return 1;

            int rc = 0;


            for (int i = 0; i < groupColumns.size(); i++) {
                SelectColumn groupCol = groupColumns.get(i);

                Object obj1 = _queryResult.getFieldValue(groupCol, e1);
                Object obj2 = _queryResult.getFieldValue(groupCol, e2);

                if (obj1 == null && obj2 == null)
                    rc = 0;
                else if (obj1 == null && obj2 != null)
                    rc = -1;
                else if (obj1 != null && obj2 == null)
                    rc = 1;
                else
                    rc = ((Comparable) obj1).compareTo(obj2);

                if (rc != 0)
                    return rc;
            }

            return rc;
        }
    }

    public Comparator<IEntryPacket> getGroupByComparator(IQueryResultSet<IEntryPacket> queryResult, List<SelectColumn> groupColumns) {
        return new GroupByComparator(groupColumns, queryResult);
    }

    /**
     * Order the results according to the order by clause
     */
    public void orderBy(IQueryResultSet<IEntryPacket> entries, List<OrderColumn> orderColumns) throws SQLException {
        Collections.sort((List<IEntryPacket>) entries, getOrderByComparator(entries, orderColumns));

    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.executor.AbstractQueryExecutor#getOrderByComparator(java.util.List)
     */
    public Comparator<IEntryPacket> getOrderByComparator(IQueryResultSet<IEntryPacket> entries, List<OrderColumn> orderColumns) {
        return new EntriesOrderByComparator(entries, orderColumns);
    }

    /**
     * Finds the minimum/maximum value of given column
     *
     * @return minimum/maximum value of given column
     */
    public Object minMax(
            SelectColumn funcColumn, IQueryResultSet<IEntryPacket> entries,
            boolean isMax) {
        Object value, maxMin = null;

        Iterator<IEntryPacket> iter = entries.iterator();
        while (iter.hasNext()) {
            IEntryPacket entry = iter.next();

            value = entries.getFieldValue(funcColumn, entry);

            if (value == null)
                continue;

            if (maxMin != null)
                if (isMax)
                    maxMin = (((Comparable) value).compareTo(maxMin) > 0) ? value : maxMin;
                else
                    maxMin = (((Comparable) value).compareTo(maxMin) < 0) ? value : maxMin;
            else
                maxMin = value;
        }

        return maxMin;
    }

    /**
     * Calculate the average value of the given column for all entries . Average is calculated using
     * Double precision
     *
     * @return the sum value of the field
     */
    public Number avg(SelectColumn funcColumn, IQueryResultSet<IEntryPacket> entries) throws SQLException {

        Iterator<IEntryPacket> iter = entries.iterator();
        IEntryPacket entry = iter.next();

        Number sum = getNumber(entry, funcColumn, entries);
        String numberClassName = sum.getClass().getName();
        sum = sum.doubleValue();
        String doubleType = Double.class.getName();

        double size = entries.size();

        while (iter.hasNext()) {
            entry = iter.next();

            Number value = getNumber(entry, funcColumn, entries);

            sum = NumberUtil.add(sum, value.doubleValue(), doubleType);
        }

        Number avg = NumberUtil.divide(sum, size, numberClassName);

        return avg;

    }

    /**
     * Calculate the sum value of the given column for all entries
     *
     * @return the sum value of the field
     */
    public Number sum(SelectColumn funcColumn, IQueryResultSet<IEntryPacket> entries) throws SQLException {
        if (entries.isEmpty())
            return null;

        Iterator<IEntryPacket> iter = entries.iterator();
        IEntryPacket entry = iter.next();

        Number sum = getNumber(entry, funcColumn, entries);

        while (iter.hasNext()) {
            entry = iter.next();
            sum = NumberUtil.add(sum, getNumber(entry, funcColumn, entries), sum.getClass().getName());
        }

        return sum;

    }


    /**
     * Extract the value from given entry field
     *
     * @return value from given entry field
     */
    private Number getNumber(IEntryPacket entry, SelectColumn funcColumn, IQueryResultSet<IEntryPacket> entries) {
        Object value = entries.getFieldValue(funcColumn, entry);

        if (value == null)
            value = 0;

        return (Number) value;
    }


    /**
     * Removes duplicate entries from the result set. The rows are inserted into a TreeSet and only
     * the rows that were successfully added, are considered distinct. Note: TreeSet is used and not
     * HashSet to avoid creating hashCode for all fields.
     * @return new IQueryResultSet with distinct entries
     */
    public IQueryResultSet<IEntryPacket> filterDistinctEntries(final IQueryResultSet<IEntryPacket> entries) {

        // First define the comparator for distinct
        TreeSet<IEntryPacket> treeSet = new TreeSet<IEntryPacket>(new Comparator<IEntryPacket>() {
            public int compare(IEntryPacket o1, IEntryPacket o2) {

                for (SelectColumn column : query.getQueryColumns()) {

                    if (!column.isVisible())
                        continue;

                    Comparable c1 = (Comparable) entries.getFieldValue(column, o1);
                    Comparable c2 = (Comparable) entries.getFieldValue(column, o2);

                    if (c1 == null && c2 == null)
                        continue;

                    if (c1 == null)
                        return 1;

                    if (c2 == null)
                        return -1;

                    int c = c1.compareTo(c2);

                    if (c != 0)
                        return c;
                }

                return 0;
            }
        });

        //Add entries to the treeSet. The treeSet will contain distinct entries
        treeSet.addAll(entries);

        //If all unique, return original list
        if (treeSet.size() == entries.size()) {
            return entries;
        }

        //Convert the treeSet to a new IQueryResultSet
        return entries.newResultSet(treeSet);
    }

    /**
     * Converts the list of ExternaEntries to 2 dimensional array that contains the values of the
     * entries. Each row matches specific IEntryPacket. The conversion is necessary for entries
     * ordering.
     */
    public ResultEntry convertEntriesToResultArrays(IQueryResultSet<IEntryPacket> entries) {
        // Column (field) names and labels (aliases)
        LinkedList<String> columnNames = new LinkedList<String>();
        LinkedList<String> columnLabelsList = new LinkedList<String>();
        LinkedList<String> tableNamesList = new LinkedList<String>();

        for (SelectColumn col : query.getQueryColumns()) {
            // Only add for visible columns
            if (col.isVisible()) {
                columnNames.add(col.getName());
                columnLabelsList.add(col.getAlias());
                tableNamesList.add(col instanceof ValueSelectColumn ? "anonymous" : col.getColumnTableData().getTableName());
            }
        }

        String[] fieldNames = columnNames.toArray(new String[columnNames.size()]);
        String[] columnLabels = columnLabelsList.toArray(new String[columnLabelsList.size()]);
        String[] tableNames = tableNamesList.toArray(new String[tableNamesList.size()]);

        //the field values for the result
        Object[][] fieldValues = new Object[entries.size()][columnNames.size()];

        Iterator<IEntryPacket> iter = entries.iterator();

        int row = 0;

        while (iter.hasNext()) {
            IEntryPacket entry = iter.next();

            int column = 0;
            for (int i = 0; i < query.getQueryColumns().size(); i++) {
                SelectColumn sc = query.getQueryColumns().get(i);

                if (!sc.isVisible())
                    continue;

                fieldValues[row][column++] = entries.getFieldValue(sc, entry);
            }

            row++;
        }

        ResultEntry result = new ResultEntry(
                fieldNames,
                columnLabels,
                tableNames,
                fieldValues);

        return result;
    }

    @Override
    public IQueryResultSet<IEntryPacket> flattenEntryPackets(IQueryResultSet<IEntryPacket> entries) {
        // Column (field) names and labels (aliases)
        LinkedList<String> columnNames = new LinkedList<String>();

        for (SelectColumn col : query.getQueryColumns()) {
            // Only add for visible columns
            if (col.isVisible()) {
                columnNames.add(col.getAlias());
            }
        }

        String[] fieldNames = columnNames.toArray(new String[columnNames.size()]);

        Iterator<IEntryPacket> iter = entries.iterator();
        IQueryResultSet<IEntryPacket> result = new ArrayListResult();

        while (iter.hasNext()) {
            IEntryPacket entry = iter.next();
            int column = 0;
            Object[] fields = new Object[fieldNames.length];
            for (int i = 0; i < query.getQueryColumns().size(); i++) {
                SelectColumn sc = query.getQueryColumns().get(i);

                if (!sc.isVisible())
                    continue;

                fields[column++] = entry.getFieldValue(sc.getProjectedIndex());
            }
            IEntryPacket ep = new QueryEntryPacket(fieldNames, fields);
//            ep.setFieldsValues(fields);
            result.add(ep);
        }

        return result;
    }

    /**
     * Executes an inner query
     */
    public void execute(InnerQueryNode innerQueryNode, ISpaceProxy space, Transaction txn,
                        int readModifier, int max) throws SQLException {

        SelectQuery innerQuery = innerQueryNode.getInnerQuery();
        // Execute inner query
        innerQuery.validateQuery(space);
        innerQuery.setPreparedValues(query.getPreparedValues());
        innerQuery.setRouting(query.getRouting());
        if (!innerQuery.isPrepared() && !innerQuery.containsSubQueries())
            innerQuery.build();
        ResponsePacket innerResponse = innerQuery.executeOnSpace(space, txn);

        // Get the values from the inner query response
        innerQueryNode.setResults(innerResponse.getResultEntry());
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, query);
        IOUtils.writeObject(out, _intermediateResults);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        query = IOUtils.readObject(in);
        _intermediateResults = IOUtils.readObject(in);
    }
}
