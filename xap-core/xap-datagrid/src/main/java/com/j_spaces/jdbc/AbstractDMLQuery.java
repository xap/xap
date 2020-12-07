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

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.metadata.ObjectType;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.transport.AbstractProjectionTemplate;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.utils.ObjectUtils;
import com.gigaspaces.query.explainplan.ExplainPlan;
import com.gigaspaces.security.service.SecurityInterceptor;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.client.ReadModifiers;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.jdbc.batching.BatchResponsePacket;
import com.j_spaces.jdbc.builder.QueryTemplateBuilder;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.driver.GPreparedStatement.PreparedValuesCollection;
import com.j_spaces.jdbc.executor.IQueryExecutor;
import com.j_spaces.jdbc.executor.QueryExecutor;
import com.j_spaces.jdbc.parser.ExpNode;
import com.j_spaces.jdbc.parser.InnerQueryNode;
import com.j_spaces.jdbc.parser.RowNumNode;
import com.j_spaces.jdbc.query.QueryTableData;

import net.jini.core.transaction.Transaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author Michael Mitrani, 2Train4, 2004
 *
 *         This abstract class is the super class for DML queries like: SELECT, DELETE, UPDATE and
 *         INSERT
 */
public abstract class AbstractDMLQuery implements Query, Cloneable {

    protected boolean isPrepared;
    protected List queryColumns = null;  //list of columns of the query.
    protected ExpNode expTree = null;   //root Node of the expression tree
    protected Object[] preparedValues = null;
    //of the tables
    protected List<QueryTableData> _tablesData = Collections.synchronizedList(new ArrayList<QueryTableData>());
    private QuerySession session = null;
    protected RowNumNode rownum = null;
    protected TreeMap<String, Object> valueMap = null;
    private boolean m_isUseTemplate = false;

    protected ConcurrentHashMap<String, QueryTableData> tables = new ConcurrentHashMap<String, QueryTableData>(); //keeps a mapping between an alias and a table
    //if there was no alias, it will be like the table name

    // Indicates whether the query contains a "field[*] = ?" syntax
    // which is currently unsupported for read/take operations.
    protected transient boolean _containsQuery = false;

    // If set to true - the generated external template
    // will not be executed - just converted to external entry
    protected boolean _buildOnly = false;

    // If set to true - query result is converted to
    // an array of values
    protected boolean _convertResultToArray = true;

    // Template builder
    private QueryTemplateBuilder _builder = new QueryTemplateBuilder(this);

    private int _readModifier = ReadModifiers.REPEATABLE_READ;
    private long _timeout;
    private boolean _ifExists;
    private Object _routing;
    private boolean _dirtyState = false;
    protected IQueryExecutor _executor;
    private int _minEntriesToWaitFor;


    // flag that indicates whether query result should be returned to the client
    // used in delete
    private boolean _returnResult = true;

    // operation id of this query - assigned by the proxy
    protected OperationID _operationID;

    protected QueryResultTypeInternal _queryResultType = QueryResultTypeInternal.NOT_SET;
    protected SecurityInterceptor securityInterceptor;
    private boolean _containsSubQueries;
    protected AbstractProjectionTemplate _projectionTemplate;

    private ExplainPlan _explainPlan;

    /**
     * Build  query internal structures - called after parsing
     */
    public void build() throws SQLException {
        // Build subqueries recursivly, if any.
        for (QueryTableData tableData : getTablesData()) {
            Query subQuery = tableData.getSubQuery();
            if (subQuery != null && !subQuery.containsSubQueries())
                subQuery.build();
        }
        buildTemplates();
    }

    protected void validateBuiltTemplates() throws SQLException {
        //complex queries that are broken into several space queries are not supported by some operations
        if (expTree != null && (expTree.getTemplate() == null || expTree.getTemplate().isComplex())) {
            // validate build only queries(snapshot/notify)
            // Handle composite queries
            if (isBuildOnly())
                throw new SQLException("Operation doesn't support complex SQL queries that can't be translated to a single template.");

            // validate queries with timeout
            if (getTimeout() > 0)
                throw new SQLException("Operation with timeout doesn't support complex SQL queries that can't be translated to a single template.");

            if (Modifiers.contains(getReadModifier(), Modifiers.FIFO_GROUPING_POLL)) {
                throw new SQLException("Fifo-Groups Operation doesn't support complex SQL queries that can't be translated to a single template.");
            }
        }
    }

    public boolean isContainsQuery() {
        return _containsQuery;
    }

    public void setContainsQuery(boolean containsQuery) {
        _containsQuery = containsQuery;
    }

    /**
     * @return the readModifier
     */
    public int getReadModifier() {
        return _readModifier;
    }

    /**
     * @param readModifier the readModifier to set
     */
    public void setReadModifier(int readModifier) {
        _readModifier = readModifier;
    }


    /**
     * @param isPrepared is this a PreparedStatment or not.
     */
    public void setPrepared(boolean isPrepared) {
        this.isPrepared = isPrepared;
    }

    /**
     * @return isPrepared is this a PreparedStatment or not.
     */
    public boolean isPrepared() {
        return isPrepared;
    }

    /**
     * Returns the list of columns in this query as Strings. if the list is empty it means all
     * columns (the '*' notation).
     */
    public List<SelectColumn> getQueryColumns() {
        return queryColumns;
    }

    public SelectColumn getQueryColumnByAlias(String alias) {
        for (SelectColumn sc : getQueryColumns()) {
            if (sc.hasAlias() && sc.getAlias().equalsIgnoreCase(alias)) {
                return sc;
            }
        }
        return null;
    }


    /**
     * Add a column to the list of columns.
     *
     * @param column the column to add
     */
    public void addColumn(String column) {
        if (queryColumns == null)
            queryColumns = new ArrayList();

        queryColumns.add(column);
    }


    /**
     * sets the table name of the query.
     */
    public void setTableName(String table) {
        tables.clear();
        _tablesData.clear();
        addTableWithAlias(table, null);
    }

    /**
     * @return String the table name of the query
     */
    public String getTableName() {
        return _tablesData.get(0).getTableName();
    }

    /**
     * Sets the root node of the expression tree.
     */
    public void setExpTree(ExpNode rootNode) {
        expTree = rootNode;
    }

    /**
     * @return the root node of the expression tree.
     */
    public ExpNode getExpTree() {
        return expTree;
    }


    public boolean isJoined() {
        return _tablesData.size() > 1;
    }

    @Override
    public abstract AbstractDMLQuery clone();

  public void setRownum(RowNumNode rownum) {
        this.rownum = rownum;
    }

    public RowNumNode getRownum() {
        return rownum;
    }

    public Object[] getPreparedValues() {
        return preparedValues;
    }

    public void setPreparedValues(Object[] preparedValues) {
        this.preparedValues = preparedValues;
    }

    public QuerySession getSession() {
        return session;
    }

    public void setSession(QuerySession session) {
        this.session = session;

        if (session.getModifiers() != null) {
            setReadModifier(session.getModifiers());
        }
    }


    public void setTemplatePreparedValues(ITypeDesc typeDesc, Object[] fieldValues) {
        if (fieldValues != null) {
            valueMap = new TreeMap<String, Object>();
            this.preparedValues = new Object[fieldValues.length];
            m_isUseTemplate = true;

            if (typeDesc != null) {
                int numOfProperties = typeDesc.getNumOfFixedProperties();
                for (int i = 0; i < numOfProperties; i++)
                    if (fieldValues[i] != null)
                        valueMap.put(typeDesc.getFixedProperty(i).getName(), fieldValues[i]);
            }

            this.preparedValues = fieldValues;
        }
    }

    boolean isUseTemplate() {
        return m_isUseTemplate;
    }

    public QueryTableData addTableWithAlias(String table, String alias) {
        QueryTableData tableData = new QueryTableData(table, alias, _tablesData.size());
        _tablesData.add(tableData);
        if (alias != null) {
            tables.put(alias, tableData);
        }
        if(table != null) {
            tables.put(table, tableData);
        }
        return tableData;
    }

    /**
     * Return a table name according to its alias.
     *
     * @return The table name
     */
    public String getTableByAlias(String alias) {
        if (tables == null) {
            tables = new ConcurrentHashMap<String, QueryTableData>();
        }
        QueryTableData tableData = tables.get(alias);

        if (tableData == null && !QueryProcessor.getDefaultConfig().isParserCaseSensitivity())
            return tables.get(alias.toLowerCase()).getTableName();

        if (tableData == null)
            return alias;

        return tableData.getTableName();
    }

    public boolean isBuildOnly() {
        return _buildOnly;
    }

    public void setBuildOnly(boolean isBuildOnly) {
        _buildOnly = isBuildOnly;
    }

    /**
     * @return true if convert result to array
     */
    public boolean isConvertResultToArray() {
        return _convertResultToArray;
    }

    /**
     *
     * @param convertResultToArray
     */
    public void setConvertResultToArray(boolean convertResultToArray) {
        _convertResultToArray = convertResultToArray;
    }


    /**
     * Convert the expression tree to space queries in form of IEntryPacket templates
     */
    public void buildTemplates() throws SQLException {
        _builder.traverseExpressionTree(expTree, true);
    }

    public QueryTemplatePacket getTemplatePacketIfExists() {
        return  expTree != null ? expTree.getTemplate() : null;
    }

    /**
     * @return ITypeDesc
     */
    public ITypeDesc getTypeInfo() {
        QueryTableData queryTableData = tables.get(getTableName());
        if (queryTableData == null)
            return null;
        return queryTableData.getTypeDesc();
    }


    /**
     * Set the prepared values on the expression tree
     */
    public void prepare(ISpaceProxy space, Transaction txn) throws SQLException {
        boolean buildTemplate = isDirtyState() || containsSubQueries();
        //if this is a prepared statement, prepare the values
        if (isPrepared() && expTree != null) {
            if (isUseTemplate()) {
                expTree.prepareTemplateValues(valueMap, null);
            } else {
                if (preparedValues == null)
                    throw new SQLException("Prepared values are not set");
                expTree.prepareValues(preparedValues);
            }
            buildTemplate = true;
        }

        if (buildTemplate) {
            if (containsSubQueries())
                executeSubQueries(space, txn);
            build();
        }

        // at this point the builder finished to build the query templates and this validation can be performed
        validateBuiltTemplates();
    }

    protected void executeSubQueries(ISpaceProxy space, Transaction txn) throws SQLException {
        final Stack<ExpNode> nodes = new Stack<ExpNode>();
        nodes.push(expTree);
        while (!nodes.isEmpty()) {
            final ExpNode currentNode = nodes.pop();
            if (currentNode.getLeftChild() != null)
                nodes.push(currentNode.getLeftChild());
            if (currentNode.getRightChild() != null) {
                if (currentNode.getRightChild().isInnerQuery()) {
                    final IQueryExecutor executor = new QueryExecutor(this);
                    final InnerQueryNode innerQueryNode = (InnerQueryNode) currentNode.getRightChild();
                    innerQueryNode.accept(executor, space, txn, getReadModifier(), Integer.MAX_VALUE);
                    currentNode.validateInnerQueryResult();
                } else {
                    nodes.push(currentNode.getRightChild());
                }
            }
        }
    }

    public boolean isReturnResult() {
        return _returnResult;
    }

    public void setReturnResult(boolean returnResult) {
        _returnResult = returnResult;
    }

    /**
     * @return row number limit
     */
    protected int getRownumLimit() {
        return rownum == null ? Integer.MAX_VALUE : rownum.getLimit();
    }

    protected void filterByRownum(Collection<IEntryPacket> entries) {
        if (rownum == null)
            return;

        // check if rownum should not be applied (entries are in range)
        if (!rownum.hasLimit() || entries.isEmpty() ||
                (rownum.getStartIndex() <= 1 && getRownumLimit() >= entries.size()))
            return;


        // check if rownum filters all the entries
        if (rownum.getStartIndex() > entries.size()) {
            entries.clear();
            return;
        }

        // Copy the trimmed entries to new list
        Iterator<IEntryPacket> iter = entries.iterator();
        for (int i = 1; iter.hasNext(); i++) {
            iter.next();
            if (rownum.isIndexOutOfRange(i))
                iter.remove();

        }

    }


    /**
     * @return the operationID
     */
    public OperationID getOperationID() {
        return _operationID;
    }

    /**
     * @param operationID the operationID to set
     */
    public void setOperationID(OperationID operationID) {
        _operationID = operationID;
    }

    public QueryResultTypeInternal getQueryResultType() {
        return _queryResultType;
    }

    public void setQueryResultType(QueryResultTypeInternal queryResultType) {
        _queryResultType = queryResultType;
    }

    /*
     * @see com.j_spaces.jdbc.Query#setSecurityInterceptor(com.gigaspaces.security.service.SecurityInterceptor)
     */
    public void setSecurityInterceptor(SecurityInterceptor securityInterceptor) {
        this.securityInterceptor = securityInterceptor;
    }

    /**
     * @return the securityInterceptor
     */
    public SecurityInterceptor getSecurityInterceptor() {
        return securityInterceptor;
    }


    public List<QueryTableData> getTablesData() {
        return _tablesData;
    }

    public String getTablesNames() {
        return _tablesData.stream().map(QueryTableData::getTableName).collect(Collectors.joining(","));
    }

    public QueryTableData getTableData(String tableName) {
        if (tableName == null)
            return null;
        return tables.get(tableName);
    }

    public QueryTableData getTableData() {
        if (_tablesData.isEmpty())
            return null;

        return _tablesData.get(0);
    }

    @Override
    public void validateQuery(ISpaceProxy space) throws SQLException {
        for (QueryTableData tableData : _tablesData) {
            if (tableData.getSubQuery() != null)
                tableData.getSubQuery().validateQuery(space);
            else {
                ITypeDesc typeDesc = tableData.getTypeDesc();
                if (typeDesc == null) {
                    String tableName = tableData.getTableName();
                    typeDesc = SQLUtil.checkTableExistence(tableName, space);
                    tableData.setTypeDesc(typeDesc);
                }
            }
        }
    }

    public QueryTemplateBuilder getBuilder() {
        return _builder;
    }

    public long getTimeout() {
        return _timeout;
    }

    public void setTimeout(long timeout) {
        _timeout = timeout;
    }

    public boolean getIfExists() {
        return _ifExists;
    }

    public void setIfExists(boolean ifExists) {
        this._ifExists = ifExists;
    }

    /**
     * Sets the routing value of the query.
     */
    public void setRouting(Object routing) {
        _dirtyState |= !ObjectUtils.equals(this._routing, routing);
        this._routing = routing;
    }

    /**
     * Gets the routing value of the query.
     */
    public Object getRouting() {
        return _routing;
    }

    public void setProjectionTemplate(AbstractProjectionTemplate projectionTemplate) {
        _dirtyState |= !ObjectUtils.equals(this._projectionTemplate, projectionTemplate);
        this._projectionTemplate = projectionTemplate;
    }

    public AbstractProjectionTemplate getProjectionTemplate() {
        return _projectionTemplate;
    }

    /**
     * Gets whether the query is in dirty state
     */
    public boolean isDirtyState() {
        return this._dirtyState;
    }


    /**
     * Gets whether this query is a SELECT query.
     */
    public boolean isSelectQuery() {
        return false;
    }

    /**
     * Gets whether this query is forced to be executed under transaction.
     */
    public boolean isForceUnderTransaction() {
        return false;
    }

    public boolean containsSubQueries() {
        return _containsSubQueries;
    }

    public void setContainsSubQueries(boolean containsSubQueries) {
        _containsSubQueries = containsSubQueries;
    }

    /**
     * Executes a query with batched prepared values. This is the default implementation - NOT
     * optimized.
     *
     * @return BatchResponsePacket
     */
    public BatchResponsePacket executePreparedValuesBatch(ISpaceProxy space, Transaction transaction,
                                                          PreparedValuesCollection preparedValuesCollection) throws SQLException {
        int[] result = new int[preparedValuesCollection.size()];
        String exceptionText = null;
        int batchIndex = 0;
        for (Object[] preparedValues : preparedValuesCollection.getBatchValues()) {
            AbstractDMLQuery query = clone();
            query.setPreparedValues(preparedValues);
            query.setSession(getSession());
            query.setSecurityInterceptor(securityInterceptor);
            try {
                ResponsePacket response = query.executeOnSpace(space, transaction);
                result[batchIndex] = response.getIntResult();
            } catch (SQLException e) {
                exceptionText = e.getMessage();
                result[batchIndex] = Statement.EXECUTE_FAILED;
            }
            batchIndex++;
        }
        if (exceptionText != null)
            throw new BatchUpdateException(exceptionText, result);
        return new BatchResponsePacket(result);
    }

    public void setMaxResults(int maxResults) {
        if (maxResults == Integer.MAX_VALUE)
            return;

        if (rownum == null) {
            rownum = new RowNumNode(1, Integer.MAX_VALUE);
        }

        rownum.setMaxResults(maxResults);
    }

    public void setMinEntriesToWaitFor(int minEntriesToWaitFor) {
        _minEntriesToWaitFor = minEntriesToWaitFor;
    }

    public int getMinEntriesToWaitFor() {
        return _minEntriesToWaitFor;
    }

    public void assignParameters(SQLQuery<?> sqlQuery, IDirectSpaceProxy proxy) {
        // If the query has parameters, set them anyway since they might
        // only relate to a sub query
        if (sqlQuery.hasParameters()) {
            setPreparedValues(sqlQuery.getParameters());
        }
        // Otherwise, for prepared query the template values are used as values
        else if (isPrepared()) {
            Object dataEntry = sqlQuery.getObject();

            ITemplatePacket packet;
            if (dataEntry instanceof ITemplatePacket) {
                packet = (ITemplatePacket) dataEntry;
                proxy.getTypeManager().loadTypeDescToPacket(packet);
            } else {
                ObjectType objectType = ObjectType.fromObject(dataEntry);
                packet = proxy.getTypeManager().getTemplatePacketFromObject(dataEntry, objectType);
                packet.setSerializeTypeDesc(true);
            }

            setTemplatePreparedValues(packet.getTypeDescriptor(), packet.getFieldValues());
        }
    }

    public ExplainPlan getExplainPlan() {
        return _explainPlan;
    }

    public void setExplainPlan(ExplainPlan _explainPlan) {
        this._explainPlan = _explainPlan;
    }

    protected void writeExternal(ObjectOutput out) throws IOException {
        if (securityInterceptor != null)
            throw new IllegalStateException("Cannot be serialized in secured mode");
        out.writeBoolean(isPrepared);
        IOUtils.writeList(out, queryColumns);
        IOUtils.writeObject(out, expTree);
        IOUtils.writeObjectArray(out, preparedValues);
        IOUtils.writeList(out, _tablesData);
        IOUtils.writeObject(out, session);
        IOUtils.writeObject(out, rownum);
        IOUtils.writeObject(out, valueMap);
        out.writeBoolean(m_isUseTemplate);
        IOUtils.writeObject(out, tables);
        out.writeBoolean(_buildOnly);
        out.writeBoolean(_convertResultToArray);
        IOUtils.writeObject(out, _builder);
        out.writeInt(_readModifier);
        out.writeLong(_timeout);
        out.writeBoolean(_ifExists);
        IOUtils.writeObject(out, _routing);
        out.writeBoolean(_dirtyState);
        IOUtils.writeObject(out, _executor);
        out.writeInt(_minEntriesToWaitFor);
        out.writeBoolean(_returnResult);
        IOUtils.writeObject(out, _operationID);
        out.writeByte(_queryResultType.getCode());
        out.writeBoolean(_containsSubQueries);
        IOUtils.writeObject(out, _projectionTemplate);
        // IOUtils.writeObject(out, _explainPlan); not serializable
    }

    protected void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        isPrepared = in.readBoolean();
        queryColumns = IOUtils.readList(in);
        expTree = IOUtils.readObject(in);
        preparedValues = IOUtils.readObjectArray(in);
        _tablesData = Collections.synchronizedList(IOUtils.readList(in));
        session = IOUtils.readObject(in);
        rownum = IOUtils.readObject(in);
        valueMap = IOUtils.readObject(in);
        m_isUseTemplate = in.readBoolean();
        tables = IOUtils.readObject(in);
        _buildOnly = in.readBoolean();
        _convertResultToArray = in.readBoolean();
        _builder = IOUtils.readObject(in);
        _readModifier = in.readInt();
        _timeout = in.readLong();
        _ifExists = in.readBoolean();
        _routing = IOUtils.readObject(in);
        _dirtyState = in.readBoolean();
        _executor = IOUtils.readObject(in);
        _minEntriesToWaitFor = in.readInt();
        _returnResult = in.readBoolean();
        _operationID = IOUtils.readObject(in);
        _queryResultType = QueryResultTypeInternal.fromCode(in.readByte());
        _containsSubQueries = in.readBoolean();
        _projectionTemplate = IOUtils.readObject(in);
        //_explainPlan =IOUtils.readObject(in); not serializable
    }

    public IQueryExecutor getExecutor() {
        return _executor;
    }
}
