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


package com.j_spaces.core.client;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.query.explainplan.ExplainPlanImpl;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.utils.ObjectUtils;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.query.ISpaceQuery;
import com.gigaspaces.query.QueryResultType;
import com.gigaspaces.query.explainplan.ExplainPlan;
import com.j_spaces.core.client.sql.SQLQueryException;

import java.io.Serializable;
import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * The SQLQuery class is used to query the space using the SQL syntax. The query statement should
 * only include the WHERE clause.<br><br>
 *
 * <b>Date and Time Formats</b><br> The following Date/TimeStamp Format is supported:
 * <code>'yyyy-mm-dd hh:mm:ss'</code> - i.e. '2004-12-20 20:40:10'<br> The following Time Format is
 * supported: <code>'hh:mm:ss'</code> - i.e. '20:40:10'
 *
 * @see SQLQueryException thrown when space operations with SQLQuery fails.
 */

public class SQLQuery<T> implements ISpaceQuery<T>, Serializable {
    private static final long serialVersionUID = 436464302946431981L;

    // Default query result type explicitly set to depracated NOT_SET to preserve backwards compatibility.
    private static final QueryResultType DEFAULT_QUERY_RESULT_TYPE = QueryResultType.NOT_SET;

    private static final String SPACES = "( |\\n|\\t)+";
    private static final String ALL = ".*";
    private static final Pattern CALL = Pattern.compile("call" + SPACES + ALL);
    private static final Pattern GROUP = Pattern.compile("group" + SPACES + "by" + SPACES + ALL);
    private static final Pattern ORDER = Pattern.compile("order" + SPACES + "by" + SPACES + ALL);

    private String _typeName;
    private String _expression;
    private boolean _isNullExpression;
    private T _template;
    private Object[] _parameters;
    private QueryResultType _queryResultType = DEFAULT_QUERY_RESULT_TYPE;
    private transient Object _routing;
    private transient String[] _projections;
    private transient ExplainPlan _explainPlan;


    /**
     * Empty constructor.
     */
    public SQLQuery() {
        this(Object.class.getName(), "");
    }

    /**
     * Creates a SQLQuery using the specified type and expression.
     *
     * @param typeName      Entry type to be queried.
     * @param sqlExpression The SQL Query expression (contents of the WHERE part).
     */
    public SQLQuery(String typeName, String sqlExpression) {
        this(sqlExpression, typeName, null, DEFAULT_QUERY_RESULT_TYPE, null);
    }

    /**
     * Creates a SQLQuery using the specified type, expression and result type.
     *
     * @param typeName        Entry type to be queried.
     * @param sqlExpression   The SQL Query expression (contents of the WHERE part).
     * @param queryResultType Type of result.
     * @since 8.0
     */
    public SQLQuery(String typeName, String sqlExpression, QueryResultType queryResultType) {
        this(sqlExpression, typeName, null, queryResultType, null);
    }

    /**
     * Creates a SQLQuery using the specified type, expression and parameters.
     *
     * @param typeName      Entry type to be queried.
     * @param sqlExpression The SQL Query expression (contents of the WHERE part).
     * @param parameters    Parameters for the sql query.
     */
    public SQLQuery(String typeName, String sqlExpression, Object... parameters) {
        this(sqlExpression, typeName, null, DEFAULT_QUERY_RESULT_TYPE, parameters);
    }

    /**
     * Creates a SQLQuery using the specified type, expression, result type and parameters.
     *
     * @param typeName        Entry type to be queried.
     * @param sqlExpression   The SQL Query expression (contents of the WHERE part).
     * @param queryResultType Type of result.
     * @param parameters      Parameters for the sql query.
     * @since 8.0
     */
    public SQLQuery(String typeName, String sqlExpression, QueryResultType queryResultType, Object... parameters) {
        this(sqlExpression, typeName, null, queryResultType, parameters);
    }

    /**
     * Creates a SQLQuery using the specified type and expression.
     *
     * @param type          Entry class  to be queried.
     * @param sqlExpression The SQL Query expression (contents of the WHERE part).
     */
    public SQLQuery(Class<T> type, String sqlExpression) {
        this(sqlExpression, type.getName(), null, DEFAULT_QUERY_RESULT_TYPE, null);
    }

    /**
     * Creates a SQLQuery using the specified type, expression and result type.
     *
     * @param type            Entry type to be queried.
     * @param sqlExpression   The SQL Query expression (contents of the WHERE part).
     * @param queryResultType Type of result.
     * @since 8.0
     */
    public SQLQuery(Class<T> type, String sqlExpression, QueryResultType queryResultType) {
        this(sqlExpression, type.getName(), null, queryResultType, null);
    }

    /**
     * Creates a SQLQuery using the specified type, expression and parameters.
     *
     * @param type          Entry type to be queried.
     * @param sqlExpression The SQL Query expression (contents of the WHERE part).
     * @param parameters    Parameters for the sql query.
     */
    public SQLQuery(Class<T> type, String sqlExpression, Object... parameters) {
        this(sqlExpression, type.getName(), null, DEFAULT_QUERY_RESULT_TYPE, parameters);
    }

    /**
     * Creates a SQLQuery using the specified type, expression, result type and parameters.
     *
     * @param type            Entry type to be queried.
     * @param sqlExpression   The SQL Query expression (contents of the WHERE part).
     * @param queryResultType Type of result.
     * @param parameters      Parameters for the sql query.
     * @since 8.0
     */
    public SQLQuery(Class<T> type, String sqlExpression, QueryResultType queryResultType, Object... parameters) {
        this(sqlExpression, type.getName(), null, queryResultType, parameters);
    }

    /**
     * This constructor has been depracated and should not be used.
     *
     * @deprecated Use {@link #SQLQuery(Class, String)} or {@link SQLQuery#SQLQuery(String, String)}
     * instead.
     */
    @Deprecated // Since 8.0
    public SQLQuery(T object, String sqlQuery) {
        this(sqlQuery, null, object, DEFAULT_QUERY_RESULT_TYPE, null);
    }

    private SQLQuery(String expression, String typeName, T template, QueryResultType queryResultType, Object[] parameters) {
        if (queryResultType == null)
            throw new IllegalArgumentException("Argument cannot be null - 'queryResultType'.");

        this._isNullExpression = (expression == null);
        this._expression = _isNullExpression ? "" : expression;
        this._typeName = template != null ? initTypeNameFromTemplate(template) : typeName;
        this._template = template;
        this._queryResultType = queryResultType;

        if (template == null && (parameters == null || parameters.length == 0))
            initParameters();
        else
            setParameters(parameters);
    }

    private static String initTypeNameFromTemplate(Object template) {
        if (template instanceof IEntryPacket)
            return ((IEntryPacket) template).getTypeName();
        if (template instanceof ExternalEntry)
            return ((ExternalEntry) template).getClassName();
        if (template instanceof SpaceDocument)
            return ((SpaceDocument) template).getTypeName();
        return template.getClass().getName();
    }

    @ExperimentalApi
    public SQLQuery<T> withExplainPlan() {
        _explainPlan = new ExplainPlanImpl(this);
        return this;
    }

    @ExperimentalApi
    public ExplainPlan getExplainPlan(){
        return this._explainPlan;
    }


    /**
     * This method has been depracated and should not be used.
     *
     * @deprecated Use SQLQuery with parameters instead of a template.
     */
    @Deprecated // Since 8.0
    public T getObject() {
        return _template;
    }

    /**
     * Returns the name of the type which will be queried.
     *
     * @return the type name
     */
    public String getTypeName() {
        return _typeName;
    }

    /**
     * This method should be used for check expression string.
     *
     * @return <code>true</code> if expression is null or empty
     */
    public boolean isNullExpression() {
        return _isNullExpression;
    }

    /**
     * Returns the 'WHERE' part of this SQL Query.
     *
     * @return 'WHERE' part expression.
     */
    public String getQuery() {
        return _expression;
    }

    /**
     * Returns a string representation of this SQLQuery, in the form of: <p> FROM <tt>table
     * name</tt> WHERE <tt>query expression</tt> <p>
     *
     * @return the string representation of the query
     */
    public String getFromQuery() {
        StringBuilder fromPart = new StringBuilder("FROM ").append(_typeName);
        if (_expression != null) {
            if (hasWhereClause())
                fromPart.append(" WHERE ");
            else
                fromPart.append(" ");

            fromPart.append(_expression);
        }
        return fromPart.toString();
    }

    /**
     * Returns a string representation of this SQLQuery, in the form of: <p> SELECT * FROM <tt>table
     * name</tt> WHERE <tt>query expression</tt> <p>
     *
     * @return a string representation of the object.
     */
    public String getSelectAllQuery() {
        return "SELECT * " + getFromQuery();
    }

    /**
     * Returns a string representation of this SQLQuery, in the form of: <p> SELECT count(*) FROM
     * <tt>table name</tt> WHERE <tt>query expression</tt> <p>
     *
     * @return a string representation of the object.
     */
    public String getSelectCountQuery() {
        return "SELECT count(*) " + getFromQuery();
    }

    /**
     * Returns a string representation of this SQLQuery, in the form of: <p> SELECT * FROM <tt>table
     * name</tt> WHERE <tt>query expression</tt> <p>
     *
     * @return a string representation of the object.
     */
    @Override
    public String toString() {
        return getSelectAllQuery();
    }

    /**
     * Returns true if this query is a stored procedure
     *
     * @return <code>true</code> if represents a stored procedure
     */
    public boolean isStoredProcedure() {
        if (_expression == null)
            return false;

        String trimmed = _expression.trim();

        return CALL.matcher(trimmed).matches();
    }

    /**
     * Returns true if the query has a where clause. Used in partial SQLQuery
     *
     * @return <code>true</code> has a "where" part
     */
    public boolean hasWhereClause() {
        if (_expression == null)
            return false;

        String trimmed = _expression.trim();

        return !(trimmed.length() == 0 || ORDER.matcher(trimmed).matches() || GROUP.matcher(trimmed).matches());
    }

    /**
     * @return the parameters
     */
    public Object[] getParameters() {
        return _parameters;
    }

    /**
     * @return true if the SQLQuery has parameters
     */
    public boolean hasParameters() {
        return _parameters != null && _parameters.length != 0;
    }

    /**
     * @param parameters the parameters to set
     */
    public void setParameters(Object... parameters) {
        _parameters = parameters;
    }

    /**
     * Set the query parameter value.
     *
     * @param index parameter index - start with 1
     * @param value parameter value
     */
    public SQLQuery<T> setParameter(int index, Object value) {
        if (_parameters == null)
            initParameters();

        if (index > _parameters.length)
            throw new IllegalArgumentException("Parameter index [" + index + "] exceeds number of parameters [" + _parameters.length + "]");

        _parameters[index - 1] = value;
        return this;
    }

    private void initParameters() {
        int parametersNum = StringUtils.countOccurrencesOf(_expression, '?');
        if (parametersNum != 0)
            _parameters = new Object[parametersNum];
    }

    /**
     * Returns the result type of this query.
     */
    public QueryResultType getQueryResultType() {
        if (_queryResultType == null)
            return DEFAULT_QUERY_RESULT_TYPE;
        return _queryResultType;
    }

    /**
     * Sets the routing value of this query.
     *
     * @param routing The routing value
     */
    public void setRouting(Object routing) {
        this._routing = routing;
    }

    /**
     * Gets the routing value of this query.
     *
     * @return The routing value of the query
     */
    public Object getRouting() {
        return _routing;
    }

    @Override
    public int hashCode() {
        return _expression == null ? 0 : _expression.hashCode();
    }

    /**
     * Indicates whether some {@link SQLQuery} is "equal to" this one.
     *
     * @return <code>true</code> if the input object object is not null, of type {@link SQLQuery}
     * and this query's SQL expression, class (class name) and parameters are equal to the input
     * SQLQuery.
     */
    @Override
    public boolean equals(Object obj) {
        /*
         *  Equals ignore the template member. Since it is only used for {@link SharedDataIterator} and
         * when a .NET data source is in use the equals of the template will never return true 
		 */
        if (obj == null)
            return false;
        if (!(obj instanceof SQLQuery))
            return false;

        SQLQuery<T> other = (SQLQuery<T>) obj;

        if (!ObjectUtils.equals(this._expression, other._expression))
            return false;
        if (!ObjectUtils.equals(this._typeName, other._typeName))
            return false;
        if (!Arrays.equals(this._parameters, other._parameters))
            return false;
        if (!Arrays.equals(this._projections, other._projections))
            return false;

        return true;
    }

    /**
     * Sets projection properties which specifies that a result for an operation using this query
     * should contain data for the specified projection properties. Other properties which were not
     * specifies as a projection will not contain data. By default, if no projection was added, all
     * the properties are returned with full data.
     */
    public SQLQuery<T> setProjections(String... properties) {
        _projections = properties;
        return this;
    }

    /**
     * Returns the projections set on this query, null means no projection is used and all the
     * properties should be returned.
     */
    public String[] getProjections() {
        return _projections;
    }
}
