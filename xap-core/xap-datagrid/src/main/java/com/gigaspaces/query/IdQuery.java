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
package com.gigaspaces.query;

import java.util.Arrays;

/**
 * Class to encapsulate information of a query based on a Space ID.
 *
 * An ID query is composed of a type (provided either as a java class or as a string which contains
 * the type name) and an ID value, and optionally a routing value an a query result type indicator.
 *
 * @author Niv Ingberg
 * @see com.gigaspaces.query.QueryResultType
 * @see com.gigaspaces.query.IdsQuery
 * @since 8.0
 */
public class IdQuery<T> implements ISpaceQuery<T> {
    private final String _typeName;
    private final Object _id;
    private final Object _routing;
    private final QueryResultType _queryResultType;
    private final int _version;
    private String[] _projections;

    /**
     * Creates an IdQuery using the specified type and id.
     *
     * @param type Type to query.
     * @param id   Id to match.
     */
    public IdQuery(Class<T> type, Object id) {
        this(type.getName(), id, null, QueryResultType.DEFAULT, 0);
    }

    /**
     * Creates an IdQuery using the specified type, id and routing.
     *
     * @param type    Type to query.
     * @param id      Id to match.
     * @param routing Routing value to determine which partition to query.
     */
    public IdQuery(Class<T> type, Object id, Object routing) {
        this(type.getName(), id, routing, QueryResultType.DEFAULT, 0);
    }

    /**
     * Creates an IdQuery using the specified type, id and routing.
     *
     * @param type    Type to query.
     * @param id      Id to match.
     * @param routing Routing value to determine which partition to query.
     * @param version to consider when performing a modifying operation (take/clear/change), 0
     *                version means no version check should be made.
     */
    public IdQuery(Class<T> type, Object id, Object routing, int version) {
        this(type.getName(), id, routing, QueryResultType.DEFAULT, version);
    }

    /**
     * Creates an IdQuery using the specified type, id and query result type.
     *
     * @param type            Type to query.
     * @param id              Id to match.
     * @param queryResultType Type of result.
     */
    public IdQuery(Class<T> type, Object id, QueryResultType queryResultType) {
        this(type.getName(), id, null, queryResultType, 0);
    }

    /**
     * Creates an IdQuery using the specified type, id, routing and query result type.
     *
     * @param type            Type to query.
     * @param id              Id to match.
     * @param routing         Routing value to determine which partition to query.
     * @param queryResultType Type of result.
     */
    public IdQuery(Class<T> type, Object id, Object routing, QueryResultType queryResultType) {
        this(type.getName(), id, routing, queryResultType, 0);
    }

    /**
     * Creates an IdQuery using the specified type, id, routing and query result type.
     *
     * @param type            Type to query.
     * @param id              Id to match.
     * @param routing         Routing value to determine which partition to query.
     * @param queryResultType Type of result.
     * @param version         to consider when performing a modifying operation (take/clear/change),
     *                        0 version means no version check should be made.
     */
    public IdQuery(Class<T> type, Object id, Object routing, QueryResultType queryResultType, int version) {
        this(type.getName(), id, routing, queryResultType, version);
    }

    /**
     * Creates an IdQuery using the specified type and id.
     *
     * @param typeName Type to query.
     * @param id       Id to match.
     */
    public IdQuery(String typeName, Object id) {
        this(typeName, id, null, QueryResultType.DEFAULT, 0);
    }

    /**
     * Creates an IdQuery using the specified type, id and routing.
     *
     * @param typeName Type to query.
     * @param id       Id to match.
     * @param routing  Routing value to determine which partition to query.
     */
    public IdQuery(String typeName, Object id, Object routing) {
        this(typeName, id, routing, QueryResultType.DEFAULT, 0);
    }

    /**
     * Creates an IdQuery using the specified type, id and routing.
     *
     * @param typeName Type to query.
     * @param id       Id to match.
     * @param routing  Routing value to determine which partition to query.
     * @param version  to consider when performing a modifying operation (take/clear/change), 0
     *                 version means no version check should be made.
     */
    public IdQuery(String typeName, Object id, Object routing, int version) {
        this(typeName, id, routing, QueryResultType.DEFAULT, version);
    }

    /**
     * Creates an IdQuery using the specified type, id and query result type.
     *
     * @param typeName        Type to query.
     * @param id              Id to match.
     * @param queryResultType Type of result.
     */
    public IdQuery(String typeName, Object id, QueryResultType queryResultType) {
        this(typeName, id, null, queryResultType, 0);
    }

    /**
     * Creates an IdQuery using the specified type, id, routing and query result type.
     *
     * @param typeName        Type to query.
     * @param id              Id to match.
     * @param routing         Routing value to determine which partition to query.
     * @param queryResultType Type of result.
     */
    public IdQuery(String typeName, Object id, Object routing, QueryResultType queryResultType) {
        this(typeName, id, routing, queryResultType, 0);
    }

    /**
     * Creates an IdQuery using the specified type, id, routing and query result type.
     *
     * @param typeName        Type to query.
     * @param id              Id to match.
     * @param routing         Routing value to determine which partition to query.
     * @param queryResultType Type of result.
     * @param version         to consider when performing a modifying operation (take/clear/change),
     *                        0 version means no version check should be made.
     */
    public IdQuery(String typeName, Object id, Object routing, QueryResultType queryResultType, int version) {
        if (typeName == null || typeName.length() == 0)
            throw new IllegalArgumentException("Argument cannot be null or empty - 'typeName'.");
        if (id == null)
            throw new IllegalArgumentException("Argument cannot be null - 'id'.");
        if (queryResultType == null)
            throw new IllegalArgumentException("Argument cannot be null - 'queryResultType'.");
        if (version < 0)
            throw new IllegalArgumentException("Argument cannot be less than zero - 'version'");

        this._typeName = typeName;
        this._id = id;
        this._routing = routing;
        this._queryResultType = queryResultType;
        this._version = version;
    }

    /**
     * Returns the name of type to query.
     */
    public String getTypeName() {
        return _typeName;
    }

    /**
     * Returns the id to match.
     */
    public Object getId() {
        return _id;
    }

    /**
     * Returns the routing value used to determine the partition to query.
     */
    public Object getRouting() {
        return _routing;
    }

    /**
     * Returns the type of result.
     */
    public QueryResultType getQueryResultType() {
        return _queryResultType;
    }

    /**
     * Returns the version.
     */
    public int getVersion() {
        return _version;
    }

    /**
     * Sets projection properties which specifies that a result for an operation using this query
     * should contain data for the specified projection properties. Other properties which were not
     * specifies as a projection will not contain data. By default, if no projection was added, all
     * the properties are returned with full data.
     */
    public IdQuery<T> setProjections(String... properties) {
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

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!(obj instanceof IdQuery))
            return false;
        IdQuery other = (IdQuery) obj;

        return equals(this._id, other._id) &&
                equals(this._routing, other._routing) &&
                equals(this._queryResultType, other._queryResultType) &&
                this._version == other._version &&
                Arrays.equals(this._projections, other._projections);
    }

    private static boolean equals(Object o1, Object o2) {
        return o1 == o2 || o1 != null && o1.equals(o2);
    }
}
