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
package com.gigaspaces.sql.aggregatornode.netty.query;

import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;

import java.util.List;

/**
 * Hides query execution internals, may be built
 * over a JDBC driver or exploit internal query API.
 */
public interface QueryProvider {
    /**
     * Executes multiline query.
     *
     * @param session Session
     * @param qry Query string.
     * @return List of query cursors.
     */
    List<Portal<?>> executeQueryMultiline(Session session, String qry) throws ProtocolException;

    /**
     * Prepares a query for future execution
     * @param session Session
     * @param stmt Statement name, named statement may be executed without
     *             parsing and validation. You should consider it as a prepared statement id.
     *             May be empty, in this case the statement will free resources and be destroyed
     *             right after execution
     * @param qry Query string.
     * @param paramTypes Inferred parameter types, an ODBC/JDBC driver does a query pre-parsing to
*                   identify query parameter types. May be empty, in this case server should
     */
    void prepare(Session session, String stmt, String qry, int[] paramTypes) throws ProtocolException;

    /**
     * Setups prepared statement parameters and binds the statement with
     * a portal - a PG abstraction describing server side cursor.
     * @param session Session
     * @param portal Portal name, or a server side cursor name.
     * @param stmt Statement name.
     * @param params Parameter values.
     * @param formatCodes Result format codes, it says how to serialize values - as text or as binary data.
     */
    void bind(Session session, String portal, String stmt, Object[] params, int[] formatCodes) throws ProtocolException;

    /**
     * Describes a statement, its parameters and result columns
     * @param stmt Describing statement name.
     * @return Statement description.
     */
    StatementDescription describeS(String stmt) throws ProtocolException;

    /**
     * Describes a portal and portal result columns.
     * @param portal Describing portal.
     * @return Portal description.
     */
    RowDescription describeP(String portal) throws ProtocolException;

    /**
     * Executes prepared and bind to a portal statement.
     * @param portal Portal name.
     * @return Result iterator.
     */
    Portal<?> execute(String portal) throws ProtocolException;

    /**
     * Closes a statement with given name and releases associated resources (including bound portals).
     *
     * @param stmt Statement name;
     */
    void closeS(String stmt) throws ProtocolException;

    /**
     * Closes a portal with given name and releases associated resources.
     *
     * @param portal Portal name.
     */
    void closeP(String portal) throws ProtocolException;

    /**
     * Cancels currently running query. Since in PostgreSQL all sessions are single-threaded,
     * session process cannot process messages while query executing, so that in order to cancel
     * a running query a new process started and cancels the query using query process id and session secret.
     * @param pid Process id.
     * @param secret Session secret.
     */
    void cancel(int pid, int secret);
}
