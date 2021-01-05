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

import com.gigaspaces.client.transaction.ITransactionManagerProvider;
import com.gigaspaces.client.transaction.TransactionManagerProviderFactory;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.exceptions.BatchQueryException;
import com.gigaspaces.internal.query.explainplan.ExplainPlanImpl;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.security.service.SecurityInterceptor;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.jdbc.driver.GConnection;
import com.j_spaces.jdbc.parser.grammar.SqlParser;
import com.j_spaces.jdbc.request.SetAutoCommitRequest;
import com.j_spaces.jdbc.request.SetTransaction;
import com.j_spaces.jdbc.request.SetUseSingleSpace;
import net.jini.core.lease.LeaseDeniedException;
import net.jini.core.transaction.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Reader;
import java.io.StringReader;
import java.rmi.RemoteException;
import java.sql.SQLException;

/**
 * QueryHandler executes the JDBC  statements set by the {@link GConnection}. For each statement the
 * {@link QueryHandler} performs the parsing (or retrieves the statement from {@link QueryCache} )
 * and calls the necessary methods of the GigaSpaces API.
 *
 * @author Michael Mitrani, 2Train4, 2004
 * @author anna - refactoring
 */
@com.gigaspaces.api.InternalApi
public class QueryHandler {
    private static final Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_QUERY);

    private final ISpaceProxy _spaceCluster;
    private final ISpaceProxy _spaceRegular;
    private final QueryCache _queryCache;
    private final QueryProcessorConfiguration _config;
    private final SecurityInterceptor securityInterceptor;

    // Transaction manager provider is lazy initialized...
    private volatile ITransactionManagerProvider _transactionManagerProvider;

    public QueryHandler(IJSpace spaceCluster, IJSpace spaceRegular,
                        QueryProcessorConfiguration config, SecurityInterceptor securityInterceptor) {
        _spaceCluster = (ISpaceProxy) spaceCluster;
        _spaceRegular = (ISpaceProxy) spaceRegular;
        this.securityInterceptor = securityInterceptor;
        _queryCache = QueryCache.create();
        _config = config;
    }

    /**
     * This is where each packet is handled.
     *
     * @return The ResponsePacket
     */
    private ResponsePacket handleRequest(RequestPacket request, QuerySession session)
            throws LeaseDeniedException, RemoteException, SQLException,
            TransactionException {
        ResponsePacket response = new ResponsePacket();

        //see RequestPacket documentation for types
        ISpaceProxy space = getSpace(session.isUseRegularSpace());

        switch (request.getType()) {
            case STATEMENT:
                Query query = handleStatement(request, space);
                attachTransaction(session, query);
                query.setSession(session);
                query.setSecurityInterceptor(securityInterceptor);

                if( query instanceof SelectQuery &&
                    request.getModifiers() != null &&
                    Modifiers.contains( request.getModifiers(), Modifiers.EXPLAIN_PLAN ) &&
                    ( ( SelectQuery )query ).getExplainPlan() == null ){

                    ( ( SelectQuery )query ).setExplainPlan( new ExplainPlanImpl( null ) );
                }
                response = query.executeOnSpace(space,
                        session.getTransaction());
                if( query instanceof AbstractDMLQuery && ((AbstractDMLQuery)query ).getExplainPlan() != null ){
                    response = new ExplainPlanResponsePacket( response, ((AbstractDMLQuery)query ).getExplainPlan().toString() );
                }

                session.setUnderTransaction(request.getStatement());
                commitForcedTransaction(query, session);
                break;
            case PREPARED_WITH_VALUES:
                AbstractDMLQuery dmlQuery = (AbstractDMLQuery) handleStatement(request, space);
                attachTransaction(session, dmlQuery);
                request.build(dmlQuery);

                dmlQuery.setSession(session);
                dmlQuery.setSecurityInterceptor(securityInterceptor);
                response = dmlQuery.executeOnSpace(space,
                        session.getTransaction());
                session.setUnderTransaction(request.getStatement());
                commitForcedTransaction(dmlQuery, session);
                break;
            case PREPARED_STATEMENT:
                query = handleStatement(request, space);
                query.setSession(session);
                query.setSecurityInterceptor(securityInterceptor);
                response.setIntResult(0);// parsed statement
                break;
            case PREPARED_VALUES_BATCH:
                dmlQuery = (AbstractDMLQuery) handleStatement(request, space);
                attachTransaction(session, dmlQuery);
                dmlQuery.setSession(session);
                dmlQuery.setSecurityInterceptor(securityInterceptor);
                response = dmlQuery.executePreparedValuesBatch(
                        space, session.getTransaction(), request.getPreparedValuesCollection());
                session.setUnderTransaction(request.getStatement());
                commitForcedTransaction(dmlQuery, session);
                break;
            default:
                throw new SQLException("Unknown execution type [" + request.getType() + "]", "GSP", -117);
        }
        return response;
    }

    /**
     * Commits the transaction if it was forced by the query.
     */
    private void commitForcedTransaction(Query query, QuerySession session) throws UnknownTransactionException, CannotCommitException, RemoteException {
        if (session.isAutoCommit() && query.isForceUnderTransaction()) {
            session.getTransaction().commit();
            session.setSelectedForUpdate(null);
            session.setTransaction(null);
        }
    }

    /**
     * Attach transaction to the session if needed
     */
    private void attachTransaction(QuerySession session, Query query)
            throws TransactionException, RemoteException,
            LeaseDeniedException {
        if ((!session.isAutoCommit() && session.getTransaction() == null)
                || query.isForceUnderTransaction())
            session.setTransaction(createTransaction());

        session.setQueryHandler(this);
    }

    /**
     * The main method to handle the query. first it will try to retrieve the query from the cache,
     * if its not there, it will build a parser to parse the statement and then put it in the
     * cache.
     */
    public Query handleStatement(RequestPacket request, ISpaceProxy space) throws SQLException {
        // first, try to get it from the cache.
        Query query = _queryCache.getQueryFromCache(request.getStatement());
        try {
            if (query == null) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug("Query wasn't in cache, will be parsed");
                }

                // query was not in the cache to build a parser to parse it.
                StringReader sReader = new StringReader(request.getStatement());
                Reader reader = new BufferedReader(sReader);
                SqlParser parser = new SqlParser(reader);
                query = parser.parseStatement();
                query.validateQuery(space);

                if (!query.isPrepared() && !query.containsSubQueries())
                    query.build();

                _queryCache.addQueryToCache(request.getStatement(), query);

            }
            // Clone the query  to avoid concurrency issues
            if (query instanceof AbstractDMLQuery) {
                query = ((AbstractDMLQuery) query).clone();
            }

            return query;
        } catch (SQLException sqlEx) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("Error executing statement ["
                        + request + "]", sqlEx);
            }
            throw sqlEx;
        } catch (Throwable t) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("Couldn't parse given statement ["
                        + request + "]", t);
            }
            // now should throw an SQLException back to the JDBC driver.
            SQLException sqlEx = new SQLException("Error in statement ["
                    + request + "]; Cause: " + t, "GSP", -201, t);
            throw sqlEx;
        }
    }

    /**
     * Create new transaction using the local transaction manager
     *
     * @return created transaction
     */
    private Transaction createTransaction() throws LeaseDeniedException,
            RemoteException, TransactionException {
        ITransactionManagerProvider transactionManagerProvider = getTransactionManagerProvider();
        return (TransactionFactory.create(transactionManagerProvider.getTransactionManager(), _config.getTransactionTimeout())).transaction;
    }

    public ITransactionManagerProvider getTransactionManagerProvider() throws TransactionException, RemoteException {
        if (_transactionManagerProvider == null) {
            synchronized (this) {
                if (_transactionManagerProvider == null) {
                    _transactionManagerProvider = TransactionManagerProviderFactory.newInstance(getSpace(false),
                            _config.getTransactionManagerConfiguration());
                }
            }
        }
        return _transactionManagerProvider;
    }

    /**
     * Get the space for query execution
     *
     * @param useRegularSpace if set to true - regular proxy is returned, otherwise - clustered
     *                        proxy
     */
    ISpaceProxy getSpace(boolean useRegularSpace) {
        return (useRegularSpace == false && _spaceCluster != null) ? _spaceCluster
                : _spaceRegular;
    }

    /**
     * Close QueryHandler resources
     */
    public void close() throws RemoteException {

        if (_transactionManagerProvider != null)
            _transactionManagerProvider.destroy();

        _queryCache.clear();
    }

    public QueryCache getQueryCache() {
        return _queryCache;
    }

    /**
     * @return response packet
     */
    public ResponsePacket visit(RequestPacket request, QuerySession session)
            throws LeaseDeniedException, RemoteException,
            TransactionException, SQLException {
        try {
            return handleRequest(request, session);
        } catch (BatchQueryException ex) {
            SQLException sqlEx = new SQLException("Failed to execute SQL command.");
            sqlEx.initCause(ex);
            throw sqlEx;
        }
    }

    /**
     * Handle set auto commit [on off] request
     *
     * @return response packet
     */
    public ResponsePacket visit(SetAutoCommitRequest request,
                                QuerySession session) throws LeaseDeniedException, RemoteException, TransactionException {
        ResponsePacket response = new ResponsePacket();

        if (request.isAutoCommit() == session.isAutoCommit())
            return response;

        if (request.isAutoCommit()) {
            if (session.getTransaction() != null)
                try {
                    session.getTransaction().abort();
                } catch (Exception e) {
                    if (_logger.isDebugEnabled()) {
                        _logger.debug(
                                "Failed to abort transaction.",
                                e);
                    }
                }

            session.setTransaction(null);
            session.clearUnderTransaction();
        } else {
            session.setTransaction(createTransaction());
        }
        session.setAutoCommit(request.isAutoCommit());
        session.setSelectedForUpdate(null);//just to make sure there aren't any
        response.setIntResult(0);// all OK
        return response;
    }

    /**
     * Handle set single space [on off] request
     *
     * @return response packet
     */
    public ResponsePacket visit(SetUseSingleSpace request, QuerySession session) {
        ResponsePacket response = new ResponsePacket();

        session.setUseRegularSpace(request.isUseSingleSpace());

        return response;
    }

    /**
     * Handle set transaction lease time
     *
     * @return response packet
     */
    public ResponsePacket visit(SetTransaction request, QuerySession session) {
        ResponsePacket response = new ResponsePacket();
        session.setTransaction(request.getTransaction());
        response.setIntResult(0);// all OK
        return response;
    }

}
