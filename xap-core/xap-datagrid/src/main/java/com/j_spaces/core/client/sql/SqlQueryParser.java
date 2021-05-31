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

package com.j_spaces.core.client.sql;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.query.explainplan.ExplainPlan;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.jdbc.AbstractDMLQuery;
import com.j_spaces.jdbc.Query;
import com.j_spaces.jdbc.QueryCache;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.parser.grammar.ParseException;
import com.j_spaces.jdbc.parser.grammar.SqlParser;

import java.io.BufferedReader;
import java.io.Reader;
import java.io.StringReader;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The parser manager is responsible for handling statements and calling the SqlParser for parsing
 * and creating Query objects. ParserManager also maintains the statements cache in a {@link
 * QueryCache}
 */
public abstract class SqlQueryParser {
    // logger
    private final static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_QUERY);
    private final QueryCache _queryCache = QueryCache.create();
    private final ThreadLocal<SqlParser> _parser = new ThreadLocal<SqlParser>();


    public SqlQueryParser() {
    }


    /**
     * The main method to handle the query. first it will try to retrieve the query from the cache,
     * if its not there, it will build a parser to parse the statement and then put it in the
     * cache.
     */
    public AbstractDMLQuery parseSqlQuery(SQLQuery sqlQuery, ISpaceProxy space) throws SQLException {
        // first, try to get it from the cache.
        AbstractDMLQuery query = (AbstractDMLQuery) getQueryFromCache(getUniqueKey(sqlQuery));
        try {
            if (query == null) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug("Query wasn't in cache, will be parsed");
                }

                // query was not in the cache to build a parser to parse it.
                SqlParser parser = initParser(sqlQuery.getQuery());

                query = parse(parser);
                query.setTableName(sqlQuery.getTypeName());

                query.validateQuery(space);

                if (!query.isPrepared() && !query.containsSubQueries())
                    query.build();

                addQueryToCache(getUniqueKey(sqlQuery), query);

                if (!query.isPrepared())
                    return query;
            }
            // Clone the query  to avoid concurrency issues
            query = query.clone();

            return query;
        } catch (SQLException sqlEx) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("Error executing statement ["
                        + sqlQuery.getQuery() + "]", sqlEx);
            }
            throw sqlEx;
        } catch (Throwable t) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("Couldn't parse given statement ["
                        + sqlQuery.getQuery() + "]", t);
            }
            // now should throw an SQLException back to the JDBC driver.
            SQLException sqlEx = new SQLException("Error in statement [" + sqlQuery.getQuery() + "]; Cause: " + t,
                    "GSP", -201);
            sqlEx.initCause(t);
            throw sqlEx;
        }
    }

    public AbstractDMLQuery parseSqlQuery(SQLQuery sqlQuery, SpaceTypeManager typeManager) throws SQLException {
        // first, try to get it from the cache.
        AbstractDMLQuery query = (AbstractDMLQuery) getQueryFromCache(getUniqueKey(sqlQuery));
        try {
            if (query == null) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug("Query wasn't in cache, will be parsed");
                }

                // query was not in the cache to build a parser to parse it.
                SqlParser parser = initParser(sqlQuery.getQuery());

                query = parse(parser);
                query.setTableName(sqlQuery.getTypeName());

                query.validateQuery(typeManager);

                if (!query.isPrepared() && !query.containsSubQueries())
                    query.build();

                addQueryToCache(getUniqueKey(sqlQuery), query);

                if (!query.isPrepared())
                    return query;
            }
            // Clone the query  to avoid concurrency issues
            query = query.clone();

            return query;
        } catch (SQLException sqlEx) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("Error executing statement ["
                        + sqlQuery.getQuery() + "]", sqlEx);
            }
            throw sqlEx;
        } catch (Throwable t) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("Couldn't parse given statement ["
                        + sqlQuery.getQuery() + "]", t);
            }
            // now should throw an SQLException back to the JDBC driver.
            SQLException sqlEx = new SQLException("Error in statement [" + sqlQuery.getQuery() + "]; Cause: " + t,
                    "GSP", -201);
            sqlEx.initCause(t);
            throw sqlEx;
        }
    }

    private void attachExplainPlan(AbstractDMLQuery query, ExplainPlan explainPlan) {
        QueryTemplatePacket qtp = query.getTemplatePacketIfExists();
        if (qtp != null) {
            qtp.setExplainPlan(explainPlan);
        }
    }


    private void addQueryToCache(String statement, Query query) {
        if (query instanceof AbstractDMLQuery && ((AbstractDMLQuery) query).getExplainPlan() != null) {
            return;
        }
        _queryCache.addQueryToCache(statement, query);
    }

    // return the query from the cache, it may be null though, so the caller
    // method should check
    private Query getQueryFromCache(String statement) {
        return _queryCache.getQueryFromCache(statement);
    }


    /**
     * @return SqlParser
     */
    private SqlParser initParser(String query) {
        StringReader sReader = new StringReader(query);
        Reader reader = new BufferedReader(sReader);


        SqlParser parser = _parser.get();

        if (parser == null) {
            parser = new SqlParser(reader);
            _parser.set(parser);
        } else {
            parser.reset(reader);
        }
        return parser;
    }


    private String getUniqueKey(SQLQuery<?> sqlQuery) {
        return sqlQuery.getTypeName() + ":" + sqlQuery.getQuery();
    }


    /**
     * Cleans resources held by this parser manager.
     */
    public synchronized void clean() {
        _queryCache.clear();


    }

    /**
     * @param parser
     * @throws ParseException
     */
    protected abstract AbstractDMLQuery parse(SqlParser parser)
            throws ParseException;


}
