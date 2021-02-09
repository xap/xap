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

import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.CountClearProxyActionInfo;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.QueryProxyActionInfo;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.ReadTakeMultipleProxyActionInfo;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.ReadTakeProxyActionInfo;
import com.gigaspaces.internal.exceptions.BatchQueryException;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.jdbc.AbstractDMLQuery;
import com.j_spaces.jdbc.QueryProcessor;
import com.j_spaces.jdbc.QueryProcessorConfiguration;
import com.j_spaces.jdbc.ResponsePacket;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.SQLQueryTemplatePacket;
import com.j_spaces.kernel.threadpool.DynamicThreadPoolExecutor;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.Transaction;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Manages SQLQuery execution on the proxy side
 *
 * @author anna
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class QueryManager implements IQueryManager {
    private static final QueryThreadPoolExecutor _queryExecutionPool = new QueryThreadPoolExecutor();

    private final IDirectSpaceProxy _proxy;

    private final ReadQueryParser _readQueryParser = new ReadQueryParser();
    private final TakeQueryParser _takeQueryParser = new TakeQueryParser();
    private final CountQueryParser _countQueryParser = new CountQueryParser();

    public static QueryThreadPoolExecutor getQueryExecutionPool() {
        return _queryExecutionPool;
    }

    public QueryManager(IDirectSpaceProxy proxy) {
        _proxy = proxy;
        QueryProcessor.setDefaultConfig(new QueryProcessorConfiguration(_proxy.getProxySettings().getSpaceAttributes(), null));
        QueryProcessor.getDefaultConfig().setParserCaseSensitivity(true);
    }

    @Override
    public ITemplatePacket getSQLTemplate(SQLQueryTemplatePacket template, Transaction txn) {
        SQLQuery<?> sqlQuery = template.getQuery();
        try {
            AbstractDMLQuery query = _readQueryParser.parseSqlQuery(sqlQuery, _proxy);
            query.assignParameters(sqlQuery, _proxy);
            query.setBuildOnly(true);
            query.setConvertResultToArray(false);
            query.validateQuery(_proxy);
            query.setRouting(sqlQuery.getRouting());

            ResponsePacket rPacket = query.executeOnSpace(_proxy, txn);

            QueryTemplatePacket resultPacket = (QueryTemplatePacket) rPacket.getFirst();
            resultPacket.setEntryType(template.getEntryType());
            resultPacket.setQueryResultType(template.getQueryResultType());
            resultPacket.setRouting(sqlQuery.getRouting());
            resultPacket.setProjectionTemplate(template.getProjectionTemplate());
            resultPacket.setOperationID(template.getOperationID());
            return resultPacket;
        } catch (SQLException e) {
            throw new SQLQueryException("Failed to create template from SQLQuery : [" + sqlQuery.getQuery() + "]", e);
        }
    }

    public ReadQueryParser getReadQueryParser() {
        return _readQueryParser;
    }

    @Override
    public int countClear(CountClearProxyActionInfo actionInfo) {
        SQLQuery<?> sqlQuery = ((SQLQueryTemplatePacket) actionInfo.queryPacket).getQuery();

        try {
            SqlQueryParser parser = actionInfo.isTake ? _takeQueryParser : _countQueryParser;
            AbstractDMLQuery query = parser.parseSqlQuery(sqlQuery, _proxy);

            query.assignParameters(sqlQuery, _proxy);
            query.setReadModifier(actionInfo.modifiers);
            query.setOperationID(actionInfo.queryPacket.getOperationID());
            query.setQueryResultType(actionInfo.queryPacket.getQueryResultType());
            query.setRouting(sqlQuery.getRouting());
            query.setReturnResult(!actionInfo.isTake);
            query.setConvertResultToArray(!actionInfo.isTake);
            query.setExplainPlan(sqlQuery.getExplainPlan());
            ResponsePacket responsePacket = query.executeOnSpace(_proxy, actionInfo.txn);
            return actionInfo.isTake ? responsePacket.getIntResult() : (Integer) responsePacket.getFirst();
        } catch (SQLException e) {
            throw new SQLQueryException("Failed to execute SQLQuery : [" + sqlQuery.getQuery() + "]" + " cause: " + e.toString(), e);
        }
    }

    @Override
    public IEntryPacket readTake(ReadTakeProxyActionInfo actionInfo)
            throws RemoteException, UnusableEntryException {
        try {
            IEntryPacket[] result = readTake(actionInfo, 1 /*maxResults*/, actionInfo.timeout, actionInfo.ifExists, actionInfo.isTake, 1 /*minEntriesToWaitFor*/);

            if (result.length == 0)
                return null;

            return result[0];
        } catch (BatchQueryException ex) {
            Throwable mainCause = ex.getMajorityCause();
            if (mainCause instanceof RemoteException)
                throw (RemoteException) mainCause;
            if (mainCause instanceof UnusableEntryException)
                throw (UnusableEntryException) mainCause;

            // shouldn't get here - added just to be on the safe side and handle unpredictable bugs
            throw new SQLQueryException("Failed to execute SQLQuery, cause: " + ex.toString(), mainCause);
        }
    }

    @Override
    public IEntryPacket[] readTakeMultiple(ReadTakeMultipleProxyActionInfo actionInfo) {
        return readTake(actionInfo, actionInfo.maxResults, actionInfo.timeout, actionInfo.ifExist, actionInfo.isTake, actionInfo.minEntriesToWaitFor);
    }

    public void clean() {
        _readQueryParser.clean();
        _takeQueryParser.clean();
        _countQueryParser.clean();
    }

    private IEntryPacket[] readTake(QueryProxyActionInfo actionInfo, int maxEntries, long timeout, boolean ifExists, boolean isTake, int minEntriesToWaitFor) {
        final SQLQueryTemplatePacket sqlQueryTemplatePacket = (SQLQueryTemplatePacket) actionInfo.queryPacket;
        final SQLQuery<?> sqlQuery = sqlQueryTemplatePacket.getQuery();

        try {
            AbstractDMLQuery query = isTake
                    ? _takeQueryParser.parseSqlQuery(sqlQuery, _proxy)
                    : _readQueryParser.parseSqlQuery(sqlQuery, _proxy);
            query.assignParameters(sqlQuery, _proxy);
            query.setMaxResults(maxEntries);
            query.setReturnResult(true);
            query.setReadModifier(actionInfo.modifiers);
            query.setOperationID(actionInfo.queryPacket.getOperationID());
            query.setQueryResultType(actionInfo.queryPacket.getQueryResultType());
            query.setConvertResultToArray(false);
            query.setTimeout(timeout);
            query.setIfExists(ifExists);
            query.setRouting(sqlQuery.getRouting());
            query.setProjectionTemplate(sqlQueryTemplatePacket.getProjectionTemplate());
            query.setMinEntriesToWaitFor(minEntriesToWaitFor);
            query.setExplainPlan(sqlQuery.getExplainPlan());
            ResponsePacket rPacket = query.executeOnSpace(_proxy, actionInfo.txn);

            return rPacket.getArray();
        } catch (SQLException e) {
            throw new SQLQueryException("Failed to execute SQLQuery : [" + sqlQuery.getQuery() + "]" + " cause: " + e.toString(), e);
        }
    }

    public static class QueryThreadPoolExecutor extends DynamicThreadPoolExecutor {
        private static final int DEFAULT_QUERY_POOL_MAX_THREADS = 100;
        private static final String QUERY_POOL_MAX_THREADS = "com.gs.proxy.query_max_threads";

        public QueryThreadPoolExecutor(int corePoolSize, int maximumPoolSize,
                                       long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
        }

        private QueryThreadPoolExecutor() {
            this(1, Integer.getInteger(QUERY_POOL_MAX_THREADS, DEFAULT_QUERY_POOL_MAX_THREADS), 10, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        }
    }
}
