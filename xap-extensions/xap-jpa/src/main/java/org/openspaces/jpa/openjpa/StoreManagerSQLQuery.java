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

package org.openspaces.jpa.openjpa;

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.executor.SpaceTask;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.utils.CollectionUtils;
import com.gigaspaces.internal.utils.StringUtils;
import com.j_spaces.core.client.SQLQuery;

import org.apache.openjpa.kernel.AbstractStoreQuery;
import org.apache.openjpa.kernel.QueryContext;
import org.apache.openjpa.kernel.StoreContext;
import org.apache.openjpa.kernel.StoreQuery;
import org.apache.openjpa.lib.rop.ListResultObjectProvider;
import org.apache.openjpa.lib.rop.ResultObjectProvider;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.util.GeneralException;
import org.apache.openjpa.util.UserException;
import org.openspaces.core.executor.DistributedTask;
import org.openspaces.core.executor.Task;
import org.openspaces.core.executor.internal.ExecutorMetaDataProvider;
import org.openspaces.core.executor.internal.InternalDistributedSpaceTaskWrapper;
import org.openspaces.core.executor.internal.InternalSpaceTaskWrapper;
import org.openspaces.jpa.StoreManager;
import org.openspaces.remoting.scripting.Script;
import org.openspaces.remoting.scripting.ScriptingExecutor;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Executes native SQLQueries and task
 *
 * @author anna
 * @since 8.0.1
 */
public class StoreManagerSQLQuery extends AbstractStoreQuery {

    private static final long serialVersionUID = 1L;

    private StoreManager _store;

    public StoreManagerSQLQuery(StoreManager store) {
        this._store = store;
    }

    public StoreManager getStore() {
        return _store;
    }

    public boolean supportsParameterDeclarations() {
        return false;
    }

    public boolean supportsDataStoreExecution() {
        return true;
    }

    public Executor newDataStoreExecutor(ClassMetaData meta, boolean subclasses) {
        return new SQLExecutor();
    }

    public boolean requiresCandidateType() {
        return false;
    }

    public boolean requiresParameterDeclarations() {
        return false;
    }

    /**
     * Executes the filter as a SQL query.
     */
    protected static class SQLExecutor extends AbstractExecutor {

        private final String _executeCommand = "execute ?";
        private final ExecutorMetaDataProvider _executorMetaDataProvider = new ExecutorMetaDataProvider();

        public SQLExecutor() {
        }

        public int getOperation(StoreQuery q) {
            return (q.getContext().getCandidateType() != null || q.getContext().getResultType() != null
                    || q.getContext().getResultMappingName() != null || q.getContext().getResultMappingScope() != null) ? OP_SELECT : OP_UPDATE;
        }

        public ResultObjectProvider executeQuery(StoreQuery storeQuery, Object[] params, Range range) {
            // If candidate type was not set => task execution
            if (storeQuery.getContext().getCandidateType() == null) {
                return executeTaskOrScript(storeQuery, params);

                // Otherwise, SQLQuery execution
            } else {
                return executeSqlQuery(storeQuery, params);
            }
        }

        /**
         * Executes a GigaSpaces {@link SQLQuery} syntax query.
         *
         * @param storeQuery The query execute was called for.
         * @param params     The query's parameters.
         * @return Query execution result as a list.
         */
        @SuppressWarnings({"rawtypes", "deprecation", "unchecked"})
        private ResultObjectProvider executeSqlQuery(StoreQuery storeQuery, Object[] params) {
            try {

                // Throw an exception for a maybe common user mistake
                final String sql = StringUtils.trimToNull(storeQuery.getContext().getQueryString());
                if (_executeCommand.equalsIgnoreCase(sql))
                    throw new UserException(
                            "When specifying a candidate type - SQLQuery syntax should be used. " +
                                    "For task execution use the entityManager.createNativeQuery(String sql) method.");

                final QueryContext context = storeQuery.getContext();
                final Class<?> type = context.getCandidateType();
                final SQLQuery sqlQuery = new SQLQuery(type, context.getQueryString());
                if (params != null) {
                    for (int i = 0; i < params.length; i++) {
                        sqlQuery.setParameter(i + 1, params[i]);
                    }
                }
                final StoreManagerSQLQuery query = (StoreManagerSQLQuery) storeQuery;
                final StoreManager store = (StoreManager) query.getStore();
                final Object[] result = store.getConfiguration().getSpace().readMultiple(
                        sqlQuery, store.getCurrentTransaction(), Integer.MAX_VALUE, store.getConfiguration().getReadModifier());

                return new ListResultObjectProvider(CollectionUtils.toList(result));

            } catch (Exception e) {
                throw new GeneralException(e);
            }

        }

        /**
         * Executes a GigaSpaces {@link Task} or {@link Script}. Decision is made based on the
         * provided first parameter's instance.
         *
         * @param storeQuery The query which execute was called for.
         * @param params     Query execution parameters.
         * @return {@link Task} or {@link Script} execution result.
         */
        @SuppressWarnings({"rawtypes"})
        private ResultObjectProvider executeTaskOrScript(StoreQuery storeQuery, Object[] params) {
            QueryContext ctx = storeQuery.getContext();
            String sql = StringUtils.trimToNull(ctx.getQueryString());

            // Task execution SQL string must be: "execute ?"
            if (!_executeCommand.equalsIgnoreCase(sql))
                throw new UserException("Unsupported native query task/script execution syntax - " + sql);

            final StoreManagerSQLQuery query = (StoreManagerSQLQuery) storeQuery;

            if (params == null)
                throw new UserException("Execute task/script is not supported for non-parametrized query.");
            if (params.length != 1)
                throw new UserException("Illegal number of arguments <" + params.length + "> should be <1>.");

            if (params[0] instanceof Task)
                return executeTask((Task) params[0], query);

            if (params[0] instanceof Script)
                return executeScript((Script) params[0], query);

            throw new UserException("Illegal task/script execution parameter type - " + params[0].getClass().getName()
                    + ". " + Task.class.getName() + " or " + Script.class.getName() + " is expected.");

        }

        /**
         * Executes the provided GigaSpaces dynamic {@link Script}. Execution shouldn't be
         * transactional and if it is, the transaction isn't passed to the script.
         *
         * @param script The {@link Script} to execute.
         * @param query  The query which execute was called for.
         * @return {@link Script} execution returned value.
         */
        private ResultObjectProvider executeScript(Script script, StoreManagerSQLQuery query) {
            try {
                final ScriptingExecutor<?> executor = query.getStore().getConfiguration().getScriptingExecutorProxy();

                Object result = executor.execute(script);

                List<Object> resultList = new LinkedList<Object>();
                resultList.add(result);

                return new ListResultObjectProvider(resultList);
            } catch (Exception e) {
                throw new GeneralException(e);
            }
        }

        /**
         * Executes the provided GigaSpaces {@link Task}. This operation must be under a {@link
         * StoreManager} transaction.
         *
         * @param task  The {@link Task} to execute.
         * @param query The query which execute was called for.
         * @return {@link Task} execution returned value.
         */
        private ResultObjectProvider executeTask(Task<?> task, final StoreManagerSQLQuery query) {

            // Make sure there's an active store transaction
            // since we assume the task is changing data within the space
            final StoreContext context = query.getStore().getContext();
            context.getBroker().assertActiveTransaction();
            context.beginStore();

            final ISpaceProxy space = (ISpaceProxy) query.getStore().getConfiguration().getSpace();

            // Get routing from annotation
            final Object routing = _executorMetaDataProvider.findRouting(task);
            try {
                SpaceTask<?> spaceTask;
                if (task instanceof DistributedTask)
                    spaceTask = new InternalDistributedSpaceTaskWrapper((DistributedTask) task);
                else
                    spaceTask = new InternalSpaceTaskWrapper(task, routing);

                AsyncFuture<?> future = space.execute(spaceTask, routing, query.getStore().getCurrentTransaction(), null);
                Object taskResult = future.get();
                List resultList = new LinkedList();
                resultList.add(taskResult);
                return new ListResultObjectProvider(resultList);
            } catch (Exception e) {
                throw new GeneralException(e);
            }
        }

        public String[] getDataStoreActions(StoreQuery q, Object[] params, Range range) {
            return new String[]{q.getContext().getQueryString()};
        }

        public boolean isPacking(StoreQuery q) {
            return q.getContext().getCandidateType() == null;
        }

        /**
         * The given query is parsed to find the parameter tokens of the form <code>?n</code> which
         * is different than <code>?</code> tokens in actual SQL parameter tokens. These
         * <code>?n</code> style tokens are replaced in the query string by <code>?</code> tokens.
         *
         * During the token parsing, the ordering of the tokens is recorded. The given userParam
         * must contain parameter keys as Integer and the same Integers must appear in the tokens.
         */
        public Object[] toParameterArray(StoreQuery q, Map userParams) {
            if (userParams == null || userParams.isEmpty())
                return StoreQuery.EMPTY_OBJECTS;
            String sql = q.getContext().getQueryString();
            List<Integer> paramOrder = new ArrayList<Integer>();
            try {
                sql = substituteParams(sql, paramOrder);
            } catch (IOException ex) {
                throw new UserException(ex.getLocalizedMessage());
            }

            Object[] result = new Object[paramOrder.size()];
            int idx = 0;
            for (Integer key : paramOrder) {
                if (!userParams.containsKey(key))
                    throw new UserException("Missing parameter " + key + " in " + sql);
                result[idx++] = userParams.get(key);
            }
            // modify original JPA-style SQL to proper SQL
            q.getContext().getQuery().setQuery(sql);
            return result;
        }
    }

    /**
     * Utility method to substitute '?num' for parameters in the given SQL statement, and fill-in
     * the order of the parameter tokens
     */
    public static String substituteParams(String sql, List<Integer> paramOrder) throws IOException {
        // if there's no "?" parameter marker, then we don't need to
        // perform the parsing process
        if (sql.indexOf("?") == -1)
            return sql;

        paramOrder.clear();
        StreamTokenizer tok = new StreamTokenizer(new StringReader(sql));
        tok.resetSyntax();
        tok.quoteChar('\'');
        tok.wordChars('0', '9');
        tok.wordChars('?', '?');

        StringBuilder buf = new StringBuilder(sql.length());
        for (int ttype; (ttype = tok.nextToken()) != StreamTokenizer.TT_EOF; ) {
            switch (ttype) {
                case StreamTokenizer.TT_WORD:
                    // a token is a positional parameter if it starts with
                    // a "?" and the rest of the token are all numbers
                    if (tok.sval.startsWith("?")) {
                        buf.append("?");
                        String pIndex = tok.sval.substring(1);
                        if (pIndex.length() > 0) {
                            paramOrder.add(Integer.valueOf(pIndex));
                        } else { // or nothing
                            paramOrder.add(paramOrder.size() + 1);
                        }
                    } else
                        buf.append(tok.sval);
                    break;
                case '\'':
                    buf.append('\'');
                    if (tok.sval != null) {
                        buf.append(tok.sval);
                        buf.append('\'');
                    }
                    break;
                default:
                    buf.append((char) ttype);
            }
        }
        return buf.toString();
    }
}
