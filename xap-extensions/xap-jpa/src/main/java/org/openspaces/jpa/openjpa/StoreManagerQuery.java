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


import org.apache.openjpa.kernel.ExpressionStoreQuery;
import org.apache.openjpa.kernel.StoreQuery;
import org.apache.openjpa.kernel.exps.ExpressionFactory;
import org.apache.openjpa.kernel.exps.ExpressionParser;
import org.apache.openjpa.kernel.exps.QueryExpressions;
import org.apache.openjpa.lib.rop.ResultObjectProvider;
import org.apache.openjpa.meta.ClassMetaData;
import org.openspaces.jpa.StoreManager;
import org.openspaces.jpa.openjpa.query.QueryExpressionFactory;
import org.openspaces.jpa.openjpa.query.executor.JpaQueryExecutor;
import org.openspaces.jpa.openjpa.query.executor.JpaQueryExecutorFactory;

/**
 * Executes select queries. update & delete SQL operations are performed in memory and executed on
 * commit. The provided OpenJPA expression tree is translated to either GigaSpaces' SQLQuery or
 * JDBC.
 *
 * @author idan
 * @since 8.0
 */
public class StoreManagerQuery extends ExpressionStoreQuery {

    private static final long serialVersionUID = 1L;

    private StoreManager _store;

    public StoreManagerQuery(ExpressionParser parser, StoreManager store) {
        super(parser);
        _store = store;
    }

    @Override
    public boolean supportsDataStoreExecution() {
        return true;
    }

    /**
     * Execute the given expression against the given candidate extent.
     */
    protected ResultObjectProvider executeQuery(Executor ex, ClassMetaData classMetaData,
                                                ClassMetaData[] types, boolean subClasses, ExpressionFactory[] factories,
                                                QueryExpressions[] expressions, Object[] parameters, Range range) {
        final JpaQueryExecutor executor = JpaQueryExecutorFactory.newExecutor(expressions[0], classMetaData, parameters);
        try {
            return executor.execute(_store);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }


    /**
     * Return the commands that will be sent to the datastore in order to execute the query,
     * typically in the database's native language.
     *
     * @param base       the base type the query should match
     * @param types      the independent candidate types
     * @param subclasses true if subclasses should be included in the results
     * @param facts      the expression factory used to build the query for each base type
     * @param parsed     the parsed query values
     * @param params     parameter values, or empty array
     * @param range      result range
     * @return a textual description of the query to execute
     */
    protected String[] getDataStoreActions(ClassMetaData base,
                                           ClassMetaData[] types, boolean subclasses, ExpressionFactory[] facts,
                                           QueryExpressions[] parsed, Object[] params, Range range) {
        return StoreQuery.EMPTY_STRINGS;
    }

    /**
     * Return the assignable types for the given metadata whose expression trees must be compiled
     * independently.
     */
    protected ClassMetaData[] getIndependentExpressionCandidates
    (ClassMetaData type, boolean subclasses) {
        return new ClassMetaData[]{type};
    }

    /**
     * Return an {@link ExpressionFactory} to use to create an expression to be executed against an
     * extent. Each factory will be used to compile one filter only. The factory must be cachable.
     */
    protected ExpressionFactory getExpressionFactory(ClassMetaData type) {
        return new QueryExpressionFactory(_store);
    }


}
