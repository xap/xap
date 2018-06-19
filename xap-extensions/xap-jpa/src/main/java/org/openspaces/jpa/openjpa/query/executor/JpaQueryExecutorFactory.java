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

package org.openspaces.jpa.openjpa.query.executor;

import org.apache.openjpa.kernel.exps.QueryExpressions;
import org.apache.openjpa.meta.ClassMetaData;

/**
 * A factory for creating a JpaQueryExecutor instance.
 *
 * @author idan
 * @since 8.0
 */
public class JpaQueryExecutorFactory {

    /**
     * Returns a new JpaQueryExecutor implementation instance based on the provided parameters. If
     * the provided expression tree requires aggregation - a JDBC executor is returned. Otherwise an
     * SQLQuery executor is returned.
     *
     * @param expression The expression tree.
     * @param cm         The queried class meta data.
     * @param parameters The user set parameters.
     * @return A JpaQueryExecutor implementation instance.
     */
    public static JpaQueryExecutor newExecutor(QueryExpressions expression, ClassMetaData cm, Object[] parameters) {
        if (expression.projections.length > 0)
            return new JpaJdbcQueryExecutor(expression, cm, parameters);
        return new JpaSqlQueryExecutor(expression, cm, parameters);
    }

}
