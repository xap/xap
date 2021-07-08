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
package com.gigaspaces.jdbc.calcite.handlers;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.calcite.*;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.ExplainPlanQueryResult;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.ResponsePacket;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.validate.SqlValidatorException;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.Properties;

import static com.gigaspaces.jdbc.calcite.CalciteDefaults.isCalcitePropertySet;

public class CalciteQueryHandler {
    private boolean explainPlan;

    public ResponsePacket handle(String query, IJSpace space, Object[] preparedValues) throws SQLException {
        Properties customProperties = space.getURL().getCustomProperties();
        GSRelNode calcitePlan = optimizeWithCalcite(query, space, customProperties);
        return executeStatement(space, calcitePlan, preparedValues);
    }

    public ResponsePacket executeStatement(IJSpace space, GSRelNode relNode, Object[] preparedValues) throws SQLException {
        ResponsePacket packet = new ResponsePacket();
        QueryExecutionConfig queryExecutionConfig;
        if (explainPlan) {
            queryExecutionConfig = new QueryExecutionConfig(true, false);
        } else {
            queryExecutionConfig = new QueryExecutionConfig();
        }
        QueryExecutor qE = new QueryExecutor(space, queryExecutionConfig, preparedValues);
        SelectHandler selectHandler = new SelectHandler(qE);
        relNode.accept(selectHandler);
        QueryResult queryResult = qE.execute();
        if (explainPlan) {
            packet.setResultEntry(((ExplainPlanQueryResult) queryResult).convertEntriesToResultArrays(queryExecutionConfig));
        } else {
            packet.setResultEntry(queryResult.convertEntriesToResultArrays());
        }
        return packet;
    }

    private GSRelNode optimizeWithCalcite(String query, IJSpace space, Properties properties) throws SQLException {
        try {
            query = prepareQueryForCalcite(query, properties);
            GSOptimizer optimizer = new GSOptimizer(space);
            SqlNode ast = optimizer.parse(query);
            if (ast instanceof SqlExplain) {
                ast = ((SqlExplain) ast).getExplicandum();
                explainPlan = true;
            }
            GSOptimizerValidationResult validated = optimizer.validate(ast);
            GSRelNode physicalPlan = optimizer.optimize(validated.getValidatedAst());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            RelWriterImpl writer = new RelWriterImpl(pw, SqlExplainLevel.EXPPLAN_ATTRIBUTES, false);
            physicalPlan.explain(writer);
            System.out.println(sw);

            return physicalPlan;
        } catch (CalciteException calciteException) {
            Throwable cause = calciteException.getCause();
            if (cause != null) {
                if (cause instanceof SqlValidatorException) {
                    throw new SQLException("Query validation failed.", cause);
                }
            }
            throw calciteException; //runtime
        } catch (SqlParseException sqlParseException) {
            throw new SQLException("Query parsing failed.", sqlParseException);
        }
    }

    /**
     * Based on the system property or custom Space property, we parse the query
     * and adapt it to calcite notation. We do this only if the property is set,
     * in order to avoid performance penalty of String manipulation.
     */
    private static String prepareQueryForCalcite(String query, Properties properties) {
        //support for ; at end of statement - more than one statement is not supported.
        if (isCalcitePropertySet(CalciteDefaults.SUPPORT_SEMICOLON_SEPARATOR, properties)) {
            if (query.endsWith(";")) {
                query = query.replaceFirst(";", "");
            }
        }
        //support for != instead of <>
        if (isCalcitePropertySet(CalciteDefaults.SUPPORT_INEQUALITY, properties)) {
            query = query.replaceAll("!=", "<>");
        }

        return query;
    }
}

