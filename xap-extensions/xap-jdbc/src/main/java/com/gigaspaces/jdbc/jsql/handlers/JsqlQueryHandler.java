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
package com.gigaspaces.jdbc.jsql.handlers;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.exceptions.SQLExceptionWrapper;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.ExplainPlanQueryResult;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.ResponsePacket;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.feature.Feature;
import net.sf.jsqlparser.statement.ExplainStatement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitorAdapter;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.validation.ValidationContext;
import net.sf.jsqlparser.util.validation.ValidationException;
import net.sf.jsqlparser.util.validation.feature.FeaturesAllowed;
import net.sf.jsqlparser.util.validation.validator.StatementValidator;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;


public class JsqlQueryHandler {


    private final Feature[] allowedFeatures = new Feature[]{Feature.select, Feature.explain, Feature.exprLike,
            Feature.jdbcParameter, Feature.join, Feature.joinInner, Feature.joinLeft, Feature.orderBy,
            Feature.orderByNullOrdering, Feature.function, Feature.selectGroupBy, Feature.distinct};

    public ResponsePacket handle(String query, IJSpace space, Object[] preparedValues) throws SQLException {

        try {
            Statement statement = CCJSqlParserUtil.parse(query);

            validateStatement(statement);
            return executeStatement(space, statement, preparedValues);
        } catch (JSQLParserException e) {
            throw new SQLException("Failed to parse query", e);
        }
    }

    private ResponsePacket executeStatement(IJSpace space, Statement statement, Object[] preparedValues) {
        ResponsePacket packet = new ResponsePacket();

        statement.accept(new StatementVisitorAdapter() {
            @Override
            public void visit(ExplainStatement explainStatement) {
                QueryExecutionConfig config = new QueryExecutionConfig(true, explainStatement.getOption(ExplainStatement.OptionType.VERBOSE) != null);
                QueryExecutor qE = new QueryExecutor(space, config, preparedValues);
                ExplainPlanQueryResult res;
                try {
                    com.gigaspaces.jdbc.jsql.handlers.SelectHandler physicalPlanHandler = new com.gigaspaces.jdbc.jsql.handlers.SelectHandler(qE);
                    qE = physicalPlanHandler.prepareForExecution(explainStatement.getStatement().getSelectBody());
                    res = (ExplainPlanQueryResult) qE.execute();
                    packet.setResultEntry(res.convertEntriesToResultArrays(config));
                } catch (SQLException e) {
                    throw new SQLExceptionWrapper(e);
                }
            }

            @Override
            public void visit(Select select) {
                QueryExecutor qE = new QueryExecutor(space, preparedValues);
                QueryResult res;
                try {
                    com.gigaspaces.jdbc.jsql.handlers.SelectHandler physicalPlanHandler = new com.gigaspaces.jdbc.jsql.handlers.SelectHandler(qE);
                    qE = physicalPlanHandler.prepareForExecution(select.getSelectBody());
                    res = qE.execute();
                    packet.setResultEntry(res.convertEntriesToResultArrays());
                } catch (SQLException e) {
                    throw new SQLExceptionWrapper(e);
                }
            }
        });

        return packet;
    }

    private void validateStatement(Statement statement) throws SQLException {
        StatementValidator validator = new StatementValidator();
        validator.setContext(new ValidationContext()
                .setCapabilities(Collections.singletonList(new FeaturesAllowed(allowedFeatures))));
        validator.validate(statement);
        if (validator.getValidationErrors().size() != 0) {
            for (Set<ValidationException> validationCapability : validator.getValidationErrors().values()) {
                throw new SQLException("Validation error", validationCapability.iterator().next());
            }
        }
    }

}
