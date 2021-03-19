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
package com.gigaspaces.jdbc;

import com.gigaspaces.jdbc.exceptions.ExecutionException;
import com.gigaspaces.jdbc.exceptions.GenericJdbcException;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.jdbc.model.result.TableRow;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.ResponsePacket;
import com.j_spaces.jdbc.ResultEntry;
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
import java.util.Iterator;
import java.util.Set;

public class QueryHandler {

    private final Feature[] allowedFeatures = new Feature[] {Feature.select, Feature.explain/*, Feature.join, Feature.joinInner*/};

    public ResponsePacket handle(String query, IJSpace space) throws SQLException {

        try {
            Statement statement = CCJSqlParserUtil.parse(query);
            validateStatement(statement);
            return executeStatement(space, statement);
        } catch (JSQLParserException e) {
            throw new SQLException("Failed to parse query", e);
        } catch (GenericJdbcException e) {
            throw new SQLException("Failed to execute query", e);
        }
    }

    private ResponsePacket executeStatement(IJSpace space, Statement statement) {
        ResponsePacket packet = new ResponsePacket();

        statement.accept(new StatementVisitorAdapter() {
            @Override
            public void visit(ExplainStatement explainStatement) {
                QueryExecutor qE = new QueryExecutor(space);
                qE.setExplain(true);
                QueryResult res;
                try {
                    res = qE.execute(explainStatement.getStatement().getSelectBody());
                    packet.setResultEntry(convertEntriesToResultArrays(res));
                } catch (SQLException e) {
                    throw new ExecutionException("Failed to execute query", e);
                }
            }

            @Override
            public void visit(Select select) {
                QueryExecutor qE = new QueryExecutor(space);
                QueryResult res;
                try {
                    res = qE.execute(select.getSelectBody());
                    packet.setResultEntry(convertEntriesToResultArrays(res));
                } catch (SQLException e) {
                    throw new ExecutionException("Failed to execute query", e);
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

    public ResultEntry convertEntriesToResultArrays(QueryResult entries) {
        // Column (field) names and labels (aliases)
        int columns = entries.getQueryColumns().size();

        String[] fieldNames = entries.getQueryColumns().stream().map(QueryColumn::getName).toArray(String[]::new);
        String[] columnLabels = entries.getQueryColumns().stream().map(qC -> qC.getAlias() == null ? qC.getName() : qC.getAlias()).toArray(String[]::new);

        //the field values for the result
        Object[][] fieldValues = new Object[entries.size()][columns];

        Iterator<TableRow> iter = entries.iterator();

        int row = 0;

        while (iter.hasNext()) {
            TableRow entry = iter.next();

            int column = 0;
            for (int i = 0; i < columns; i++) {
                fieldValues[row][column++] = entry.getPropertyValue(i);
            }

            row++;
        }


        return new ResultEntry(
                fieldNames,
                columnLabels,
                null, //TODO
                fieldValues);
    }
}
