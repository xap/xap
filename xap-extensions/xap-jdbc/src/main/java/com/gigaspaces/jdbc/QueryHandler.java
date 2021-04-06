package com.gigaspaces.jdbc;

import com.gigaspaces.jdbc.exceptions.ExecutionException;
import com.gigaspaces.jdbc.exceptions.GenericJdbcException;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
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

    private final Feature[] allowedFeatures = new Feature[] {Feature.select, Feature.explain, Feature.exprLike, Feature.jdbcParameter, Feature.join, Feature.joinInner};

    public ResponsePacket handle(String query, IJSpace space, Object[] preparedValues) throws SQLException {

        try {
            Statement statement = CCJSqlParserUtil.parse(query);
            validateStatement(statement);
            return executeStatement(space, statement, preparedValues);
        } catch (JSQLParserException e) {
            throw new SQLException("Failed to parse query", e);
        } catch (GenericJdbcException e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    private ResponsePacket executeStatement(IJSpace space, Statement statement, Object[] preparedValues) {
        ResponsePacket packet = new ResponsePacket();

        statement.accept(new StatementVisitorAdapter() {
            @Override
            public void visit(ExplainStatement explainStatement) {
                QueryExecutionConfig context = new QueryExecutionConfig(true, explainStatement.getOption(ExplainStatement.OptionType.VERBOSE)!= null);
                QueryExecutor qE = new QueryExecutor(space, context, preparedValues);
                QueryResult res;
                try {
                    res = qE.execute(explainStatement.getStatement().getSelectBody());
                    packet.setResultEntry(convertEntriesToResultArrays(res));
                } catch (SQLException e) {
                    throw new ExecutionException(e.getMessage(), e.getCause());
                }
            }

            @Override
            public void visit(Select select) {
                QueryExecutor qE = new QueryExecutor(space, preparedValues);
                QueryResult res;
                try {
                    res = qE.execute(select.getSelectBody());
                    packet.setResultEntry(convertEntriesToResultArrays(res));
                } catch (SQLException e) {
                    throw new ExecutionException(e.getMessage(), e.getCause());
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

    public ResultEntry convertEntriesToResultArrays(QueryResult queryResult) {
        // Column (field) names and labels (aliases)
        int columns = queryResult.getQueryColumns().size();

        String[] fieldNames = queryResult.getQueryColumns().stream().map(QueryColumn::getName).toArray(String[]::new);
        String[] columnLabels = queryResult.getQueryColumns().stream().map(qC -> qC.getAlias() == null ? qC.getName() : qC.getAlias()).toArray(String[]::new);

        //the field values for the result
        Object[][] fieldValues = new Object[queryResult.size()][columns];


        int row = 0;

        while (queryResult.next()) {
            TableRow entry = queryResult.getCurrent();
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
