package com.gigaspaces.jdbc;

import com.gigaspaces.jdbc.calcite.GSOptimizer;
import com.gigaspaces.jdbc.calcite.GSOptimizerValidationResult;
import com.gigaspaces.jdbc.calcite.GSRelNode;
import com.gigaspaces.jdbc.calcite.RelNodePhysicalPlanHandler;
import com.gigaspaces.jdbc.exceptions.SQLExceptionWrapper;
import com.gigaspaces.jdbc.jsql.JsqlPhysicalPlanHandler;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.ResponsePacket;
import net.sf.jsqlparser.parser.feature.Feature;
import net.sf.jsqlparser.statement.ExplainStatement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitorAdapter;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.util.validation.ValidationContext;
import net.sf.jsqlparser.util.validation.ValidationException;
import net.sf.jsqlparser.util.validation.feature.FeaturesAllowed;
import net.sf.jsqlparser.util.validation.validator.StatementValidator;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;

public class QueryHandler {

    private final Feature[] allowedFeatures = new Feature[] {Feature.select, Feature.explain, Feature.exprLike, Feature.jdbcParameter, Feature.join, Feature.joinInner, Feature.joinLeft};

    public ResponsePacket handle(String query, IJSpace space, Object[] preparedValues) throws SQLException {
        GSRelNode calcitePlan = optimizeWithCalcite(query, space);
        return executeStatement(space, calcitePlan, preparedValues);

//        try {
//            Statement statement = CCJSqlParserUtil.parse(query);
//            validateStatement(statement);
//            return executeStatement(space, statement, preparedValues);
//        } catch (JSQLParserException e) {
//            throw new SQLException("Failed to parse query", e);
//        } catch (SQLExceptionWrapper e) {
//            throw e.getException();
//        } catch (GenericJdbcException | UnsupportedOperationException e) {
//            throw new SQLException(e.getMessage(), e);
//        }
    }

    public ResponsePacket executeStatement(IJSpace space, GSRelNode relNode, Object[] preparedValues) throws SQLException {
        ResponsePacket packet = new ResponsePacket();
        QueryExecutor qE = new QueryExecutor(space, preparedValues);
        RelNodePhysicalPlanHandler planHandler = new RelNodePhysicalPlanHandler(qE);
        qE = planHandler.prepareForExecution(relNode);
        QueryResult queryResult = qE.execute();
        packet.setResultEntry(queryResult.convertEntriesToResultArrays(null));
        return packet;
    }

    private ResponsePacket executeStatement(IJSpace space, Statement statement, Object[] preparedValues) {
        ResponsePacket packet = new ResponsePacket();

        statement.accept(new StatementVisitorAdapter() {
            @Override
            public void visit(ExplainStatement explainStatement) {
                QueryExecutionConfig config = new QueryExecutionConfig(true, explainStatement.getOption(ExplainStatement.OptionType.VERBOSE)!= null);
                QueryExecutor qE = new QueryExecutor(space, config, preparedValues);
                QueryResult res;
                try {
                    PhysicalPlanHandler<SelectBody> physicalPlanHandler = new JsqlPhysicalPlanHandler(qE);
                    qE = physicalPlanHandler.prepareForExecution(explainStatement.getStatement().getSelectBody());
                    res = qE.execute();
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
                    PhysicalPlanHandler<SelectBody> physicalPlanHandler = new JsqlPhysicalPlanHandler(qE);
                    qE = physicalPlanHandler.prepareForExecution(select.getSelectBody());
                    res = qE.execute();
                    packet.setResultEntry(res.convertEntriesToResultArrays(null));
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

    private static GSRelNode optimizeWithCalcite(String query, IJSpace space) {
        GSOptimizer optimizer = new GSOptimizer(space);
        SqlNode ast = optimizer.parse(query);
        GSOptimizerValidationResult validated = optimizer.validate(ast);
        GSRelNode physicalPlan = optimizer.optimize(validated.getValidatedAst());
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        RelWriterImpl writer = new RelWriterImpl(pw, SqlExplainLevel.EXPPLAN_ATTRIBUTES, false);
        physicalPlan.explain(writer);
        System.out.println(sw);

        return physicalPlan;
    }
}
