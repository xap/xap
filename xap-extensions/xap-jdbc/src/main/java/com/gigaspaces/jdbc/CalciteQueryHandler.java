package com.gigaspaces.jdbc;

import com.gigaspaces.jdbc.calcite.GSOptimizer;
import com.gigaspaces.jdbc.calcite.GSRelNode;
import com.gigaspaces.jdbc.calcite.RelNodePhysicalPlanHandler;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.utils.Pair;
import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.ResponsePacket;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;

public class CalciteQueryHandler {

    public Pair<RelDataType, RelDataType> extractTypes(String query, IJSpace space) {
        GSOptimizer optimizer = new GSOptimizer(space);
        SqlNode ast = optimizer.parse(query);
        ast = optimizer.validate(ast);
        return new Pair<>(optimizer.extractParameterType(ast), optimizer.extractRowType(ast));
    }

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

    private ResponsePacket executeStatement(IJSpace space, GSRelNode relNode, Object[] preparedValues) throws SQLException {
        ResponsePacket packet = new ResponsePacket();
        QueryExecutor qE = new QueryExecutor(space, preparedValues);
        RelNodePhysicalPlanHandler planHandler = new RelNodePhysicalPlanHandler(qE);
        qE = planHandler.prepareForExecution(relNode);
        QueryResult queryResult = qE.execute();
        packet.setResultEntry(queryResult.convertEntriesToResultArrays(null));
        return packet;
    }

    private static GSRelNode optimizeWithCalcite(String query, IJSpace space) {
        GSOptimizer optimizer = new GSOptimizer(space);
        SqlNode ast = optimizer.parse(query);
        SqlNode validatedAst = optimizer.validate(ast);
        RelNode logicalPlan = optimizer.createLogicalPlan(validatedAst);
        GSRelNode physicalPlan = optimizer.createPhysicalPlan(logicalPlan);
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        RelWriterImpl writer = new RelWriterImpl(pw, SqlExplainLevel.EXPPLAN_ATTRIBUTES, false);
        physicalPlan.explain(writer);
        System.out.println(sw);

        return physicalPlan;
    }
}
