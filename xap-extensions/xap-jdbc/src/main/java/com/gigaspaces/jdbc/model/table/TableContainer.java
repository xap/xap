package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.Range;
import net.sf.jsqlparser.expression.Expression;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class TableContainer {

    private final List<OrderColumn> orderColumns = new ArrayList<>();
    private final List<AggregationFunction> aggregationFunctionColumns = new ArrayList<>();
    private Expression exprTree;

    public abstract QueryResult executeRead(QueryExecutionConfig config) throws SQLException;

    public abstract QueryColumn addQueryColumn(String columnName, String alias, boolean visible);

    public abstract QueryColumn addQueryColumn(AggregationFunction aggregationFunction);

    public abstract List<QueryColumn> getVisibleColumns(); //TODO create one for "getQueryColumn"?!?!

    public abstract List<String> getAllColumnNames();

    public abstract String getTableNameOrAlias();

    public abstract void setLimit(Integer value);

    public abstract QueryTemplatePacket createQueryTemplatePacketWithRange(Range range);

    public abstract void setQueryTemplatePacket(QueryTemplatePacket queryTemplatePacket);

    public abstract Object getColumnValue(String columnName, Object value) throws SQLException;

    public abstract TableContainer getJoinedTable();

    public abstract void setJoinedTable(TableContainer joinedTable);

    public abstract QueryResult getQueryResult();

    public abstract boolean isJoined();

    public abstract void setJoined(boolean joined);

    public abstract boolean hasColumn(String columnName);

    public abstract JoinInfo getJoinInfo();

    public abstract void setJoinInfo(JoinInfo joinInfo);

    public abstract boolean checkJoinCondition();

    public void setExpTree(Expression value) {
        this.exprTree = value;
    }

    public Expression getExprTree() {
        return exprTree;
    }

    public void addOrderColumns(OrderColumn orderColumn) {
        this.orderColumns.add(orderColumn);
    }

    public List<OrderColumn> getOrderColumns() {
        return orderColumns;
    }

    public void addAggregationFunctionColumn(AggregationFunction aggregationFunction) {
        this.aggregationFunctionColumns.add(aggregationFunction);
    }

    public List<AggregationFunction> getAggregationFunctionColumns() {
        return aggregationFunctionColumns;
    }

    public boolean hasAggregationFunctions() {
        return !this.aggregationFunctionColumns.isEmpty();
    }

    public boolean hasOrderColumns() {
        return !this.orderColumns.isEmpty();
    }


    protected void validateAggregationFunction() {
        List<QueryColumn> trulyVisibleColumns =
                getVisibleColumns().stream().filter(QueryColumn::isVisible).collect(Collectors.toList());
        //TODO: block until supports of group by implementation.
        if(hasAggregationFunctions() && !trulyVisibleColumns.isEmpty()) {
            throw new IllegalArgumentException("Column [" + trulyVisibleColumns.get(0) + "] must appear in the " +
                    "GROUP BY clause or be used in an aggregate function");
        }
        //TODO: block operation not supported -- see AggregationsUtil.convertAggregationResult
        if(hasAggregationFunctions() && hasOrderColumns()) {
            throw new IllegalArgumentException("Column [" + getOrderColumns().get(0).getNameOrAlias() + "] must appear in the " +
                    "GROUP BY clause or be used in an aggregate function");
        }
    }

}
