package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.Range;
import net.sf.jsqlparser.expression.Expression;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class TableContainer {

    private final List<OrderColumn> orderColumns = new ArrayList<>();
    private final List<AggregationColumn> aggregationColumns = new ArrayList<>();
    private final List<ConcreteColumn> groupByColumns = new ArrayList<>();
    private Expression exprTree;

    public abstract QueryResult executeRead(QueryExecutionConfig config) throws SQLException;

    public abstract IQueryColumn addQueryColumn(String columnName, String columnAlias, boolean isVisible, int columnOrdinal);

    public abstract List<IQueryColumn> getVisibleColumns();

    public abstract Set<IQueryColumn> getInvisibleColumns();

    public List<IQueryColumn> getAllQueryColumns() {
        return Stream.concat(getVisibleColumns().stream(), getInvisibleColumns().stream()).collect(Collectors.toList());
    }

    public List<IQueryColumn> getSelectedColumns() {
        return Stream.concat(getVisibleColumns().stream(), getAggregationColumns().stream())
                .sorted().collect(Collectors.toList());
    }

    public abstract List<String> getAllColumnNames();

    public abstract String getTableNameOrAlias();

    public abstract void setLimit(Integer value);

    public abstract QueryTemplatePacket createQueryTemplatePacketWithRange(Range range);

    public abstract void setQueryTemplatePacket(QueryTemplatePacket queryTemplatePacket);

    public abstract Object getColumnValue(String columnName, Object value) throws SQLException;

    public abstract TableContainer getJoinedTable();

    public abstract void setJoinedTable(TableContainer joinedTable);

    public abstract QueryResult getQueryResult();

    public abstract void setJoined(boolean joined);

    public abstract boolean isJoined();

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

    public void addGroupByColumns(ConcreteColumn groupByColumn) {
        this.groupByColumns.add(groupByColumn);
    }

    public List<ConcreteColumn> getGroupByColumns() {
        return groupByColumns;
    }

    public List<OrderColumn> getOrderColumns() {
        return orderColumns;
    }

    public void addAggregationColumn(AggregationColumn aggregationColumn) {
        this.aggregationColumns.add(aggregationColumn);
    }

    public List<AggregationColumn> getAggregationColumns() {
        return aggregationColumns;
    }

    public boolean hasGroupByColumns() {
        return !this.groupByColumns.isEmpty();
    }

    public boolean hasAggregationFunctions() {
        return !this.aggregationColumns.isEmpty();
    }

    public boolean hasOrderColumns() {
        return !this.orderColumns.isEmpty();
    }

    protected void validate() {

        validateGroupBy();

        //TODO: block operation not supported -- see AggregationsUtil.convertAggregationResult
        if (hasAggregationFunctions() && hasOrderColumns()) {
            throw new IllegalArgumentException("Column [" + getOrderColumns().get(0).getAlias() + "] must appear in the " +
                    "GROUP BY clause or be used in an aggregate function");
        }
    }

    private void validateGroupBy() {
        if( hasGroupByColumns() ){
            List<ConcreteColumn> groupByColumns = getGroupByColumns();
            List<IQueryColumn> selectedColumns = getSelectedColumns();
            List<AggregationColumn> aggregationColumns = getAggregationColumns();

            List<String> selectedColumnNames = new ArrayList<>();
            List<String> aggregationColumnNames = new ArrayList<>();
            for( IQueryColumn selectedColumn : selectedColumns ){
                selectedColumnNames.add( selectedColumn.getName() );
            }
            for( AggregationColumn aggregationColumn : aggregationColumns ){
                aggregationColumnNames.add( aggregationColumn.getColumnName() );
            }

            List<String> invalidGroupByColumns = new ArrayList<>();
            for( ConcreteColumn groupByColumn : groupByColumns ){
                String groupByColumnName = groupByColumn.getName();
                if( !selectedColumnNames.contains( groupByColumnName ) &&
                        !aggregationColumnNames.contains( groupByColumnName ) ){
                    invalidGroupByColumns.add( groupByColumnName );
                }
            }

            if( !invalidGroupByColumns.isEmpty() ){
                throw new IllegalArgumentException( ( invalidGroupByColumns.size() == 1 ? "Column" : "Columns" ) +
                        Arrays.toString( invalidGroupByColumns.toArray( new String[0] ) ) + " must appear in the " +
                        "SELECT clause or be used in an aggregate function");
            }
        }
    }

}
