package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.internal.utils.ObjectConverter;
import com.gigaspaces.jdbc.exceptions.ColumnNotFoundException;
import com.gigaspaces.jdbc.explainplan.SubqueryExplainPlan;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.result.ExplainPlanResult;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.jdbc.model.result.TempTableQTP;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.EqualValueRange;
import com.j_spaces.jdbc.builder.range.Range;
import com.j_spaces.jdbc.builder.range.SegmentRange;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TempTableContainer extends TableContainer {
    private final String alias;
    private final QueryResult tableResult;
    private TempTableQTP queryTemplatePacket;
    private final List<QueryColumn> visibleColumns = new ArrayList<>();
    private final List<QueryColumn> tableColumns = new ArrayList<>();
    private final List<String> allColumnNamesSorted;
    private TableContainer joinedTable;


    public TempTableContainer(QueryResult tableResult, String alias) {
        this.tableResult = tableResult;
        this.alias = alias;
        if (tableResult instanceof ExplainPlanResult) {
            tableColumns.addAll(((ExplainPlanResult) tableResult).getVisibleColumns());
        } else {
            tableColumns.addAll(tableResult.getQueryColumns());
        }

        allColumnNamesSorted = tableColumns.stream().map(QueryColumn::getName).collect(Collectors.toList());
    }

    @Override
    public QueryResult executeRead(QueryExecutionConfig config) {
        if (config.isExplainPlan()) {
            ExplainPlanResult explainResult = ((ExplainPlanResult) tableResult);
            SubqueryExplainPlan subquery = new SubqueryExplainPlan(visibleColumns, (alias == null ? config.getTempTableNameGenerator().generate() : alias), explainResult.getExplainPlanInfo(), getExprTree());
            return new ExplainPlanResult(visibleColumns, subquery, this);
        }
        if (queryTemplatePacket != null)
            tableResult.filter(x -> queryTemplatePacket.eval(x));
        return new QueryResult(visibleColumns, tableResult);
    }

    @Override
    public QueryColumn addQueryColumn(String columnName, String alias, boolean visible) {
        QueryColumn queryColumn = tableColumns.stream().filter(qc -> qc.getName().equalsIgnoreCase(columnName)).findFirst().orElseThrow(() -> new ColumnNotFoundException("Could not find column with name [" + columnName + "]"));
        if (visible) visibleColumns.add(queryColumn);
        return queryColumn;
    }

    @Override
    public List<QueryColumn> getVisibleColumns() {
        return visibleColumns;
    }

    @Override
    public List<String> getAllColumnNames() {
        return allColumnNamesSorted;
    }

    @Override
    public String getTableNameOrAlias() {
        return alias;
    }

    @Override
    public TableContainer getJoinedTable() {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public void setJoinedTable(TableContainer joinedTable) {
        this.joinedTable = joinedTable;
    }

    @Override
    public QueryResult getQueryResult() {
        return tableResult;
    }

    @Override
    public void setLimit(Integer value) {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public QueryTemplatePacket createQueryTemplatePacketWithRange(Range range) {
        addQueryColumn(range.getPath(), null, false);
        if (range instanceof EqualValueRange) {
            return new TempTableQTP((EqualValueRange) range);
        } else if (range instanceof SegmentRange) {
            return new TempTableQTP((SegmentRange) range);
        } else {
            throw new UnsupportedOperationException("Range: " + range);
        }
    }

    @Override
    public void setQueryTemplatePacket(QueryTemplatePacket queryTemplatePacket) {
        this.queryTemplatePacket = ((TempTableQTP) queryTemplatePacket);
    }

    @Override
    public void setJoined(boolean joined) {

    }

    @Override
    public boolean isJoined() {
        return false;
    }

    @Override
    public boolean hasColumn(String columnName) {
        return getAllColumnNames().contains(columnName);
    }

    @Override
    public Object getColumnValue(String columnName, Object value) throws SQLException {
        QueryColumn column = tableColumns.stream().filter(queryColumn -> queryColumn.getName().equalsIgnoreCase(columnName)).findFirst()
                .orElseThrow(() -> new ColumnNotFoundException("Could not find column with name [" + columnName + "]"));
        //todo change table column into map and review with Yohana
        return ObjectConverter.convert(value, column.getPropertyType());
    }

    @Override
    public JoinInfo getJoinInfo() {
        return null;
    }

    @Override
    public void setJoinInfo(JoinInfo joinInfo) {

    }

    @Override
    public boolean checkJoinCondition() {
        return false;
    }
}
