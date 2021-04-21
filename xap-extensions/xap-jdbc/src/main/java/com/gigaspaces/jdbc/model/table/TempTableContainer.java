package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.jdbc.exceptions.ColumnNotFoundException;
import com.gigaspaces.jdbc.explainplan.SubqueryExplainPlan;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.ExplainPlanResult;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.Range;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TempTableContainer extends TableContainer {
    private final QueryResult tableResult;
    private final String alias;
    private TableContainer joinedTable;
    private final List<QueryColumn> visibleColumns = new ArrayList<>();
    private final List<QueryColumn> tableColumns = new ArrayList<>();

    public TempTableContainer(QueryResult tableResult, String alias) {
        this.tableResult = tableResult;
        this.alias = alias;
        if (tableResult instanceof ExplainPlanResult) {
            tableColumns.addAll(((ExplainPlanResult) tableResult).getVisibleColumns());
        } else {
            tableColumns.addAll(tableResult.getQueryColumns());
        }
    }

    @Override
    public QueryResult executeRead(QueryExecutionConfig config) {
        if (config.isExplainPlan()) {
            ExplainPlanResult explainResult = ((ExplainPlanResult) tableResult);
            SubqueryExplainPlan subquery = new SubqueryExplainPlan(visibleColumns, (alias == null ? config.getTempTableNameGenerator().generate() : alias), explainResult.getExplainPlanInfo());
            return new ExplainPlanResult(visibleColumns, subquery, this);
        }
        return new QueryResult(visibleColumns, tableResult);
    }

    @Override
    public QueryColumn addQueryColumn(String columnName, String alias, boolean visible) {
        QueryColumn queryColumn = tableColumns.stream().filter(qc -> qc.getName().equalsIgnoreCase(columnName)).findFirst().orElseThrow(() -> new ColumnNotFoundException("Could not find column with name [" + columnName + "]"));
        visibleColumns.add(queryColumn);
        return queryColumn;
    }

    @Override
    public List<QueryColumn> getVisibleColumns() {
        return visibleColumns;
    }

    @Override
    public List<String> getAllColumnNames() {
        return tableColumns.stream().map(QueryColumn::getName).collect(Collectors.toList());
    }

    @Override
    public String getTableNameOrAlias() {
        throw new UnsupportedOperationException("Not supported yet!");
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
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public void setQueryTemplatePacket(QueryTemplatePacket queryTemplatePacket) {
        throw new UnsupportedOperationException("Not supported yet!");
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
    public Object getColumnValue(String columnName, Object value) {
        return value;
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
