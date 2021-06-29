package com.gigaspaces.sql.aggregatornode.netty.query.pgcatalog;

import com.gigaspaces.jdbc.calcite.GSTable;
import com.gigaspaces.jdbc.exceptions.ColumnNotFoundException;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.jdbc.model.result.TempTableQTP;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.EqualValueRange;
import com.j_spaces.jdbc.builder.range.Range;
import com.j_spaces.jdbc.builder.range.SegmentRange;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.Collectors;

public abstract class PgCatalogTable extends TableContainer implements GSTable {
    protected final IJSpace space;
    protected final String name;
    protected final BitSet visible;
    protected final List<PgTableColumn> columns;

    private TempTableQTP queryTemplatePacket;

    public PgCatalogTable(IJSpace space, String name) {
        this.space = space;
        this.name = name;
        this.visible = new BitSet();
        this.columns = new ArrayList<>();
    }

    protected void init(PgCatalogSchema parent) {
        for (int i = 0; i < columns.size(); i++) {
            PgTableColumn column = columns.get(i);
            if (column.isVisible())
                visible.set(i);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    protected QueryResult executeReadInternal(QueryExecutionConfig config) throws SQLException {
        return new QueryResult((List) columns);
    }

    @Override
    public QueryResult executeRead(QueryExecutionConfig config) throws SQLException {
        QueryResult queryResult = executeReadInternal(config);
        if (queryTemplatePacket != null) {
            queryResult.filter(x -> queryTemplatePacket.eval(x));
        }
        return new QueryResult(getVisibleColumns(), queryResult);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        for (PgTableColumn column : columns) {
            builder.add(column.getName(), column.sqlType(typeFactory));
        }
        return builder.build();
    }

    @Override
    public TableContainer createTableContainer(IJSpace space) {
        return this;
    }

    @Override
    public List<String> getAllColumnNames() {
        return columns.stream().map(QueryColumn::getName).collect(Collectors.toList());
    }

    @Override
    public QueryColumn addQueryColumn(String columnName, String alias, boolean visible) {
        int idx = -1;
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equalsIgnoreCase(columnName)) {
                idx = i;
                break;
            }
        }

        if (idx == -1)
            throw new ColumnNotFoundException("Could not find column with name [" + columnName + "]");

        if (visible)
            this.visible.set(idx);
        else
            this.visible.clear(idx);

        return columns.get(idx);
    }

    @Override
    public String getTableNameOrAlias() {
        return name;
    }

    @Override
    public List<QueryColumn> getVisibleColumns() {
        return visible.stream()
                .mapToObj(columns::get)
                .map(QueryColumn.class::cast)
                .collect(Collectors.toList());
    }

    @Override
    public void setQueryTemplatePacket(QueryTemplatePacket queryTemplatePacket) {
        this.queryTemplatePacket = ((TempTableQTP) queryTemplatePacket);
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
    public Object getColumnValue(String columnName, Object value) {
        return value;
    }

    @Override
    public TableContainer getJoinedTable() {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public void setJoinedTable(TableContainer joinedTable) {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public QueryResult getQueryResult() {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public void setJoined(boolean joined) {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public boolean isJoined() {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public boolean hasColumn(String columnName) {
        return getAllColumnNames().contains(columnName);
    }

    @Override
    public JoinInfo getJoinInfo() {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public void setJoinInfo(JoinInfo joinInfo) {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public boolean checkJoinCondition() {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    // Default implementation. Override if you have statistics.
    public Statistic getStatistic() {
        return Statistics.UNKNOWN;
    }

    public Schema.TableType getJdbcTableType() {
        return Schema.TableType.TABLE;
    }

    public <C> C unwrap(Class<C> aClass) {
        if (aClass.isInstance(this)) {
            return aClass.cast(this);
        }
        return null;
    }

    @Override public boolean isRolledUp(String column) {
        return false;
    }

    @Override public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
        return true;
    }
}
