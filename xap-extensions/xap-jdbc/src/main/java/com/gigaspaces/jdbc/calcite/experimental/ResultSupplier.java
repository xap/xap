package com.gigaspaces.jdbc.calcite.experimental;

import com.gigaspaces.jdbc.calcite.experimental.model.*;
import com.gigaspaces.jdbc.calcite.experimental.model.join.JoinInfo;
import com.gigaspaces.jdbc.calcite.experimental.result.QueryResult;
import com.gigaspaces.jdbc.exceptions.ColumnNotFoundException;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;


import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.Range;

import java.sql.SQLException;
import java.util.List;

public interface ResultSupplier{
    QueryResult executeRead(QueryExecutionConfig config) throws SQLException;

    Object[] getPreparedValues();

    QueryTemplatePacket createQueryTemplatePacketWithRange(Range range);

    JoinInfo getJoinInfo();

    Object getColumnValue(String column, Object value) throws SQLException;

    QueryResult getQueryResult();

    List<OrderColumn> getOrderColumns();

    boolean hasAggregationFunctions();

    List<PhysicalColumn> getGroupByColumns();

    String getTableNameOrAlias();

    List<IQueryColumn> getProjectedColumns();

    List<IQueryColumn> getAllQueryColumns();

    void setLimit(Integer value);

    boolean isDistinct();

    void setDistinct(boolean distinct);

    IQueryColumn getColumnByName(String column) throws ColumnNotFoundException;

    boolean hasColumn(String column);

    void addProjection(IQueryColumn projection);

    IQueryColumn getOrCreatePhysicalColumn(String physicalColumn) throws ColumnNotFoundException;

    void addAggregationColumn(AggregationColumn aggregationColumn);

    void addOrderColumn(OrderColumn orderColumn);

    void addFunctionColumn(FunctionColumn functionColumn);

    Class<?> getReturnType(String columnName) throws SQLException;

    void setQueryTemplatePacket(QueryTemplatePacket queryTemplatePacket);

    List<AggregationColumn> getAggregationColumns();

    void addGroupByColumn(PhysicalColumn physicalColumn);

    boolean clearProjections();

    boolean checkJoinCondition();
}
