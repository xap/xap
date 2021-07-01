package com.gigaspaces.jdbc.calcite.experimental;

import com.gigaspaces.jdbc.calcite.experimental.model.ConcreteColumn;
import com.gigaspaces.jdbc.calcite.experimental.model.IQueryColumn;
import com.gigaspaces.jdbc.calcite.experimental.model.OrderColumn;
import com.gigaspaces.jdbc.calcite.experimental.model.join.JoinInfo;
import com.gigaspaces.jdbc.calcite.experimental.result.QueryResult;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;


import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.Range;

import java.sql.SQLException;
import java.util.List;

public interface ResultSupplier {
    QueryResult executeRead(QueryExecutionConfig config) throws SQLException;

    Object[] getPreparedValues();

    QueryTemplatePacket createQueryTemplatePacketWithRange(Range range);

    JoinInfo getJoinInfo();

    Object getColumnValue(String column, Object value) throws SQLException;

    QueryResult getQueryResult();

    List<OrderColumn> getOrderColumns();

    boolean hasAggregationFunctions();

    List<ConcreteColumn> getGroupByColumns();

    String getTableNameOrAlias();

    List<IQueryColumn> getSelectedColumns();

    List<IQueryColumn> getAllQueryColumns();

    void setLimit(Integer value);

    boolean isDistinct();

    void setDistinct(boolean distinct);
}
