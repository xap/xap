package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.table.OrderColumn;
import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.Range;

import java.util.*;

public class QueryTemplatePacketsHolder {
    private final Map<String, QueryTemplatePacket> queryTemplatePackets = new LinkedHashMap<>();
    private final Map<String, JoinInfo> joinInfoMap = new HashMap<>();
    private final Object[] preparedValues;
    private final IJSpace space;
    private List<OrderColumn> orderColumns = new ArrayList<>();
    private Integer limit = Integer.MAX_VALUE;


    public QueryTemplatePacketsHolder(Object[] preparedValues, IJSpace space) {
        this.preparedValues = preparedValues;
        this.space = space;
    }

    public Map<String, QueryTemplatePacket> getQueryTemplatePackets() {
        return queryTemplatePackets;
    }

    public void addQueryTemplatePacket(QueryTemplatePacket queryTemplatePacket) {
        queryTemplatePackets.put(queryTemplatePacket.getTypeDescriptor().getTypeName(), queryTemplatePacket);
    }

//    public void addJoinInfo(String tableName, JoinInfo joinInfo) {
//        joinInfoMap.put(tableName, joinInfo);
//    }
//
//    public Map<String, JoinInfo> getJoinInfoMap() {
//        return joinInfoMap;
//    }

    public boolean addRangeToJoinInfo(String tableName, Range range) {
        JoinInfo joinInfo = joinInfoMap.get(tableName);
        if (joinInfo != null) {
            joinInfo.insertRangeToJoinInfo(range);
        }
        return joinInfo != null;
    }

    public Object[] getPreparedValues() {
        return preparedValues;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public Map<String, JoinInfo> getJoinInfoMap() {
        return joinInfoMap;
    }

    public IJSpace getSpace() {
        return space;
    }

    public List<OrderColumn> getOrderColumns() {
        return orderColumns;
    }

    public void setOrderColumns(List<OrderColumn> orderColumns) {
        this.orderColumns = orderColumns;
    }
}
