package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.internal.metadata.TypeDesc;
import com.gigaspaces.jdbc.model.table.OrderColumn;
import com.gigaspaces.query.aggregators.AggregationSet;
import com.gigaspaces.query.aggregators.OrderBy;
import com.gigaspaces.query.aggregators.OrderByAggregator;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;

import java.util.List;

public class SortOperator implements ISQLOperator {
    private final QueryTemplatePacketsHolder queryTemplatePacketsHolder;
    private final List<OrderColumn> orderColumns;


    public SortOperator(QueryTemplatePacketsHolder queryTemplatePacketsHolder, List<OrderColumn> orderColumns) {
        this.queryTemplatePacketsHolder = queryTemplatePacketsHolder;
        this.orderColumns = orderColumns;
    }

    @Override
    public QueryTemplatePacketsHolder build() {
        for (QueryTemplatePacket queryTemplatePacket : queryTemplatePacketsHolder.getQueryTemplatePackets().values()) {
            OrderByAggregator orderByAggregator = new OrderByAggregator();
            for (OrderColumn column : orderColumns) {
                if(queryTemplatePacket.getTypeDescriptor().getFixedPropertyPosition(column.getName()) != TypeDesc.NO_SUCH_PROPERTY){
                    orderByAggregator.orderBy(column.getName(), column.isAsc() ? OrderBy.ASC : OrderBy.DESC, column.isNullsLast());
                }
            }

            if( queryTemplatePacket.getAggregationSet() == null ) {
                AggregationSet aggregationSet = new AggregationSet().orderBy( orderByAggregator );
                queryTemplatePacket.setAggregationSet(aggregationSet);
            }
            else{
                queryTemplatePacket.getAggregationSet().add(orderByAggregator);
            }
        }
        queryTemplatePacketsHolder.setOrderColumns(orderColumns);
        return queryTemplatePacketsHolder;
    }

    public List<OrderColumn> getOrderColumns() {
        return orderColumns;
    }
}
