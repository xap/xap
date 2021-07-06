package com.gigaspaces.jdbc.calcite.experimental.handlers;

import com.gigaspaces.jdbc.calcite.GSSort;
import com.gigaspaces.jdbc.calcite.experimental.ResultSupplier;
import com.gigaspaces.jdbc.calcite.experimental.model.IQueryColumn;
import com.gigaspaces.jdbc.calcite.experimental.model.OrderColumn;
import org.apache.calcite.rel.RelFieldCollation;

public class OrderByHandler {
    private static OrderByHandler _instance;
    public static OrderByHandler instance(){
        if(_instance == null){
            _instance = new OrderByHandler();
        }
        return _instance;
    }

    private OrderByHandler() {
    }

    public void apply(ResultSupplier resultSupplier, GSSort gsSort) {
        for (RelFieldCollation relCollation : gsSort.getCollation().getFieldCollations()) {
            int fieldIndex = relCollation.getFieldIndex();
            RelFieldCollation.Direction direction = relCollation.getDirection();
            RelFieldCollation.NullDirection nullDirection = relCollation.nullDirection;
            String columnName = gsSort.getRowType().getFieldNames().get(fieldIndex);
            IQueryColumn physicalColumn = resultSupplier.getOrCreatePhysicalColumn(columnName);
            OrderColumn orderColumn = new OrderColumn(physicalColumn, !direction.isDescending(),
                    nullDirection == RelFieldCollation.NullDirection.LAST);
            resultSupplier.addOrderColumn(orderColumn);
        }
    }
}
