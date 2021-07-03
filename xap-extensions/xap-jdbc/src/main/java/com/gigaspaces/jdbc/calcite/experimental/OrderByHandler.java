package com.gigaspaces.jdbc.calcite.experimental;

import com.gigaspaces.jdbc.calcite.GSSort;
import com.gigaspaces.jdbc.calcite.experimental.model.IQueryColumn;
import com.gigaspaces.jdbc.calcite.experimental.model.OrderColumn;
import org.apache.calcite.rel.RelFieldCollation;

public class OrderByHandler {
    private final ResultSupplier resultSupplier;
    private final GSSort gsSort;

    public OrderByHandler(ResultSupplier resultSupplier, GSSort gsSort) {
        this.resultSupplier = resultSupplier;
        this.gsSort = gsSort;
    }

    public void apply() {
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
