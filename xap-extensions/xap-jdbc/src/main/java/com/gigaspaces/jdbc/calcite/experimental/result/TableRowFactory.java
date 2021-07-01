package com.gigaspaces.jdbc.calcite.experimental.result;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.jdbc.calcite.experimental.SingleResultSupplier;
import com.gigaspaces.jdbc.calcite.experimental.model.ConcreteColumn;
import com.gigaspaces.jdbc.calcite.experimental.model.IQueryColumn;
import com.gigaspaces.jdbc.calcite.experimental.model.OrderColumn;


import java.util.List;

public class TableRowFactory {

    public static TableRow createTableRowFromSpecificColumns(List<IQueryColumn> columns,
                                                             List<OrderColumn> orderColumns,
                                                             List<ConcreteColumn> groupByColumns) {
        return new TableRow(columns, orderColumns, groupByColumns);
    }

    public static TableRow createTableRowFromIEntryPacket(IEntryPacket entryPacket, SingleResultSupplier tableContainer) {
        return new TableRow(entryPacket, tableContainer);
    }

}
