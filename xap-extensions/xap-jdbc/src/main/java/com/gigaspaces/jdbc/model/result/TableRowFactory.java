package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.jdbc.model.table.ConcreteTableContainer;
import com.gigaspaces.jdbc.model.table.IQueryColumn;
import com.gigaspaces.jdbc.model.table.OrderColumn;
import com.gigaspaces.jdbc.model.table.TempTableContainer;

import java.util.List;

public class TableRowFactory {

    public static TableRow createTableRowFromSpecificColumns(List<IQueryColumn> columns,
                                                               List<OrderColumn> orderColumns) {
        return new TableRow(columns, orderColumns);
    }

    public static TableRow createTableRowFromIEntryPacket(IEntryPacket entryPacket, ConcreteTableContainer tableContainer) {
        return new TableRow(entryPacket, tableContainer);
    }

    public static TableRow createProjectedTableRow(TableRow row, TempTableContainer tempTableContainer) {
        return new TableRow(row, tempTableContainer);
    }
}
