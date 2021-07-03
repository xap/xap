package com.gigaspaces.jdbc.calcite.experimental.result;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.jdbc.calcite.experimental.SingleResultSupplier;
import com.j_spaces.jdbc.query.IQueryResultSet;

import java.util.List;
import java.util.stream.Collectors;

public class ConcreteQueryResult extends QueryResult {
    private final SingleResultSupplier singleResultSupplier;
    private List<TableRow> rows;

    public ConcreteQueryResult(IQueryResultSet<IEntryPacket> res, SingleResultSupplier singleResultSupplier) {
        super(singleResultSupplier.getProjectedColumns());
        this.singleResultSupplier = singleResultSupplier;
        this.rows = res.stream().map(x -> TableRowFactory.createTableRowFromIEntryPacket(x, singleResultSupplier)).collect(Collectors.toList());
    }

    @Override
    public SingleResultSupplier getSingleResultSupplier() {
        return this.singleResultSupplier;
    }

    @Override
    public int size() {
        return this.rows.size();
    }

    @Override
    public void addRow(TableRow tableRow) {
        this.rows.add(tableRow);
    }

    @Override
    public List<TableRow> getRows() {
        return this.rows;
    }

    @Override
    public void setRows(List<TableRow> rows) {
        this.rows = rows;
    }
}
