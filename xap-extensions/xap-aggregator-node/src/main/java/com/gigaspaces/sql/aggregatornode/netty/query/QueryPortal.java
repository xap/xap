package com.gigaspaces.sql.aggregatornode.netty.query;

import com.gigaspaces.sql.aggregatornode.netty.exception.BreakingException;
import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;
import com.gigaspaces.sql.aggregatornode.netty.utils.ErrorCodes;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

class QueryPortal<T> implements Portal<T> {
    private final QueryProviderImpl queryProvider;
    private final String name;
    private final Statement stmt;
    private final RowDescription description;
    private final PortalCommand command;
    private final ThrowingSupplier<Iterator<T>, ProtocolException> op;

    private int processed;
    private Iterator<T> it;

    public QueryPortal(QueryProviderImpl queryProvider, String name, Statement stmt, PortalCommand command, int[] formatCodes, ThrowingSupplier<Iterator<T>, ProtocolException> op) {
        this.queryProvider = queryProvider;
        this.name = name;
        this.stmt = stmt;
        this.command = command;
        this.op = op;

        RowDescription desc = stmt.getDescription().getRowDescription();
        if (formatCodes.length == 0 || (formatCodes.length == 1 && formatCodes[0] == 0))
            description = desc;
        else {
            List<ColumnDescription> columns = desc.getColumns();
            List<ColumnDescription> newColumns = new ArrayList<>();
            for (int i = 0, rowDescColumnsSize = columns.size(); i < rowDescColumnsSize; i++) {
                ColumnDescription c = columns.get(i);
                newColumns.add(new ColumnDescription(
                        c.getName(),
                        c.getType(),
                        c.getTypeLen(),
                        c.getTypeModifier(),
                        formatCodes.length == 1 ? formatCodes[0] : formatCodes[i],
                        c.getTableId(),
                        c.getTableIndex()));
            }
            description = new RowDescription(newColumns);
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Statement getStatement() {
        return stmt;
    }

    @Override
    public RowDescription getDescription() {
        return description;
    }

    @Override
    public String tag() {
        if (command == PortalCommand.SELECT)
            return String.format("%s 0 %d", command.tag(), processed);
        return String.format("%s %d", command.tag(), processed);
    }

    @Override
    public void execute() throws ProtocolException {
        if (it != null)
            throw new BreakingException(ErrorCodes.PROTOCOL_VIOLATION, "Duplicate execute message");
        it = op.apply();
    }

    @Override
    public boolean hasNext() {
        return it != null && it.hasNext();
    }

    @Override
    public T next() {
        if (it == null)
            throw new NoSuchElementException();

        T next = it.next();
        processed++;
        return next;
    }

    @Override
    public void close() {
        queryProvider.closeP(name);
    }
}
