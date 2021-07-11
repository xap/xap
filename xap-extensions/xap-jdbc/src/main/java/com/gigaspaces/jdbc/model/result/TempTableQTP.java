package com.gigaspaces.jdbc.model.result;

import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.*;

import java.util.function.Predicate;

public class TempTableQTP extends QueryTemplatePacket {
    private Predicate<TableRow> predicate;

    public TempTableQTP() {
    }

    private TempTableQTP(Predicate<TableRow> predicate) {
        this.predicate = predicate;
    }

    public TempTableQTP(EqualValueRange range) {
         predicate = (tableRow) -> tableRow.getPropertyValue(range.getPath()).equals(range.getValue());
    }

    public TempTableQTP(Range range) {
        predicate = (tableRow -> range.getPredicate().execute(tableRow.getPropertyValue(range.getPath())));
    }

    @Override
    public QueryTemplatePacket and(QueryTemplatePacket template) {
        if (!(template instanceof TempTableQTP)) throw new UnsupportedOperationException("unsupported");
        return new TempTableQTP((tableRow) -> ((TempTableQTP) template).predicate.test(tableRow) && this.predicate.test(tableRow));
    }

    @Override
    public QueryTemplatePacket union(QueryTemplatePacket template) {
        if (!(template instanceof TempTableQTP)) throw new UnsupportedOperationException("unsupported");
        return new TempTableQTP((tableRow) -> ((TempTableQTP) template).predicate.test(tableRow) || this.predicate.test(tableRow));
    }

    public boolean eval(TableRow tableRow) {
        return predicate.test(tableRow);
    }
}
