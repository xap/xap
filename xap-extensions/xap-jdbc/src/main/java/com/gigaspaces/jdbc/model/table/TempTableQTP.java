package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.jdbc.model.result.TableRow;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.UnionTemplatePacket;
import com.j_spaces.jdbc.builder.range.Range;

import java.util.Map;

public class TempTableQTP extends QueryTemplatePacket implements TempTableQTPI {

    public TempTableQTP(Range range) {
        getRanges().put(range.getPath(), range);
    }

    @Override
    public QueryTemplatePacket union(QueryTemplatePacket packet) {
        if (packet instanceof TempTableQTP) {
            UnionTemplatePacket composite = new UnionTempTableTemplatePacket();
            composite.add(this);
            composite.add(packet);
            return composite;
        } else {
            return super.union(packet);
        }
    }

    @Override
    public boolean matches(TableRow tableRow) {
        for (Map.Entry<String, Range> entry : getRanges().entrySet()) {
            String fieldName = entry.getKey();
            Range range = entry.getValue();

            Object fieldValue = tableRow.getPropertyValue(fieldName);
            if (!range.getPredicate().execute(fieldValue))
                return false;

        }
        return true;
    }

}
