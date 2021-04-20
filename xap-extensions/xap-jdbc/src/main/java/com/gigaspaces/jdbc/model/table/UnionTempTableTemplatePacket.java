package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.jdbc.model.result.TableRow;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.UnionTemplatePacket;

import java.util.ArrayList;

public class UnionTempTableTemplatePacket extends UnionTemplatePacket implements TempTableQTPI{
    public UnionTempTableTemplatePacket() {
        setPackets(new ArrayList<>());
    }

    @Override
    public boolean matches(TableRow tableRow) {
        for (QueryTemplatePacket packet : getPackets()) {
            boolean isInRange = ((TempTableQTPI)packet).matches(tableRow);

            if (isInRange)
                return true;
        }
        return false;
    }
}
